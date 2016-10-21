'use strict';


const inherits         = require('util').inherits;
const EventEmitter     = require('events').EventEmitter;
const CronJob          = require('cron').CronJob;
const serializeError   = require('serialize-error');
const utils            = require('./utils');
const redis            = require('redis');
const Promise          = require('bluebird');


const QueueError       = require('./error');
const TaskTemplate     = require('./task_template');
const ChainTemplate    = require('./chain_template');
const GroupTemplate    = require('./group_template');
const IteratorTemplate = require('./iterator_template');
const Command          = require('./command');


// Promisify redis (https://github.com/NodeRedis/node_redis#promises)
Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);


let scripts = {};

scripts.transaction = `
-- Validate conditions and exec commands if ok
--

local params = cjson.decode(KEYS[1])

-- Check checks
for __, check in pairs(params["validate"]) do
  if check["not"] then
    if check["not"][1] == redis.call(unpack(check["not"][2])) then
      return 0
    end
  else
    if check[1] ~= redis.call(unpack(check[2])) then
      return 0
    end
  end
end

-- Eval redis commands
for __, exec in pairs(params["exec"]) do
  redis.call(unpack(exec))
end


return 1
`;


///////////////////////////////////////////////////////////////////////////////


function Queue({ redisURL, concurrency = 100, name = 'default', ns = 'idoqueue:' } = {}) {
  EventEmitter.call(this);

  if (!redisURL) {
    throw new Error('ido error: "redisURL" is required');
  }

  // Add empty handler to avoid unhandled exceptions
  // (suppress default EventEmitter behaviour)
  this.on('error', () => {});

  this.__redis_url__ = redisURL;
  this.options({ concurrency, name, ns });

  this.__scripts__      = scripts;
  this.__redis__        = null;
  this.__timer__        = null;
  this.__stopped__      = true;
  this.__types__        = {};
  this.__tasksTracker__ = 0;
  this.__ready__        = null;

  // Register default operators
  this.registerTask({ name: 'chain', baseClass: ChainTemplate });
  this.registerTask({ name: 'group', baseClass: GroupTemplate });

  this.__init__();
}


inherits(Queue, EventEmitter);


// Return promise, resolved when queue is ready to accept commands
// (after 'ready' emited)
//
Queue.prototype.ready = function () {
  return this.__ready__;
};


// Start queue processing. Should be called after all tasks registered.
//
Queue.prototype.start = function () {
  this.__stopped__ = false;
  return this.ready();
};


// Proxies to simplify options set
Object.defineProperty(Queue.prototype, 'ns', {
  set(val) { this.__prefix__ = val; }
});

Object.defineProperty(Queue.prototype, 'concurrency', {
  set(val) { this.__concurrency__ = val; }
});

Object.defineProperty(Queue.prototype, 'name', {
  set(val) { this.__pool__ = val; }
});


// Update queue options
//
Queue.prototype.options = function (opts) {
  Object.assign(this, opts);
  return this;
};


// Stop accepting new tasks from queue & wait until active tasks done.
//
Queue.prototype.shutdown = function () {
  this.__stopped__ = true;

  if (this.__tasksTracker__ === 0) return Promise.resolve(true);

  return Promise.delay(50).then(() => this.shutdown());
};


// Register task
//
// registerTask(name [, cron], process):
//
// - name (String) - the task's name
// - cron (String) - optional, cron string ("15 */6 * * *"), default null
// - process (Function) - called as: `task.process()`
//   - this (Object) - current task (task data is available as `this.args`)
//
// registerTask(options):
//
// - options (Object)
//   - name (String) - the task's name
//   - baseClass (Function) - optional, base task's constructor, default Task
//   - init (Function) - optional, initialize function to change the task's payload
//   - taskID (Function)
//   - process (Function) - called as: `task.process(...args)`
//     - this (Object) - current task (task data is available as `this.args`)
//   - pool (String) - optional, pool name, default `default`
//   - retry (Number) - optional, number of retry on error, default 2.
//   - retryDelay (Number) - optional, delay in ms after retries, default 60000 ms.
//   - timeout (Number) - optional, execution timeout, default 120000 ms.
//   - postponeDelay (Number) - optional, if postpone is called without delay,
//     delay is assumed to be equal to this value (in milliseconds).
//   - cron (String) - optional, cron string ("15 */6 * * *"), default null
//   - track (Number) - default 3600000ms (1hr). Time to remember scheduled
//     tasks from cron to avoid rerun if several servers in cluster have wrong
//     clocks. Don't set too high for very frequent tasks, because it can occupy
//     a lot of memory.
//
Queue.prototype.registerTask = function (...args) {
  let options;

  if (args.length === 1) {
    // invoked as `registerTask(options)`
    options = Object.assign({}, args[0]);
    options.baseClass = options.baseClass || TaskTemplate;

  } else if (args.length === 2) {
    // invoked as `registerTask(name, process)`
    options = {
      baseClass: TaskTemplate,
      name:      args[0],
      process:   args[1]
    };

  } else {
    // invoked as `registerTask(name, cron, process)`
    options = {
      baseClass: TaskTemplate,
      name:      args[0],
      cron:      args[1],
      process:   args[2]
    };
  }

  if (this.__types__[options.name]) {
    throw new Error(`Queue registerTask error: task with name "${options.name}" already registered.`);
  }

  // `baseClass` & `cron` are processed in this method, everything else
  // goes to task prototype.
  let task_defaults = {};

  Object.keys(options).forEach(k => {
    if (k !== 'baseClass' && k !== 'cron') task_defaults[k] = options[k];
  });

  let TaskClass = options.baseClass.extend(task_defaults);

  this.__types__[options.name] = TaskClass;

  //
  // Create fabric to build tasks of this type as
  // `queue.<task_name>(params)`
  //
  this[options.name] = (...args) => {
    let task = new TaskClass(this, ...args);

    task.then = (onFulfilled, onRejected) => {
      if (!task.__prepare_promise__) {
        task.__prepare_promise__ = Promise.resolve().then(() => task.prepare());
      }

      return task.__prepare_promise__.then(onFulfilled, onRejected);
    };

    return task;
  };


  if (options.cron) {
    let track = options.hasOwnProperty('track') ? options.track : 1 * 60 * 60 * 1000;

    this.__schedule__(options.name, options.cron, track);
  }
};


// Get task by global task id
//
Queue.prototype.getTask = Promise.coroutine(function* (id) {
  // Get serialized payload
  let serializedPayload = yield this.__redis__.hgetallAsync(`${this.__prefix__}${id}`);

  if (!serializedPayload) return null;

  // Unserialize payload
  let payload = Object.keys(serializedPayload).reduce((acc, key) => {
    acc[key] = JSON.parse(serializedPayload[key]);
    return acc;
  }, {});

  let TaskClass = this.__types__[payload.name];

  if (!TaskClass) return null;

  let task = new TaskClass(this);

  // Restore task from payload
  task.fromObject(payload);

  return task;
});


// Cancel task by id (you can cancel only tasks without parent)
//
Queue.prototype.cancel = Promise.coroutine(function* (id) {
  let task = yield this.getTask(id);

  if (!task) return;

  if (task.parent) {
    throw new Error(`ido error: task with parent can not be cancelled, task id ${id}`);
  }

  let prefix    = this.__prefix__;
  // Collect all child of this task
  let cancelIDs = yield this.__collectChildren__(task.id);
  // and add self to cancel list
  cancelIDs.push(task.id);


  // Fetch class names (to get `removeDelay`)
  //
  let classNamesQuery = this.__redis__.multi();

  cancelIDs.forEach(id => {
    classNamesQuery.hget(`${prefix}${id}`, 'name');
  });

  let classNames = yield classNamesQuery.execAsync();


  // Remove tasks from all states collections, except `finished`
  //
  let query = this.__redis__.multi()
    .srem(`${prefix}waiting`, cancelIDs)
    .srem(`${prefix}idle`, cancelIDs)
    .zrem(`${prefix}startable`, cancelIDs)
    .zrem(`${prefix}locked`, cancelIDs)
    .zrem(`${prefix}restart`, cancelIDs);


  // Add transaction to:
  //
  // - move tasks to `finished`
  // - update their state
  // - save error
  //
  let time  = utils.redisToMs(yield this.__redis__.timeAsync());
  let state = JSON.stringify('finished');
  let error = JSON.stringify({ code: 'CANCELED', name: 'Error', message: 'ido error: task canceled' });

  cancelIDs.forEach((id, i) => {
    // If task already deleted - continue
    if (!classNames[i]) return;

    let removeDeadline = time + this.__types__[JSON.parse(classNames[i])].prototype.removeDelay;

    query.eval(
      this.__scripts__.transaction,
      1,
      JSON.stringify({
        validate: [
          { not: [ state, [ 'hget', `${prefix}${id}`, 'state' ] ] }
        ],
        exec: [
          [ 'zadd', `${prefix}finished`, removeDeadline, id ],
          [ 'hset', `${prefix}${id}`, 'state', state ],
          [ 'hset', `${prefix}${id}`, 'error', error ]
        ]
      })
    );
  });


  let res = yield query.execAsync();


  // Fetch uids (for `task:end` event)
  //
  let uidsQuery = this.__redis__.multi();

  cancelIDs.forEach(id => {
    uidsQuery.hget(`${prefix}${id}`, 'uid');
  });

  let uids = yield uidsQuery.execAsync();

  cancelIDs.forEach((id, i) => {
    if (!res[i + 5]) return;

    let eventData = { id, uid: JSON.parse(uids[i]) };

    this.emit('task:end', eventData);
    this.emit(`task:end:${id}`, eventData);
  });
});


// Get all children IDs as flat array
//
Queue.prototype.__collectChildren__ = function (id, result) {
  if (!result) {
    result = [];
  } else {
    result.push(id);
  }

  return this.__redis__.hgetAsync(`${this.__prefix__}${id}`, 'children').then(children => Promise
    .resolve(children ? JSON.parse(children) : [])
    .each(id => this.__collectChildren__(id, result))
    .then(() => result));
};


// Add new task to waiting
//
// - task
// - delay (Number) - optional, postpone task execution
//
Queue.prototype.__addTask__ = Promise.coroutine(function* (task, delay) {
  let query        = this.__redis__.multi();
  let prefix       = this.__prefix__;

  let previousTask = yield this.getTask(task.id);

  if (previousTask) {
    // If active task with the same id already exists - return it.
    if (previousTask.state !== 'finished') return previousTask.id;

    // If finished task with the same id exists - remove it
    query
      .zrem(`${prefix}finished`, previousTask.id)
      .del(`${prefix}${previousTask.id}`);
  }

  let taskQueue = [ task ];

  // Iterate through each child in tree
  while (taskQueue.length > 0) {
    let t       = taskQueue.shift();
    let payload = t.toObject();

    let serializedPayload = Object.keys(payload).reduce((acc, key) => {
      acc[key] = JSON.stringify(payload[key]);
      return acc;
    }, {});

    // Save task data
    query.hmset(`${prefix}${t.id}`, serializedPayload);
    // Add task id to `waiting` set
    query.sadd(`${prefix}waiting`, t.id);

    taskQueue = taskQueue.concat(t.__children__ || []);
  }

  let time = utils.redisToMs(yield this.__redis__.timeAsync());

  // Create task `activate` command
  query.zadd(`${prefix}commands`, time + (delay || 0), Command.fromObject({
    type:   'activate',
    to:     task.id,
    to_uid: task.uid
  }).toJSON());

  yield query.execAsync();

  setImmediate(() => {
    Promise.resolve()
      .then(() => this.__consumeCommands__())
      .then(() => this.__consumeTasks__())
      .catch(err => this.emit('error', err));
  });

  return task.id;
});


// Check if task was registered on this instance
//
Queue.prototype.__hasKnownTask__ = function (id) {
  return this.__redis__.hgetAsync(`${this.__prefix__}${id}`, 'name').then(name => {
    name = JSON.parse(name);

    return !!this.__types__[name];
  });
};


// Schedule the task executions
//
// - worker (Object) - worker options
//
Queue.prototype.__schedule__ = function (taskName, cronString, trackTime) {
  let job = new CronJob(cronString, Promise.coroutine(function* () {
    try {

      if (this.__stopped__) return;

      if (!trackTime) {
        yield this[taskName]().run();
        return;
      }

      // To generate `sheduledID` we use timestamp of next exec because `node-cron`
      // doesn't present timestamp of current exec
      //
      let scheduledID = `${this.__prefix__}cron:${taskName}:${job.cronTime.sendAt().format('X')}`;


      // Check if another instance scheduled the task
      let acquired = yield this.__redis__.setnxAsync(scheduledID, scheduledID);

      // Exit if the task already scheduled in different instance
      if (!acquired) return;

      // Set tracker lifetime (3 days) to auto collect garbage
      yield this.__redis__.expireAsync(scheduledID, trackTime / 1000);

      yield this[taskName]().run();

    } catch (err) {
      this.emit('error', err);
    }
  }).bind(this));


  job.start();
};


// Execute task
//
// - move from `startable` to `locked`
// - execute task
// - move from `locked` to `finished`
//
Queue.prototype.__execTask__ = Promise.coroutine(function* (id) {
  let time = utils.redisToMs(yield this.__redis__.timeAsync());
  let task = yield this.getTask(id);
  let prefix = this.__prefix__;

  // If task deleted - skip
  if (!task) return;


  // If parent task already finished - cancel this task
  //
  if (task.parent) {
    let parent = yield this.getTask(task.parent);

    if (parent.state === 'finished') {
      let error = JSON.stringify({ code: 'CANCELED', name: 'Error', message: 'ido error: task canceled' });

      let transaction = {
        validate: [
          [ JSON.stringify('startable'), [ 'hget', `${prefix}${task.id}`, 'state' ] ]
        ],
        exec: [
          [ 'zrem', `${prefix}startable`, task.id ],
          [ 'zadd', `${prefix}finished`, time + task.removeDelay, task.id ],
          [ 'hset', `${prefix}${task.id}`, 'state', JSON.stringify('finished') ],
          [ 'hset', `${prefix}${task.id}`, 'error', error ]
        ]
      };

      let res = yield this.__redis__.evalAsync(this.__scripts__.transaction, 1, JSON.stringify(transaction));

      if (res) {
        let eventData = { id: task.id, uid: task.uid };

        this.emit('task:end', eventData);
        this.emit(`task:end:${task.id}`, eventData);
      }

      return;
    }
  }


  task.__deadline__ = task.timeout + time;

  let prefix_pool = prefix + task.pool + ':';

  // Try to acquire task lock (move to `locked` set)
  let locked = yield this.__redis__.evalAsync(
    this.__scripts__.transaction,
    1,
    JSON.stringify({
      validate: [
        [ JSON.stringify('startable'), [ 'hget', `${prefix}${task.id}`, 'state' ] ]
      ],
      exec: [
        [ 'srem', `${prefix_pool}startable`, task.id ],
        [ 'zadd', `${prefix}locked`, task.__deadline__, task.id ],
        [ 'hset', `${prefix}${task.id}`, 'state', JSON.stringify('locked') ]
      ]
    })
  );

  if (!locked) return;

  // If task should be terminated, 2 things must be done:
  //
  // - update state (done via watchdog in `.__tick__()`)
  // - release "busy" counters in local process (see below)
  //
  // It doesn't matter, which action will be executed first.
  //
  let terminated = false;
  let terminateTimerId = setTimeout(() => {
    // Do nothing if deadline changed inside task
    // TODO: create new terminate timer
    if (task.__deadline__ !== task.timeout + time) return;

    this.__tasksTracker__--;
    terminated = true;
  }, task.timeout);

  this.__tasksTracker__++;

  let result, error;

  try {
    result = yield Promise.resolve().then(() => task.process(...task.args));
  } catch (err) {
    error = err;
  }

  clearTimeout(terminateTimerId);

  // If task timed out before `.process()` end - do nothing,
  // cleanup will be done by watchdog
  if (terminated) return;

  this.__tasksTracker__--;

  time = utils.redisToMs(yield this.__redis__.timeAsync());


  // If error we should postpone next retry of task run
  //
  if (error) {
    // Send error event with error, task name and task ID to have ability group errors
    this.emit('error', new QueueError(error, task.name, 'locked', task.id, task.args));

    if (task.retries >= task.retry - 1) {
      // If retries count exceeded - finish task
      let transaction = {
        validate: [
          [ String(task.__deadline__), [ 'zscore', `${prefix}locked`, task.id ] ]
        ],
        exec: [
          [ 'zrem', `${prefix}locked`, task.id ],
          [ 'zadd', `${prefix}finished`, task.removeDelay + time, task.id ],
          [ 'hset', `${prefix}${task.id}`, 'error', JSON.stringify(serializeError(error)) ],
          [ 'hset', `${prefix}${task.id}`, 'state', JSON.stringify('finished') ]
        ]
      };

      if (task.parent) {
        // Send command with error to parent
        transaction.exec.push([ 'zadd', `${prefix}commands`, time, Command.fromObject({
          to:     task.parent,
          to_uid: task.parent_uid,
          type:   'error',
          data:   { error: serializeError(error) }
        }).toJSON() ]);
      }

      let res = yield this.__redis__.evalAsync(this.__scripts__.transaction, 1, JSON.stringify(transaction));

      if (res) {
        let eventData = { id: task.id, uid: task.uid };

        this.emit('task:end', eventData);
        this.emit(`task:end:${task.id}`, eventData);
      }

    } else {
      // Schedule next restart
      yield this.__redis__.evalAsync(
        this.__scripts__.transaction,
        1,
        JSON.stringify({
          validate: [
            [ String(task.__deadline__), [ 'zscore', `${prefix}locked`, task.id ] ]
          ],
          exec: [
            [ 'zrem', `${prefix}locked`, task.id ],
            [ 'zadd', `${prefix}restart`, task.retryDelay + time, task.id ],
            [ 'hset', `${prefix}${task.id}`, 'state', JSON.stringify('restart') ],
            [ 'hset', `${prefix}${task.id}`, 'retries', JSON.stringify(1 + task.retries) ]
          ]
        })
      );
    }


  } else {
    // If success - move task to `finished`
    //
    let transaction = {
      validate: [
        [ String(task.__deadline__), [ 'zscore', `${prefix}locked`, task.id ] ]
      ],
      exec: [
        [ 'zrem', `${prefix}locked`, task.id ],
        [ 'zadd', `${prefix}finished`, task.removeDelay + time, task.id ],
        [ 'hset', `${prefix}${task.id}`, 'state', JSON.stringify('finished') ],

        // Set progress
        [ 'hset', `${prefix}${task.id}`, 'progress', task.total ]
      ]
    };

    if (typeof result !== 'undefined') {
      transaction.exec.push([ 'hset', `${prefix}${task.id}`, 'result', JSON.stringify(result) ]);
    }

    if (task.parent) {
      // Send command with result to parent
      transaction.exec.push([ 'zadd', `${prefix}commands`, time, Command.fromObject({
        to:     task.parent,
        to_uid: task.parent_uid,
        type:   'result',
        data:   { result }
      }).toJSON() ]);

      // Get actual task progress
      let progress = JSON.parse(yield this.__redis__.hgetAsync(`${prefix}${task.id}`, 'progress'));

      if (task.total !== progress) {
        // Send command with progress to parent
        transaction.exec.push([ 'zadd', `${prefix}commands`, time, Command.fromObject({
          to: task.parent,
          to_uid: task.parent_uid,
          type: 'progress',
          data: task.total - progress
        }).toJSON() ]);
      }
    }

    let res = yield this.__redis__.evalAsync(this.__scripts__.transaction, 1, JSON.stringify(transaction));

    if (res) {
      let eventData = { id: task.id, uid: task.uid };

      this.emit('task:end', eventData);
      this.emit(`task:end:${task.id}`, eventData);
    }
  }

  if (this.__stopped__) return;

  yield this.__consumeCommands__();
  yield this.__consumeTasks__();
});


// Run all startable tasks
//
Queue.prototype.__consumeTasks__ = Promise.coroutine(function* () {
  if (this.__tasksTracker__ >= this.__concurrency__) {
    return;
  }

  let prefix_pool = this.__prefix__ + this.__pool__ + ':';

  let IDs = yield this.__redis__.srandmemberAsync(
    `${prefix_pool}startable`,
    this.__concurrency__ - this.__tasksTracker__
  );

  for (let i = 0; i < IDs.length; i++) {
    let hasTask = yield this.__hasKnownTask__(IDs[i]);

    if (!hasTask) continue;

    // Execute tasks in parallel
    this.__execTask__(IDs[i]).catch(err => this.emit('error', err));
  }
});


// Run commands from transaction queue
//
Queue.prototype.__consumeCommands__ = Promise.coroutine(function* () {
  let prefix = this.__prefix__;
  let time   = utils.redisToMs(yield this.__redis__.timeAsync());

  let commands = yield this.__redis__.zrangebyscoreAsync(
    `${this.__prefix__}commands`,
    '-inf',
    time,
    'LIMIT',
    0,
    10,
    'withscores'
  );

  for (let i = 0; i < commands.length; i += 2) {
    let score = commands[i + 1];
    let cmd   = Command.fromJSON(commands[i]);

    let hasTask = yield this.__hasKnownTask__(cmd.to);

    if (!hasTask) continue;

    let lockLifetime = utils.redisToMs(yield this.__redis__.timeAsync()) + 30000;

    let locked = yield this.__redis__.evalAsync(
      this.__scripts__.transaction,
      1,
      JSON.stringify({
        validate: [
          [ String(score), [ 'zscore', `${prefix}commands`, commands[i] ] ]
        ],
        exec: [
          [ 'zrem', `${prefix}commands`, commands[i] ],
          [ 'zadd', `${prefix}commands_locked`, lockLifetime, commands[i] ]
        ]
      })
    );

    if (!locked) continue;

    let task = yield this.getTask(cmd.to);

    // If singletone task finishes, some commands can still be in queue.
    // So, if we rerun such task immediately, it should ignore old commands.
    if (task && task.uid === cmd.to_uid) {
      yield task.handleCommand(cmd);
    }


    // Remove locked command. Usually task should remove command by itself, this needed when:
    //
    // - task already removed
    // - uid is invalid
    //
    yield this.__redis__.zremAsync(`${this.__prefix__}commands_locked`, commands[i]);
  }
});


//
//
Queue.prototype.__tick__ = Promise.coroutine(function* () {
  if (this.__stopped__) return;


  let query  = this.__redis__.multi();
  let prefix = this.__prefix__;
  let time   = utils.redisToMs(yield this.__redis__.timeAsync());


  // (watchdog) Consume pending commands if those were not catched before
  //
  let suspendedCommands = yield this.__redis__.zrangebyscoreAsync(
    `${prefix}commands_locked`, '-inf', time, 'withscores');

  for (let i = 0; i < suspendedCommands.length; i += 2) {
    let command = suspendedCommands[i];
    let score = suspendedCommands[i + 1];

    query.eval(
      this.__scripts__.transaction,
      1,
      JSON.stringify({
        validate: [
          [ String(score), [ 'zscore', `${prefix}commands_locked`, command ] ]
        ],
        exec: [
          [ 'zrem', `${prefix}commands_locked`, command ],
          [ 'zadd', `${prefix}commands`, time, command ]
        ]
      })
    );
  }


  // (watchdog) Restart suspended tasks (move `locked` -> `startable`)
  //
  let suspendedIDs = yield this.__redis__.zrangebyscoreAsync(`${prefix}locked`, '-inf', time, 'withscores');

  for (let i = 0; i < suspendedIDs.length; i += 2) {
    let id     = suspendedIDs[i];
    let score  = suspendedIDs[i + 1];

    let pool = JSON.parse(yield this.__redis__.hgetAsync(`${prefix}${id}`, 'pool'));
    let prefix_pool = prefix + pool + ':';

    query.eval(
      this.__scripts__.transaction,
      1,
      JSON.stringify({
        validate: [
          [ String(score), [ 'zscore', `${prefix}locked`, id ] ]
        ],
        exec: [
          [ 'zrem', `${prefix}locked`, id ],
          [ 'sadd', `${prefix_pool}startable`, id ],
          [ 'hset', `${prefix}${id}`, 'state', JSON.stringify('startable') ]
        ]
      })
    );
  }


  // Schedule postponed restarts (move `restart` -> `startable`)
  //
  let restartIDs = yield this.__redis__.zrangebyscoreAsync(`${prefix}restart`, '-inf', time, 'withscores');

  for (let i = 0; i < restartIDs.length; i += 2) {
    let id     = restartIDs[i];
    let score  = restartIDs[i + 1];

    let pool = JSON.parse(yield this.__redis__.hgetAsync(`${prefix}${id}`, 'pool'));
    let prefix_pool = prefix + pool + ':';

    query.eval(
      this.__scripts__.transaction,
      1,
      JSON.stringify({
        validate: [
          [ String(score), [ 'zscore', `${prefix}restart`, id ] ]
        ],
        exec: [
          [ 'zrem', `${prefix}restart`, id ],
          [ 'sadd', `${prefix_pool}startable`, id ],
          [ 'hset', `${prefix}${id}`, 'state', JSON.stringify('startable') ]
        ]
      })
    );
  }


  // (gc) Remove old finished tasks
  //
  let oldFinishedIds = yield this.__redis__.zrangebyscoreAsync(`${prefix}finished`, '-inf', time);

  for (let i = 0; i < oldFinishedIds.length; i++) {
    query
      .del(`${prefix}${oldFinishedIds[i]}`)
      .zrem(`${prefix}finished`, oldFinishedIds[i]);
  }


  yield query.execAsync();
  yield this.__consumeCommands__();
  yield this.__consumeTasks__();
});


// Init queue
//
Queue.prototype.__init__ = function () {
  let CHECK_INTERVAL = 500;

  this.__redis__ = redis.createClient(this.__redis_url__, { enable_offline_queue: false });

  this.__redis__.on('error', err => this.emit('error', err));

  this.__redis__.once('ready', () => {
    // Start with random delay to spread requests
    // from multiple instances
    setTimeout(() => {
      this.__timer__ = setInterval(() => {
        this.__tick__().catch(err => this.emit('error', err));
      }, CHECK_INTERVAL);
    }, Math.round(Math.random() * CHECK_INTERVAL));

    this.emit('ready');
  });

  this.__ready__ = new Promise(resolve => {
    this.once('ready', () => resolve(true));
  });
};


module.exports = Queue;

module.exports.Error            = QueueError;
module.exports.TaskTemplate     = TaskTemplate;
module.exports.ChainTemplate    = ChainTemplate;
module.exports.GroupTemplate    = GroupTemplate;
module.exports.IteratorTemplate = IteratorTemplate;
