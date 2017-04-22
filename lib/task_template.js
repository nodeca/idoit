'use strict';


const utils   = require('./utils');
const Command = require('./command');


function TaskTemplate(queue, ...args) {
  this.queue = queue;

  this.uid      = utils.random();
  this.retries  = 0;
  this.total    = 1;
  this.progress = 0;
  this.state    = 'waiting';
  this.args     = args;
}


TaskTemplate.prototype.name         = 'task';
TaskTemplate.prototype.pool         = 'default';
TaskTemplate.prototype.retry        = 2;
TaskTemplate.prototype.retryDelay   = 60000;
TaskTemplate.prototype.timeout      = 120000;
TaskTemplate.prototype.removeDelay  = 3 * 24 * 60 * 60 * 1000;
TaskTemplate.prototype.__prepare_promise__ = null;


// Update task options
//
TaskTemplate.prototype.options = function (opts) {
  Object.assign(this, opts);
  return this;
};


// (internal) Prepare task prior to run. Used to modify templates
// behaviour on inherit.
//
// !!! Don't touch this method, override `.init()` to extend
// registered tasks.
//
TaskTemplate.prototype.prepare = async function () {
  this.id = this.taskID(...this.args);

  // .init() can be simple sync function
  await Promise.resolve().then(() => this.init());
};


// Override to customize task init. Async functions should return `Promise`
//
TaskTemplate.prototype.init = function () {};


// Create random task ID
//
TaskTemplate.prototype.taskID = function () {
  return utils.random();
};


// Put task to queue and run immediately
//
TaskTemplate.prototype.run = function () {
  return Promise.resolve(this).then(() => this.queue.__addTask__(this));
};


// Postpone task to `delay` ms.
//
TaskTemplate.prototype.postpone = function (delay = this.postponeDelay) {
  return Promise.resolve(this).then(() => this.queue.__addTask__(this, delay));
};


// Force restart of a running task
//
TaskTemplate.prototype.restart = function (add_retry, delay) {
  if (typeof add_retry === 'number') {
    // restart(delay)
    delay = add_retry;
    add_retry = false;
  }

  if (typeof add_retry === 'undefined') add_retry = false;
  if (typeof delay === 'undefined') delay = this.retryDelay;

  return Promise.resolve(this).then(() => this.queue.__restartTask__(this, add_retry, delay));
};


// Update task deadline, set to `timeLeft` ms.
//
TaskTemplate.prototype.setDeadline = async function (timeLeft) {
  let prefix = this.queue.__prefix__;
  let time = utils.redisToMs(await this.queue.__redis__.timeAsync());

  let success = await this.queue.__redis__.evalAsync(
    this.queue.__scripts__.transaction,
    1,
    JSON.stringify({
      validate: [
        [ String(this.__deadline__), [ 'zscore', `${prefix}locked`, this.id ] ]
      ],
      exec: [
        [ 'zadd', `${prefix}locked`, timeLeft + time, this.id ]
      ]
    })
  );

  if (success) {
    this.__deadline__ = timeLeft + time;
  }
};


// Add `value` to task progress, and propagate update to parent
//
TaskTemplate.prototype.progressAdd = async function (value) {
  // TODO: that's not safe on parallel run, but nobody cares...
  // Progress value has no strict requirements.

  let time   = utils.redisToMs(await this.queue.__redis__.timeAsync());
  let prefix = this.queue.__prefix__;

  let transaction = {
    validate: [
      [ 1, [ 'exists', `${prefix}${this.id}` ] ]
    ],
    exec: [
      [ 'hincrby', `${prefix}${this.id}`, 'progress', value ]
    ]
  };

  if (this.parent) {
    transaction.exec.push([ 'zadd', `${prefix}${this.parent_pool}:commands`, time, Command.fromObject({
      to:     this.parent,
      to_uid: this.parent_uid,
      type:   'progress',
      data:   value
    }).toJSON() ]);
  }

  let success = await this.queue.__redis__.evalAsync(
    this.queue.__scripts__.transaction,
    1,
    JSON.stringify(transaction)
  );

  if (success) {
    let progress = await this.queue.__redis__.hgetAsync(`${prefix}${this.id}`, 'progress');

    let eventData = {
      id: this.id,
      uid: this.uid,
      total: this.total,
      progress: +progress
    };

    this.queue.emit('task:progress', eventData);
    this.queue.emit(`task:progress:${this.id}`, eventData);
  }
};


// Handle command from other tasks. Route to `handleCommand_<type>` methods
//
TaskTemplate.prototype.handleCommand = function (command) {
  let handlerName = `handleCommand_${command.type}`;

  // Unknown command? -> ignore & send notification.
  // That should never happen.
  if (!this[handlerName]) {
    this.queue.emit(
      'error',
      new Error(`idoit error: ignored unknown command for "${this.name}" - ${command.toJSON()}`)
    );
    return Promise.resolve();
  }

  return this[handlerName](command);
};


// Process increment progress cmd (used in group, chain and iterator)
//
TaskTemplate.prototype.handleCommand_progress = async function (command) {
  let time   = utils.redisToMs(await this.queue.__redis__.timeAsync());
  let prefix = this.queue.__prefix__;
  let query  = this.queue.__redis__.multi();

  // Increment progress if task not finished
  let transaction = {
    validate: [
      [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
      { not: [ JSON.stringify('finished'), [ 'hget', `${prefix}${this.id}`, 'state' ] ] }
    ],
    exec: [
      [ 'hincrby', `${prefix}${this.id}`, 'progress', command.data ]
    ]
  };

  query.eval(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));

  if (this.parent) {
    // Send command to parent (even if transaction rejected)
    query.zadd(`${prefix}${this.parent_pool}:commands`, time, Command.fromObject({
      to:     this.parent,
      to_uid: this.parent_uid,
      type:   'progress',
      data:   command.data
    }).toJSON());
  }

  let res = await query.execAsync();

  if (res[0]) {
    let eventData = {
      id: this.id,
      uid: this.uid,
      total: this.total,
      progress: this.progress + command.data
    };

    this.queue.emit('task:progress', eventData);
    this.queue.emit(`task:progress:${this.id}`, eventData);
  }
};


// Process `activate` command (valid in `waiting` state only)
//
TaskTemplate.prototype.handleCommand_activate = async function (command) {
  let prefix      = this.queue.__prefix__;
  let prefix_pool = `${this.queue.__prefix__}${this.pool}:`;

  await this.queue.__redis__.evalAsync(
    this.queue.__scripts__.transaction,
    1,
    JSON.stringify({
      validate: [
        [ 1, [ 'zrem', `${prefix_pool}commands_locked`, command.toJSON() ] ],
        [ JSON.stringify('waiting'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
      ],
      exec: [
        [ 'srem', `${prefix}waiting`, this.id ],
        [ 'sadd', `${prefix_pool}startable`, this.id ],
        [ 'hset', `${prefix}${this.id}`, 'state', JSON.stringify('startable') ]
      ]
    })
  );
};


// Error command handler (used in group, chain and iterator)
//
TaskTemplate.prototype.handleCommand_error = async function (command) {
  let prefix = this.queue.__prefix__;
  let time   = utils.redisToMs(await this.queue.__redis__.timeAsync());


  let cancelIDs = await this.queue.__collectChildren__(this.id);

  // Fetch children data to get pools and tasks `removeDelay`
  //
  // TODO: This make task dependent on registered classes in this
  // queue instance. Consider rework
  //
  let rawTasks = await this.queue.__getRawTasks__(cancelIDs);

  let pools = rawTasks.reduce((acc, t) => {
    if (t && !acc.includes(t.pool)) acc.push(t.pool);
    return acc;
  }, []);


  // Remove tasks from all states collections, except `finished`
  //
  let query = this.queue.__redis__.multi()
    .srem(`${prefix}waiting`, cancelIDs.concat([ this.id ]))
    .srem(`${prefix}idle`, cancelIDs.concat([ this.id ]))
    .zrem(`${prefix}locked`, cancelIDs.concat([ this.id ]))
    .zrem(`${prefix}restart`, cancelIDs.concat([ this.id ]));

  pools.forEach(pool => query.srem(`${prefix}${pool}:startable`, cancelIDs.concat([ this.id ])));

  // Calculate number of commands before transactions, to get results later
  let shift = 4 + pools.length;

  // Add transaction for this task:
  //
  // - move this task to `finished`
  // - update state
  // - save child error
  // - send command with error to parent
  //
  let transaction = {
    validate: [
      [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
      { not: [ JSON.stringify('finished'), [ 'hget', `${prefix}${this.id}`, 'state' ] ] }
    ],
    exec: [
      [ 'zadd', `${prefix}finished`, time + this.removeDelay, this.id ],
      [ 'hset', `${prefix}${this.id}`, 'state', JSON.stringify('finished') ],
      [ 'hset', `${prefix}${this.id}`, 'error', JSON.stringify(command.data.error) ]
    ]
  };

  if (this.parent) {
    // Send command with error to parent
    transaction.exec.push([ 'zadd', `${prefix}${this.parent_pool}:commands`, time, Command.fromObject({
      to:     this.parent,
      to_uid: this.parent_uid,
      type:   'error',
      data:   command.data
    }).toJSON() ]);
  }

  query.eval(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));


  // Add transactions for children:
  //
  // - move tasks to `finished`
  // - update their state
  // - save error
  //
  let state = JSON.stringify('finished');
  let error = JSON.stringify({ code: 'CANCELED', name: 'Error', message: 'idoit error: task canceled' });

  cancelIDs.forEach((id, i) => {
    // If task already deleted/cancelled - skip
    if (!rawTasks[i] || rawTasks[i].state === 'finished') return;

    let removeDeadline = time + rawTasks[i].removeDelay;

    query.eval(
      this.queue.__scripts__.transaction,
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


  let res = await query.execAsync();

  if (res[shift]) { // transaction result
    let eventData = { id: this.id, uid: this.uid };

    this.queue.emit('task:end', eventData);
    this.queue.emit(`task:end:${this.id}`, eventData);
  }
};


// Fields for `.toObject()`
//
TaskTemplate.serializableFields = [
  'name',
  'id',
  'pool',
  'uid',
  'retries',
  'total',
  'progress',
  'state',
  'parent',
  'parent_uid',
  'parent_pool',
  'args',
  'result',
  'error',
  // keep it to access without task class cast on cancel/error
  'removeDelay'
];


// Serialize task to plain object
//
TaskTemplate.prototype.toObject = function () {
  return this.constructor.serializableFields.reduce((obj, key) => {
    if (key in this) obj[key] = this[key];
    return obj;
  }, {});
};


// Unserialize task from plain object
//
TaskTemplate.prototype.fromObject = function (obj) {
  this.constructor.serializableFields.forEach(key => {
    if (obj.hasOwnProperty(key)) this[key] = obj[key];
  });
};


// Helper to fill parent references
//
TaskTemplate.prototype.setParent = function (parentTask) {
  this.parent      = parentTask.id;
  this.parent_uid  = parentTask.uid;
  this.parent_pool = parentTask.pool;
};


// Task class factory
//
TaskTemplate.extend = function (options) {
  if (typeof options.process !== 'function') {
    throw new Error('idoit error: "process" should be a function');
  }

  class T extends TaskTemplate {}

  Object.assign(T.prototype, options);

  return T;
};


module.exports = TaskTemplate;
