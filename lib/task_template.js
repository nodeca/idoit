'use strict';


const utils       = require('./utils');
const Promise     = require('bluebird');
const Command     = require('./command');


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
TaskTemplate.prototype.prepare = Promise.coroutine(function* () {
  this.id = this.taskID(...this.args);

  // .init() can be simple sync function
  yield Promise.resolve().then(() => this.init());
});


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


// Update task deadline, set to `timeLeft` ms.
//
TaskTemplate.prototype.setDeadline = Promise.coroutine(function* (timeLeft) {
  let prefix = this.queue.__prefix__;
  let time = utils.redisToMs(yield this.queue.__redis__.timeAsync());

  let success = yield this.queue.__redis__.evalAsync(
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
});


// Add `value` to task progress, and propagate update to parent
//
TaskTemplate.prototype.progressAdd = Promise.coroutine(function* (value) {
  // TODO: that's not safe on parallel run, but nobody cares...
  // Progress value has no strict requirements.

  let time   = utils.redisToMs(yield this.queue.__redis__.timeAsync());
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
    transaction.exec.push([ 'zadd', `${prefix}commands`, time, Command.fromObject({
      to:     this.parent,
      to_uid: this.parent_uid,
      type:   'progress',
      data:   value
    }).toJSON() ]);
  }

  let res = yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));

  if (res) {
    let progress = yield this.queue.__redis__.hgetAsync(`${prefix}${this.id}`, 'progress');

    let eventData = {
      id: this.id,
      uid: this.uid,
      total: this.total,
      progress: +progress
    };

    this.queue.emit('task:progress', eventData);
    this.queue.emit(`task:progress:${this.id}`, eventData);
  }
});


// Handle command from other tasks. Route to `handleCommand_<type>` methods
//
TaskTemplate.prototype.handleCommand = function (command) {
  let handlerName = `handleCommand_${command.type}`;

  // Unknown command? -> ignore & send notification.
  // That should never happen.
  if (!this[handlerName]) {
    this.queue.emit(
      'error',
      new Error(`ido error: ignored unknown command for "${this.name}" - ${command.toJSON()}`)
    );
    return Promise.resolve();
  }

  return this[handlerName](command);
};


// Process increment progress cmd (used in group, chain and iterator)
//
TaskTemplate.prototype.handleCommand_progress = Promise.coroutine(function* (command) {
  let time   = utils.redisToMs(yield this.queue.__redis__.timeAsync());
  let prefix = this.queue.__prefix__;
  let query  = this.queue.__redis__.multi();

  // Increment progress if task not finished
  let transaction = {
    validate: [
      [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
      { not: [ JSON.stringify('finished'), [ 'hget', `${prefix}${this.id}`, 'state' ] ] }
    ],
    exec: [
      [ 'hincrby', `${prefix}${this.id}`, 'progress', command.data ]
    ]
  };

  query.eval(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));

  if (this.parent) {
    // Send command to parent (even if transaction rejected)
    query.zadd(`${prefix}commands`, time, Command.fromObject({
      to:     this.parent,
      to_uid: this.parent_uid,
      type:   'progress',
      data:   command.data
    }).toJSON());
  }

  let res = yield query.execAsync();

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
});


// Process `activate` command (valid in `waiting` state only)
//
TaskTemplate.prototype.handleCommand_activate = Promise.coroutine(function* (command) {
  let prefix      = this.queue.__prefix__;
  let prefix_pool = this.queue.__prefix__ + this.pool + ':';

  yield this.queue.__redis__.evalAsync(
    this.queue.__scripts__.transaction,
    1,
    JSON.stringify({
      validate: [
        [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
        [ JSON.stringify('waiting'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
      ],
      exec: [
        [ 'srem', `${prefix}waiting`, this.id ],
        [ 'sadd', `${prefix_pool}startable`, this.id ],
        [ 'hset', `${prefix}${this.id}`, 'state', JSON.stringify('startable') ]
      ]
    })
  );
});


// Error command handler (used in group, chain and iterator)
//
TaskTemplate.prototype.handleCommand_error = Promise.coroutine(function* (command) {
  let prefix = this.queue.__prefix__;
  let time   = utils.redisToMs(yield this.queue.__redis__.timeAsync());


  let cancelIDs = yield this.queue.__collectChildren__(this.id);

  // Fetch class names (to get pools and tasks `removeDelay`)
  //
  // TODO: This make task dependent on registered classes in this
  // queue instance. Consider rework
  //
  let rawTasks = yield this.queue.__getRawTasks__(cancelIDs);

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


  // Add transaction for this task:
  //
  // - move this task to `finished`
  // - update state
  // - save child error
  // - send command with error to parent
  //
  let transaction = {
    validate: [
      [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
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
    transaction.exec.push([ 'zadd', `${prefix}commands`, time, Command.fromObject({
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
  let error = JSON.stringify({ code: 'CANCELED', name: 'Error', message: 'ido error: task canceled' });

  cancelIDs.forEach((id, i) => {
    // If task already deleted - continue
    if (!rawTasks[i]) return;

    let removeDeadline = time + this.queue.__types__[rawTasks[i].name].prototype.removeDelay;

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


  let res = yield query.execAsync();

  if (res[5]) { // transaction result
    let eventData = { id: this.id, uid: this.uid };

    this.queue.emit('task:end', eventData);
    this.queue.emit(`task:end:${this.id}`, eventData);
  }
});


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
  'error'
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
    throw new Error('ido error: "process" should be a function');
  }

  class T extends TaskTemplate {}

  Object.assign(T.prototype, options);

  return T;
};


module.exports = TaskTemplate;
