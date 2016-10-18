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
TaskTemplate.prototype.pool_id      = utils.poolToId('default');
TaskTemplate.prototype.retry        = 2;
TaskTemplate.prototype.retryDelay   = 60000;
TaskTemplate.prototype.timeout      = 120000;
TaskTemplate.prototype.removeDelay  = 3 * 24 * 60 * 60 * 1000;


// Init task
//
TaskTemplate.prototype.init = Promise.coroutine(function* () {
  this.id = this.taskID(...this.args);

  if (this.__user_init__) {
    // __user_init__ can be simple sync function
    yield Promise.resolve().then(() => this.__user_init__());
  }
});


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

  yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
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

  yield query.execAsync();
});


// Process `activate` command (valid in `waiting` state only)
//
TaskTemplate.prototype.handleCommand_activate = Promise.coroutine(function* (command) {
  let prefix = this.queue.__prefix__;

  yield this.queue.__redis__.evalAsync(
    this.queue.__scripts__.transaction,
    1,
    JSON.stringify({
      validate: [
        [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ]
      ],
      exec: [
        [ 'srem', `${prefix}waiting`, this.id ],
        [ 'zadd', `${prefix}startable`, this.pool_id, this.id ],
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


  // Collect all child of this task
  let cancelIDs = yield this.queue.__collectChildren__(this.id);


  // Remove tasks from all states collections, except `finished`
  //
  let query = this.queue.__redis__.multi()
    .srem(`${prefix}waiting`, cancelIDs.concat([ this.id ]))
    .srem(`${prefix}idle`, cancelIDs.concat([ this.id ]))
    .zrem(`${prefix}startable`, cancelIDs.concat([ this.id ]))
    .zrem(`${prefix}locked`, cancelIDs.concat([ this.id ]))
    .zrem(`${prefix}restart`, cancelIDs.concat([ this.id ]));


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


  // Fetch class names (to get `removeDelay`)
  //
  let classNamesQuery = this.queue.__redis__.multi();

  cancelIDs.forEach(id => {
    classNamesQuery.hget(`${prefix}${id}`, 'name');
  });

  let classNames = yield classNamesQuery.execAsync();


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
    if (!classNames[i]) return;

    let removeDeadline = time + this.queue.__types__[JSON.parse(classNames[i])].prototype.removeDelay;

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


  yield query.execAsync();
});


// Fields for `.toObject()`
//
TaskTemplate.serializableFields = [
  'name',
  'id',
  'pool_id',
  'uid',
  'retries',
  'total',
  'progress',
  'state',
  'parent',
  'parent_uid',
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


// Task class factory
//
TaskTemplate.extend = function (options) {
  if (typeof options.process !== 'function') {
    throw new Error('ido error: "process" should be a function');
  }

  class T extends TaskTemplate {}

  [
    'name',
    'pool_id',
    'retry',
    'process',
    'retryDelay',
    'timeout',
    'taskID',
    'postponeDelay',
    'removeDelay'
  ].forEach(k => {
    if (options.hasOwnProperty(k)) {
      T.prototype[k] = options[k];
    }
  });

  if (options.hasOwnProperty('init')) {
    T.prototype.__user_init__ = options.init;
  }

  return T;
};


module.exports = TaskTemplate;
