'use strict';


const inherits       = require('util').inherits;
const Promise        = require('bluebird');
const serializeError = require('serialize-error');

const TaskTemplate   = require('./task_template');
const QueueError     = require('./error');
const Command        = require('./command');
const utils          = require('./utils');


function IteratorTemplate(queue, initialState = null) {
  TaskTemplate.call(this, queue);

  this.children_created  = 0;
  this.children_finished = 0;
  this.user_data         = initialState;
}


inherits(IteratorTemplate, TaskTemplate);


// Handle `activate` command
//
IteratorTemplate.prototype.handleCommand_activate = Promise.coroutine(function* (command) {
  let prefix = this.queue.__prefix__;
  let time = utils.redisToMs(yield this.queue.__redis__.timeAsync());

  yield this.queue.__redis__.evalAsync(
    this.queue.__scripts__.transaction,
    1,
    JSON.stringify({
      validate: [
        [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
        [ JSON.stringify('waiting'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
      ],
      exec: [
        // Move this task to `idle` and update state
        [ 'srem', `${prefix}waiting`, this.id ],
        [ 'sadd', `${prefix}idle`, this.id ],
        [ 'hset', `${prefix}${this.id}`, 'state', JSON.stringify('idle') ],

        // Add iterate command
        [ 'zadd', `${prefix}commands`, time, Command.fromObject({
          to:     this.id,
          to_uid: this.uid,
          type:   'iterate'
        }).toJSON() ]
      ]
    })
  );
});


// Handle child finished
//
IteratorTemplate.prototype.handleCommand_result = Promise.coroutine(function* (command) {
  let prefix = this.queue.__prefix__;
  let time   = utils.redisToMs(yield this.queue.__redis__.timeAsync());


  // - increment `children_finished`
  // - send `iterate` command
  //
  yield this.queue.__redis__.evalAsync(
    this.queue.__scripts__.transaction,
    1,
    JSON.stringify({
      validate: [
        [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
        [ JSON.stringify('idle'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
      ],
      exec: [
        [ 'hincrby', `${prefix}${this.id}`, 'children_finished', 1 ],
        [ 'zadd', `${prefix}commands`, time, Command.fromObject({
          to:     this.id,
          to_uid: this.uid,
          type:   'iterate'
        }).toJSON() ]
      ]
    })
  );
});


// Handle iterate command
//
IteratorTemplate.prototype.handleCommand_iterate = Promise.coroutine(function* (command) {
  // Run iterate
  //
  let iterateResult = yield Promise.resolve().then(() => this.iterate(this.user_data));

  let time   = utils.redisToMs(yield this.queue.__redis__.timeAsync());
  let prefix = this.queue.__prefix__;


  // Handle iterate finished
  //
  if (!iterateResult && this.children_finished >= this.children_created) {
    let transaction = {
      validate: [
        [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
        [ JSON.stringify('idle'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
      ],
      exec: [
        // Move this task to `finished` and update state
        [ 'srem', `${prefix}idle`, this.id ],
        [ 'zadd', `${prefix}finished`, this.removeDelay + time, this.id ],
        [ 'hset', `${prefix}${this.id}`, 'state', JSON.stringify('finished') ],

        // Set progress
        [ 'hset', `${prefix}${this.id}`, 'progress', this.total ]
      ]
    };

    if (this.parent) {
      // Send command with result to parent
      transaction.exec.push([ 'zadd', `${prefix}commands`, time, Command.fromObject({
        to:     this.parent,
        to_uid: this.parent_uid,
        type:   'result',
        data:   {}
      }).toJSON() ]);
    }

    yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
    return;
  }


  // Idle. Only allowed if one or more children still active
  if ((typeof iterateResult.state === 'undefined') && (typeof iterateResult.tasks === 'undefined')) {
    if (this.children_finished < this.children_created) return;
  }

  // If state has not been changed or task has not been added:
  //
  // - force task to finish
  // - emit error event
  //
  if (JSON.stringify(iterateResult.state) === JSON.stringify(this.user_data) || !(iterateResult.tasks || []).length) {
    let err = new QueueError(
      new Error('Iterator error: terminating task because bad "iterate" result'),
      this.name,
      'idle',
      this.id,
      this.user_data
    );

    this.queue.emit('error', err);

    let transaction = {
      validate: [
        [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
        [ JSON.stringify('idle'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
      ],
      exec: [
        // Move this task to `finished` and update state
        [ 'srem', `${prefix}idle`, this.id ],
        [ 'zadd', `${prefix}finished`, this.removeDelay + time, this.id ],
        [ 'hset', `${prefix}${this.id}`, 'state', JSON.stringify('finished') ],
        [ 'hset', `${prefix}${this.id}`, 'error', JSON.stringify(serializeError(err)) ]
      ]
    };

    if (this.parent) {
      // Send command with result to parent
      transaction.exec.push([ 'zadd', `${prefix}commands`, time, Command.fromObject({
        to:     this.parent,
        to_uid: this.parent_uid,
        type:   'error',
        data:   { error: serializeError(err) }
      }).toJSON() ]);
    }

    yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
    return;
  }


  // Add chunks
  //
  let transaction = {
    validate: [
      [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
      [ JSON.stringify('idle'), [ 'hget', `${prefix}${this.id}`, 'state' ] ],
      [ JSON.stringify(this.user_data), [ 'hget', `${prefix}${this.id}`, 'user_data' ] ]
    ],
    exec: [
      [ 'hincrby', `${prefix}${this.id}`, 'children_created', iterateResult.tasks.length ],
      [ 'hset', `${prefix}${this.id}`, 'user_data', JSON.stringify(iterateResult.state) ]
    ]
  };

  // Initialize children, link to parent
  yield Promise.all(iterateResult.tasks);

  for (let i = 0; i < iterateResult.tasks.length; i++) {
    iterateResult.tasks[i].parent     = this.id;
    iterateResult.tasks[i].parent_uid = this.uid;

    // Send `activate` command to chunks (without it's children)
    transaction.exec.push([ 'zadd', `${prefix}commands`, time, Command.fromObject({
      to:     iterateResult.tasks[i].id,
      to_uid: iterateResult.tasks[i].uid,
      type:   'activate'
    }).toJSON() ]);
  }

  let taskQueue = [].concat(iterateResult.tasks);

  // Add chunks & their children
  while (taskQueue.length > 0) {
    let t       = taskQueue.shift();
    let payload = t.toObject();

    Object.keys(payload).forEach(key => {
      transaction.exec.push([ 'hset', `${prefix}${t.id}`, key, JSON.stringify(payload[key]) ]);
    });

    transaction.exec.push([ 'sadd', `${prefix}waiting`, t.id ]);

    taskQueue = taskQueue.concat(t.children || []);
  }

  yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
});


IteratorTemplate.serializableFields = TaskTemplate.serializableFields.concat([
  'children_created',
  'children_finished',
  'user_data'
]);


IteratorTemplate.extend = function (options) {
  if (typeof options.iterate !== 'function') {
    throw new Error('ido error: "iterate" should be a function');
  }

  class T extends IteratorTemplate {}

  [
    'name',
    'taskID',
    'init',
    'iterate'
  ].forEach(k => {
    if (options.hasOwnProperty(k)) {
      T.prototype[k] = options[k];
    }
  });

  return T;
};


module.exports = IteratorTemplate;
