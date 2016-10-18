'use strict';


const inherits       = require('util').inherits;
const Promise        = require('bluebird');
const serializeError = require('serialize-error');

const TaskTemplate   = require('./task_template');
const QueueError     = require('./error');
const Command        = require('./command');
const utils          = require('./utils');


function GroupTemplate(queue, children = []) {
  TaskTemplate.call(this, queue);

  this.__children__ = children;
}


inherits(GroupTemplate, TaskTemplate);


GroupTemplate.prototype.init = Promise.coroutine(function* () {
  this.id = this.taskID();

  if (this.__user_init__) {
    // __user_init__ can be simple sync function
    yield Promise.resolve().then(() => this.__user_init__());
  }

  if (!this.__children__.length) {
    return Promise.reject(new Error('Queue error: you should specify group children'));
  }

  this.children          = [];
  this.children_finished = 0;
  this.total             = 0;
  this.result            = [];

  // Initialize children, link to parent & count progress total
  return Promise.all(this.__children__).then(() => {
    this.__children__.forEach(t => {
      this.total  += t.total;
      t.parent     = this.id;
      t.parent_uid = this.uid;

      this.children.push(t.id);
    });
  });
});


// Handle `activate` command.
//
// - move group from `waiting` to `idle`
// - send `activate` command to all children
//
GroupTemplate.prototype.handleCommand_activate = Promise.coroutine(function* (command) {
  let prefix = this.queue.__prefix__;
  let time   = utils.redisToMs(yield this.queue.__redis__.timeAsync());

  let transaction = {
    validate: [
      [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
      [ JSON.stringify('waiting'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
    ],
    exec: [
      // Move this task to `idle` and update state
      [ 'srem', `${prefix}waiting`, this.id ],
      [ 'sadd', `${prefix}idle`, this.id ],
      [ 'hset', `${prefix}${this.id}`, 'state', JSON.stringify('idle') ]
    ]
  };

  // Send `activate` command to children
  for (let i = 0; i < this.children.length; i++) {
    let childID  = this.children[i];
    let childUid = JSON.parse(yield this.queue.__redis__.hgetAsync(`${prefix}${childID}`, 'uid'));

    transaction.exec.push([ 'zadd', `${prefix}commands`, time, Command.fromObject({
      to:     childID,
      to_uid: childUid,
      type:   'activate'
    }).toJSON() ]);
  }

  yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
});


// Handle child result
//
GroupTemplate.prototype.handleCommand_result = Promise.coroutine(function* (command) {
  let prefix = this.queue.__prefix__;
  let time   = utils.redisToMs(yield this.queue.__redis__.timeAsync());


  // Update group:
  //
  // - increment `children_finished`
  // - send `group_check` command
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
          type:   'group_check'
        }).toJSON() ]
      ]
    })
  );
});


// Check group finished
//
GroupTemplate.prototype.handleCommand_group_check = Promise.coroutine(function* (command) {
  // If group not finished - skip
  if (this.children_finished < this.children.length) return;

  let prefix = this.queue.__prefix__;
  let time = utils.redisToMs(yield this.queue.__redis__.timeAsync());

  let transaction = {
    validate: [
      [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
      [ JSON.stringify('idle'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
    ],
    exec: [
      // Move this task to `finished` and update state
      [ 'srem', `${prefix}idle`, this.id ],
      [ 'zadd', `${prefix}finished`, this.removeDelay + time, this.id ],
      [ 'hset', `${prefix}${this.id}`, 'state', JSON.stringify('finished') ]
    ]
  };


  // Get children data
  //
  let query = this.queue.__redis__.multi();

  // Count existing children
  query.exists(this.children.map(id => `${prefix}${id}`));

  // Get their results
  this.children.forEach(id => query.hget(`${prefix}${id}`, 'result'));

  let childrenInfo = yield query.execAsync();


  // If some children was deleted - finish group with error
  //
  if (+childrenInfo[0] !== this.children.length) {
    let err = new QueueError(
      new Error('Group error: terminating task because children deleted'),
      this.name,
      'idle',
      this.id,
      this.user_data
    );

    this.queue.emit('error', err);

    if (this.parent) {
      // Send command with error to parent
      transaction.exec.push([ 'zadd', `${prefix}commands`, time, Command.fromObject({
        to:     this.parent,
        to_uid: this.parent_uid,
        type:   'error',
        data:   { error: serializeError(err) }
      }).toJSON() ]);
    }

    // Save error
    transaction.exec.push([ 'hset', `${prefix}${this.id}`, 'error', JSON.stringify(serializeError(err)) ]);

    yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
    return;
  }


  // Save array of children results and send command to parent
  //
  let result = childrenInfo.splice(1).map(result => JSON.parse(result));

  transaction.exec.push([ 'hset', `${prefix}${this.id}`, 'result', JSON.stringify(result) ]);

  // Set progress
  transaction.exec.push([ 'hset', `${prefix}${this.id}`, 'progress', this.total ]);

  if (this.parent) {
    // Send command with result to parent
    transaction.exec.push([ 'zadd', `${prefix}commands`, time, Command.fromObject({
      to:     this.parent,
      to_uid: this.parent_uid,
      type:   'result',
      data:   { result }
    }).toJSON() ]);
  }

  yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
});


GroupTemplate.serializableFields = TaskTemplate.serializableFields.concat([
  'children',
  'children_finished'
]);


// Task class factory
//
GroupTemplate.extend = function (options) {
  class T extends GroupTemplate {}

  [
    'name',
    'taskID'
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


module.exports = GroupTemplate;
