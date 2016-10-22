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


// (internal) Prepare task prior to run. Used to modify templates
// behaviour on inherit.
//
// !!! Don't touch this method, override `.init()` to extend
// registered tasks.
//
GroupTemplate.prototype.prepare = Promise.coroutine(function* () {
  this.id = this.taskID();

  // .init() can be simple sync function
  yield Promise.resolve().then(() => this.init());

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
      t.setParent(this);

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
      [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
      [ JSON.stringify('waiting'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
    ],
    exec: [
      // Move this task to `idle` and update state
      [ 'srem', `${prefix}waiting`, this.id ],
      [ 'sadd', `${prefix}idle`, this.id ],
      [ 'hset', `${prefix}${this.id}`, 'state', JSON.stringify('idle') ]
    ]
  };

  // if command was for cancelled task, data can be removed (return null)
  let rawChildren = (yield this.queue.__getRawTasks__(this.children));

  // Send `activate` command to children
  for (let i = 0; i < this.children.length; i++) {
    let rawChild = rawChildren[i];

    if (!rawChildren[i]) continue;

    transaction.exec.push([
      'zadd',
      `${prefix}${rawChild.pool}:commands`,
      time,
      Command.fromObject({ to: rawChild.id, to_uid: rawChild.uid, type: 'activate' }).toJSON()
    ]);
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
        [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
        [ JSON.stringify('idle'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
      ],
      exec: [
        [ 'hincrby', `${prefix}${this.id}`, 'children_finished', 1 ],
        [ 'zadd', `${prefix}${this.pool}:commands`, time, Command.fromObject({
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
      [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
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
  let rawChildren = yield this.queue.__getRawTasks__(this.children);


  // If some children was deleted - finish group with error
  //
  if (rawChildren.some(child => child === null)) {
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
      transaction.exec.push([ 'zadd', `${prefix}${this.parent_pool}:commands`, time, Command.fromObject({
        to:     this.parent,
        to_uid: this.parent_uid,
        type:   'error',
        data:   { error: serializeError(err) }
      }).toJSON() ]);
    }

    // Save error
    transaction.exec.push([ 'hset', `${prefix}${this.id}`, 'error', JSON.stringify(serializeError(err)) ]);

    let res = yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));

    if (res) {
      let eventData = { id: this.id, uid: this.uid };

      this.queue.emit('task:end', eventData);
      this.queue.emit(`task:end:${this.id}`, eventData);
    }

    return;
  }


  // Save array of children results and send command to parent
  //
  let result = rawChildren.map(child => child.result);

  transaction.exec.push([ 'hset', `${prefix}${this.id}`, 'result', JSON.stringify(result) ]);

  // Set progress
  transaction.exec.push([ 'hset', `${prefix}${this.id}`, 'progress', this.total ]);

  if (this.parent) {
    // Send command with result to parent
    transaction.exec.push([ 'zadd', `${prefix}${this.parent_pool}:commands`, time, Command.fromObject({
      to:     this.parent,
      to_uid: this.parent_uid,
      type:   'result',
      data:   { result }
    }).toJSON() ]);
  }

  let res = yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));

  if (res) {
    let eventData = { id: this.id, uid: this.uid };

    this.queue.emit('task:end', eventData);
    this.queue.emit(`task:end:${this.id}`, eventData);
  }
});


GroupTemplate.serializableFields = TaskTemplate.serializableFields.concat([
  'children',
  'children_finished'
]);


// Task class factory
//
GroupTemplate.extend = function (options) {
  class T extends GroupTemplate {}

  Object.assign(T.prototype, options);

  return T;
};


module.exports = GroupTemplate;
