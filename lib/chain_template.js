'use strict';


const inherits     = require('util').inherits;
const Promise      = require('bluebird');

const TaskTemplate = require('./task_template');
const Command      = require('./command');
const utils        = require('./utils');


function ChainTemplate(queue, children = []) {
  TaskTemplate.call(this, queue);

  // Uninitialized children
  this.__children__ = children;
}


inherits(ChainTemplate, TaskTemplate);


// (internal) Prepare task prior to run. Used to modify templates
// behaviour on inherit.
//
// !!! Don't touch this method, override `.init()` to extend
// registered tasks.
//
ChainTemplate.prototype.prepare = Promise.coroutine(function* () {
  this.id = this.taskID();

  // .init() can be simple sync function
  yield Promise.resolve().then(() => this.init());

  if (!this.__children__.length) {
    return Promise.reject(new Error('ido error: you should specify chain children'));
  }

  this.children          = [];
  this.children_finished = 0;
  this.total             = 0;

  // Initialize children, link to parent & count progress total
  return Promise.all(this.__children__).then(() => {
    this.__children__.forEach(t => {
      this.total  += t.total;
      t.setParent(this);

      this.children.push(t.id);
    });
  });
});


// Handle `activate` command
//
// - move chain from `waiting` to `idle`
// - send `activate` command to first child
//
ChainTemplate.prototype.handleCommand_activate = Promise.coroutine(function* (command) {
  let prefix   = this.queue.__prefix__;
  let time     = utils.redisToMs(yield this.queue.__redis__.timeAsync());

  let rawChild = (yield this.queue.__getRawTasks__([ this.children[0] ]))[0];

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

  // Send `activate` command to first child
  if (rawChild) {
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
ChainTemplate.prototype.handleCommand_result = Promise.coroutine(function* (command) {
  let prefix = this.queue.__prefix__;
  let time   = utils.redisToMs(yield this.queue.__redis__.timeAsync());


  // Run next children task
  //
  if (this.children_finished + 1 < this.children.length) {
    let childID  = this.children[this.children_finished + 1];

    let rawChild = (yield this.queue.__getRawTasks__([ childID ]))[0];

    let transaction = {
      validate: [
        [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
        [ JSON.stringify('idle'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
      ],
      exec: [
        // Increment `children_finished`
        [ 'hincrby', `${prefix}${this.id}`, 'children_finished', 1 ]
      ]
    };

    // Send `activate` command to child
    if (rawChild) {
      transaction.exec.push([
        'zadd',
        `${prefix}${rawChild.pool}:commands`,
        time,
        Command.fromObject({ to: childID, to_uid: rawChild.uid, type: 'activate' }).toJSON()
      ]);
    }

    // Merge child args
    if (command.data.result) {
      let childArgs = JSON.parse(yield this.queue.__redis__.hgetAsync(`${prefix}${childID}`, 'args'));
      let newArgs   = childArgs.concat([ command.data.result ]);

      transaction.exec.push([ 'hset', `${prefix}${childID}`, 'args', JSON.stringify(newArgs) ]);
    }

    yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
    return;
  }


  // Finish chain
  //
  let transaction = {
    validate: [
      [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
      [ JSON.stringify('idle'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
    ],
    exec: [
      // Increment `children_finished`
      [ 'hincrby', `${prefix}${this.id}`, 'children_finished', 1 ],

      // Move this task to `finished` and update state
      [ 'srem', `${prefix}idle`, this.id ],
      [ 'zadd', `${prefix}finished`, this.removeDelay + time, this.id ],
      [ 'hset', `${prefix}${this.id}`, 'state', JSON.stringify('finished') ],

      // Save result
      [ 'hset', `${prefix}${this.id}`, 'result', JSON.stringify(command.data.result) ],

      // Set progress
      [ 'hset', `${prefix}${this.id}`, 'progress', this.total ]
    ]
  };

  if (this.parent) {
    // Send command with result to parent
    transaction.exec.push([ 'zadd', `${prefix}${this.parent_pool}:commands`, time, Command.fromObject({
      to:     this.parent,
      to_uid: this.parent_uid,
      type:   'result',
      data:   command.data.result
    }).toJSON() ]);
  }

  let res = yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));

  if (res) {
    let eventData = { id: this.id, uid: this.uid };

    this.queue.emit('task:end', eventData);
    this.queue.emit(`task:end:${this.id}`, eventData);
  }
});


ChainTemplate.serializableFields = TaskTemplate.serializableFields.concat([
  'children',
  'children_finished'
]);


// Task class factory
//
ChainTemplate.extend = function (options) {
  class T extends ChainTemplate {}

  Object.assign(T.prototype, options);

  return T;
};


module.exports = ChainTemplate;
