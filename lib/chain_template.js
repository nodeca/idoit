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


ChainTemplate.prototype.init = Promise.coroutine(function* () {
  this.id = this.taskID();

  if (this.__user_init__) {
    // __user_init__ can be simple sync function
    yield Promise.resolve().then(() => this.__user_init__());
  }

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
      t.parent     = this.id;
      t.parent_uid = this.uid;

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
  let childID  = this.children[0];
  let childUid = JSON.parse(yield this.queue.__redis__.hgetAsync(`${prefix}${childID}`, 'uid'));

  let transaction = {
    validate: [
      [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
      [ JSON.stringify('waiting'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
    ],
    exec: [
      // Move this task to `idle` and update state
      [ 'srem', `${prefix}waiting`, this.id ],
      [ 'sadd', `${prefix}idle`, this.id ],
      [ 'hset', `${prefix}${this.id}`, 'state', JSON.stringify('idle') ],

      // Send `activate` command to first child
      [ 'zadd', `${prefix}commands`, time, Command.fromObject({
        to:     childID,
        to_uid: childUid,
        type:   'activate'
      }).toJSON() ]
    ]
  };

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
    let childUid = JSON.parse(yield this.queue.__redis__.hgetAsync(`${prefix}${childID}`, 'uid'));

    let transaction = {
      validate: [
        [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
        [ JSON.stringify('idle'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
      ],
      exec: [
        // Increment `children_finished`
        [ 'hincrby', `${prefix}${this.id}`, 'children_finished', 1 ],

        // Send `activate` command to child
        [ 'zadd', `${prefix}commands`, time, Command.fromObject({
          to:     childID,
          to_uid: childUid,
          type:   'activate'
        }).toJSON() ]
      ]
    };

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
      [ 1, [ 'zrem', `${prefix}commands_locked`, command.toJSON() ] ],
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
    transaction.exec.push([ 'zadd', `${prefix}commands`, time, Command.fromObject({
      to:     this.parent,
      to_uid: this.parent_uid,
      type:   'result',
      data:   command.data.result
    }).toJSON() ]);
  }

  yield this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
});


ChainTemplate.serializableFields = TaskTemplate.serializableFields.concat([
  'children',
  'children_finished'
]);


// Task class factory
//
ChainTemplate.extend = function (options) {
  class T extends ChainTemplate {}

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


module.exports = ChainTemplate;
