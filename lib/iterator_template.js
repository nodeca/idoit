'use strict';


const inherits       = require('util').inherits;
const serializeError = require('serialize-error').serializeError;

const TaskTemplate   = require('./task_template');
const QueueError     = require('./error');
const Command        = require('./command');
const utils          = require('./utils');

let scripts = {};

scripts.hvaladd = `
-- Syntax: EVALSHA sha1sum(hvaladd) 1 hash field value [value ...]
--
-- If HASH has a FIELD with value as an array serialized to JSON,
-- this script will push a new VALUE into that array.
--
-- Example:
--
-- > hset test val "[1,2,3]"
-- > eval ... 1 test val foo
-- > hgetall test
-- 1) "val"
-- 2) "[1,2,3,\"foo\"]"
--

local hash, key = KEYS[1], ARGV[1]
local array = cjson.decode(redis.call('hget', hash, key))

for i = 2, #ARGV do
  array[#array + 1] = ARGV[i]
end

return redis.call('hset', hash, key, cjson.encode(array))
`;


scripts.hvalrem = `
-- Syntax: EVALSHA sha1sum(hvalrem) 1 hash field value [value ...]
--
-- If HASH has a FIELD with value as an array serialized to JSON,
-- this script will remove a VALUE from that array.
--
-- Example:
--
-- > hset test val '[1,"foo",2]'
-- > eval ... 1 test val foo
-- > hgetall test
-- 1) "val"
-- 2) "[1,2]"
--

local hash, key = KEYS[1], ARGV[1]
local array = cjson.decode(redis.call('hget', hash, key))
local newarray = {}
local toremove = {}

for i = 2, #ARGV do
  toremove[ARGV[i]] = true
end

for i = 1, #array do
  if not toremove[array[i]] then
    newarray[#newarray + 1] = array[i]
  end
end

return redis.call('hset', hash, key, #newarray > 0 and cjson.encode(newarray) or '[]')
`;


function IteratorTemplate(queue, ...args) {
  TaskTemplate.call(this, queue, ...args);

  this.__scripts__ = scripts;

  this.children          = [];
  this.children_created  = 0;
  this.children_finished = 0;

  this.user_data = {};
}


inherits(IteratorTemplate, TaskTemplate);


// Handle `activate` command
//
IteratorTemplate.prototype.handleCommand_activate = async function (command) {
  let prefix = this.queue.__prefix__;
  let time = utils.redisToMs(await this.queue.__redis__.timeAsync());

  await this.queue.__redis__.evalAsync(
    this.queue.__scripts__.transaction,
    1,
    JSON.stringify({
      validate: [
        [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
        [ JSON.stringify('waiting'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
      ],
      exec: [
        // Move this task to `idle` and update state
        [ 'srem', `${prefix}waiting`, this.id ],
        [ 'sadd', `${prefix}idle`, this.id ],
        [ 'hset', `${prefix}${this.id}`, 'state', JSON.stringify('idle') ],

        // Add iterate command
        [ 'zadd', `${prefix}${this.pool}:commands`, time, Command.fromObject({
          to:     this.id,
          to_uid: this.uid,
          type:   'iterate'
        }).toJSON() ]
      ]
    })
  );
};


// Handle child finished
//
IteratorTemplate.prototype.handleCommand_result = async function (command) {
  let prefix = this.queue.__prefix__;
  let time   = utils.redisToMs(await this.queue.__redis__.timeAsync());

  // TODO: load script more efficiently, maybe as a part of multi
  await this.queue.__redis__.scriptAsync('load', this.__scripts__.hvalrem);

  // - increment `children_finished`
  // - send `iterate` command
  //
  await this.queue.__redis__.evalAsync(
    this.queue.__scripts__.transaction,
    1,
    JSON.stringify({
      validate: [
        [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
        [ JSON.stringify('idle'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
      ],
      exec: [
        [ 'eval', this.__scripts__.hvalrem, 1, `${prefix}${this.id}`, 'children', command.data.from ],
        [ 'hincrby', `${prefix}${this.id}`, 'children_finished', 1 ],
        [ 'zadd', `${prefix}${this.pool}:commands`, time, Command.fromObject({
          to:     this.id,
          to_uid: this.uid,
          type:   'iterate'
        }).toJSON() ]
      ]
    })
  );
};


// Handle iterate command
//
IteratorTemplate.prototype.handleCommand_iterate = async function (command) {
  // Run iterate
  //
  let iterateResult = await Promise.resolve().then(() => this.iterate(this.user_data.state));

  let time   = utils.redisToMs(await this.queue.__redis__.timeAsync());
  let prefix = this.queue.__prefix__;


  // Handle iterate finished
  //
  if (!iterateResult) {
    if (this.children_finished >= this.children_created) {
      let transaction = {
        validate: [
          [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
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
        transaction.exec.push([ 'zadd', `${prefix}${this.parent_pool}:commands`, time, Command.fromObject({
          to:     this.parent,
          to_uid: this.parent_uid,
          type:   'result',
          data:   { id: this.id }
        }).toJSON() ]);
      }

      let success =
        await this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));

      if (success) {
        let eventData = { id: this.id, uid: this.uid };

        this.queue.emit('task:end', eventData);
        this.queue.emit(`task:end:${this.id}`, eventData);
      }
    }

    return;
  }


  // Idle
  if (!Object.keys(iterateResult).length) {
    return;
  }


  // Check iterate result and add new chunks
  //
  // - state should be changed
  // - `iterate` should return new tasks
  //
  if (typeof iterateResult.state !== 'undefined' &&
      JSON.stringify(iterateResult.state) !== JSON.stringify(this.user_data.state) &&
      iterateResult.tasks &&
      iterateResult.tasks.length) {

    // TODO: load script more efficiently, maybe as a part of multi
    await this.queue.__redis__.scriptAsync('load', this.__scripts__.hvaladd);

    let transaction = {
      validate: [
        [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
        [ JSON.stringify('idle'), [ 'hget', `${prefix}${this.id}`, 'state' ] ],
        [ JSON.stringify(this.user_data), [ 'hget', `${prefix}${this.id}`, 'user_data' ] ]
      ],
      exec: [
        [ 'hincrby', `${prefix}${this.id}`, 'children_created', iterateResult.tasks.length ],
        [ 'hset', `${prefix}${this.id}`, 'user_data', JSON.stringify({ state: iterateResult.state }) ]
      ]
    };

    // Initialize children, link to parent
    await Promise.all(iterateResult.tasks);

    for (let i = 0; i < iterateResult.tasks.length; i++) {
      iterateResult.tasks[i].setParent(this);

      // Send `activate` command to chunks (without it's children)
      transaction.exec.push([ 'zadd', `${prefix}${iterateResult.tasks[i].pool}:commands`, time, Command.fromObject({
        to:     iterateResult.tasks[i].id,
        to_uid: iterateResult.tasks[i].uid,
        type:   'activate'
      }).toJSON() ]);
    }

    // Add all newly created children to "children" list
    transaction.exec.push(
      [ 'eval', this.__scripts__.hvaladd, 1, `${prefix}${this.id}`, 'children' ]
        .concat(iterateResult.tasks.map(task => task.id))
    );

    let taskQueue = [].concat(iterateResult.tasks);

    // Add chunks & their children
    while (taskQueue.length > 0) {
      let t = taskQueue.shift();
      let payload = t.toObject();

      Object.keys(payload).forEach(key => {
        transaction.exec.push([ 'hset', `${prefix}${t.id}`, key, JSON.stringify(payload[key]) ]);
      });

      transaction.exec.push([ 'sadd', `${prefix}waiting`, t.id ]);

      taskQueue = taskQueue.concat(t.children || []);
    }

    await this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
    return;
  }


  // If we can't process `iterate` result:
  //
  // - force task to finish
  // - emit error event
  //
  let err = new QueueError(
    new Error('Iterator error: terminating task because bad "iterate" result'),
    this.name,
    'idle',
    this.id,
    this.user_data.state
  );

  this.queue.emit('error', err);

  let transaction = {
    validate: [
      [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
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
    transaction.exec.push([ 'zadd', `${prefix}${this.parent_pool}:commands`, time, Command.fromObject({
      to:     this.parent,
      to_uid: this.parent_uid,
      type:   'error',
      data:   { error: serializeError(err) }
    }).toJSON() ]);
  }

  await this.queue.__redis__.evalAsync(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
};


IteratorTemplate.serializableFields = TaskTemplate.serializableFields.concat([
  'children', // this is only a list of active children (added on iterate, removed when they finish)
  'children_created',
  'children_finished',
  'user_data'
]);


IteratorTemplate.extend = function (options) {
  if (typeof options.iterate !== 'function') {
    throw new Error('idoit error: "iterate" should be a function');
  }

  class T extends IteratorTemplate {}

  Object.assign(T.prototype, options);

  return T;
};


module.exports = IteratorTemplate;
