'use strict';


const serializeError = require('serialize-error').serializeError;

const TaskTemplate   = require('./task_template');
const QueueError     = require('./error');
const Command        = require('./command');
const utils          = require('./utils');

let scripts = {};

class IteratorTemplate extends TaskTemplate {
  constructor(queue, ...args) {
    super(queue, ...args);

    this.__scripts__ = scripts;

    this.children_created  = 0;
    this.children_finished = 0;

    this.user_data = {};

    this.__serializableFields = this.__serializableFields.concat([
      'children_created',
      'children_finished',
      'user_data'
    ]);
  }


  // Handle `activate` command
  //
  async handleCommand_activate(command) {
    let prefix = this.queue.__prefix__;
    let time = utils.redisToMs(await this.queue.__redis__.time());

    await this.queue.__redis__.transaction(
      JSON.stringify({
        if: [
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
  }


  // Handle child finished
  //
  async handleCommand_result(command) {
    let prefix = this.queue.__prefix__;
    let time   = utils.redisToMs(await this.queue.__redis__.time());

    // - increment `children_finished`
    // - send `iterate` command
    //
    await this.queue.__redis__.transaction(
      JSON.stringify({
        if: [
          [ 1, [ 'zrem', `${prefix}${this.pool}:commands_locked`, command.toJSON() ] ],
          [ JSON.stringify('idle'), [ 'hget', `${prefix}${this.id}`, 'state' ] ]
        ],
        exec: [
          [ 'srem', `${prefix}ichunks:${this.id}`, command.data.id ],
          [ 'hincrby', `${prefix}${this.id}`, 'children_finished', 1 ],
          [ 'zadd', `${prefix}${this.pool}:commands`, time, Command.fromObject({
            to:     this.id,
            to_uid: this.uid,
            type:   'iterate'
          }).toJSON() ]
        ]
      })
    );
  }


  // Handle iterate command
  //
  async handleCommand_iterate(command) {
    // Run iterate
    //
    let iterateResult = await Promise.resolve().then(() => this.iterate(this.user_data.state));

    let time   = utils.redisToMs(await this.queue.__redis__.time());
    let prefix = this.queue.__prefix__;


    // Handle iterate finished
    //
    if (!iterateResult) {
      if (this.children_finished >= this.children_created) {
        let transaction = {
          if: [
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
          await this.queue.__redis__.transaction(JSON.stringify(transaction));

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

      let transaction = {
        if: [
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

      // Add all newly created children to "children" list;
      // no need for ordering because it is done by queue itself
      transaction.exec.push(
        [ 'sadd', `${prefix}ichunks:${this.id}` ]
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

      await this.queue.__redis__.transaction(JSON.stringify(transaction));
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
      if: [
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

    await this.queue.__redis__.transaction(JSON.stringify(transaction));
  }


  async __fillCancelInfo__() {
    let prefix = this.queue.__prefix__;
    let result = [];

    let children = await this.queue.__redis__.smembers(`${prefix}ichunks:${this.id}`);

    await Promise.all(children.map(async id => {
      let task = await this.queue.getTask(id);

      if (task === null) return;

      let childrenList = await task.__fillCancelInfo__();
      result = result.concat(childrenList);
    }));

    let childrenList = await super.__fillCancelInfo__();
    result = result.concat(childrenList);

    return result;
  }


  static extend(options) {
    if (typeof options.iterate !== 'function') {
      throw new Error('idoit error: "iterate" should be a function');
    }

    class T extends this {}

    Object.assign(T.prototype, options);

    return T;
  }
}

module.exports = IteratorTemplate;
