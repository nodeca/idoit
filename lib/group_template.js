'use strict';


const serializeError = require('serialize-error').serializeError;

const TaskTemplate   = require('./task_template');
const QueueError     = require('./error');
const Command        = require('./command');
const utils          = require('./utils');


class GroupTemplate extends TaskTemplate {
  constructor(queue, ...args) {
    // This code can be called in 3 ways:
    //
    // 1. Direct run as `queue.group([ subtask1, subtask2, ...])
    // 2. From inherited task, extended via class/prototype
    // 3. From inherited task, quick-extended via .init() override
    //
    // We need 2 things:
    //
    // - Keep this.args serializeable
    // - Prepare list of children to init
    //
    // So, we just check content of first params, to decide what to do.
    // That's a bit dirty, but good enougth. If user need some very
    // specific signature in inherited task, he can always customize data after
    // parent (this) constructor call
    //
    let __children_memoise;

    if (Array.isArray(args[0]) && args[0].length && args[0][0] instanceof TaskTemplate) {
      // If we are here - assume user called `queue.group([ subtask1, ...])`
      __children_memoise = args[0];
      args[0] = null;
    }

    super(queue, ...args);

    // Temporary child instances store for init/prepare methods
    this.__children_to_init__ = __children_memoise || [];

    this.__serializableFields = this.__serializableFields.concat([
      'children',
      'children_finished'
    ]);
  }


  // (internal) Prepare task prior to run. Used to modify templates
  // behaviour on inherit.
  //
  // !!! Don't touch this method, override `.init()` to extend
  // registered tasks.
  //
  async prepare() {
    this.id = this.taskID();

    // .init() can be simple sync function
    // Specially for group & chain templates - it can return
    // list of children to init.
    let _children = await Promise.resolve().then(() => this.init());

    if (Array.isArray(_children)) this.__children_to_init__ = _children;


    if (!this.__children_to_init__.length) {
      return Promise.reject(new Error('Queue error: you should specify group children'));
    }

    this.children          = [];
    this.children_finished = 0;
    this.total             = 0;
    this.result            = [];

    // Initialize children, link to parent & count progress total
    return Promise.all(this.__children_to_init__).then(() => {
      this.__children_to_init__.forEach(t => {
        this.total  += t.total;
        t.setParent(this);

        this.children.push(t.id);
      });
    });
  }


  // Handle `activate` command.
  //
  // - move group from `waiting` to `idle`
  // - send `activate` command to all children
  //
  async handleCommand_activate(command) {
    let prefix = this.queue.__prefix__;
    let time   = utils.redisToMs(await this.queue.__redis__.time());

    let transaction = {
      if: [
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
    let rawChildren = await this.queue.__getRawTasks__(this.children);

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

    await this.queue.__redis__.transaction(JSON.stringify(transaction));
  }


  // Handle child result
  //
  async handleCommand_result(command) {
    let prefix = this.queue.__prefix__;
    let time   = utils.redisToMs(await this.queue.__redis__.time());


    // Update group:
    //
    // - increment `children_finished`
    // - send `group_check` command
    //
    await this.queue.__redis__.transaction(
      JSON.stringify({
        if: [
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
  }


  // Check group finished
  //
  async handleCommand_group_check(command) {
    // If group not finished - skip
    if (this.children_finished < this.children.length) return;

    let prefix = this.queue.__prefix__;
    let time = utils.redisToMs(await this.queue.__redis__.time());

    let transaction = {
      if: [
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
    let rawChildren = await this.queue.__getRawTasks__(this.children);


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

      let success = await this.queue.__redis__.transaction(
        JSON.stringify(transaction)
      );

      if (success) {
        let eventData = { id: this.id, uid: this.uid };

        this.queue.emit('task:end', eventData);
        this.queue.emit(`task:end:${this.id}`, eventData);
      }

      return;
    }


    // Save array of children results and send command to parent
    //
    let result = rawChildren.map(child => child.result);

    // Set progress
    transaction.exec.push([ 'hset', `${prefix}${this.id}`, 'progress', this.total ]);

    if (typeof result !== 'undefined') {
      transaction.exec.push([ 'hset', `${prefix}${this.id}`, 'result', JSON.stringify(result) ]);
    }

    if (this.parent) {
      // Send command with result to parent
      transaction.exec.push([ 'zadd', `${prefix}${this.parent_pool}:commands`, time, Command.fromObject({
        to:     this.parent,
        to_uid: this.parent_uid,
        type:   'result',
        data:   { id: this.id, result }
      }).toJSON() ]);
    }

    let success = await this.queue.__redis__.transaction(
      JSON.stringify(transaction)
    );

    if (success) {
      let eventData = { id: this.id, uid: this.uid };

      this.queue.emit('task:end', eventData);
      this.queue.emit(`task:end:${this.id}`, eventData);
    }
  }


  async __fillCancelInfo__() {
    let result = [];

    let children = await this.queue.__redis__.hget(`${this.queue.__prefix__}${this.id}`, 'children');

    children = children ? JSON.parse(children) : [];

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


  // Task class factory
  //
  static extend(options) {
    class T extends this {}

    Object.assign(T.prototype, options);

    return T;
  }
}

module.exports = GroupTemplate;
