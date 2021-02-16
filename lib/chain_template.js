'use strict';


const TaskTemplate = require('./task_template');
const Command      = require('./command');
const utils        = require('./utils');


class ChainTemplate extends TaskTemplate{
  constructor(queue, ...args) {
    // This code can be called in 3 ways:
    //
    // 1. Direct run as `queue.chain([ subtask1, subtask2, ...])
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
      // If we are here - assume user called `queue.chain([ subtask1, ...])`
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
      return Promise.reject(new Error('ido error: you should specify chain children'));
    }

    this.children          = [];
    this.children_finished = 0;
    this.total             = 0;

    // Initialize children, link to parent & count progress total
    return Promise.all(this.__children_to_init__).then(() => {
      this.__children_to_init__.forEach(t => {
        this.total  += t.total;
        t.setParent(this);

        this.children.push(t.id);
      });
    });
  }


  // Handle `activate` command
  //
  // - move chain from `waiting` to `idle`
  // - send `activate` command to first child
  //
  async handleCommand_activate(command) {
    let prefix   = this.queue.__prefix__;
    let time     = utils.redisToMs(await this.queue.__redis__.time());

    let rawChild = await this.queue.__getRawTask__(this.children[0]);

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

    await this.queue.__redis__.eval(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
  }


  // Handle child result
  //
  async handleCommand_result(command) {
    let prefix = this.queue.__prefix__;
    let time   = utils.redisToMs(await this.queue.__redis__.time());


    // Run next children task
    //
    if (this.children_finished + 1 < this.children.length) {
      let childID  = this.children[this.children_finished + 1];

      let rawChild = await this.queue.__getRawTask__(childID);

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
        let childArgs = JSON.parse(await this.queue.__redis__.hget(`${prefix}${childID}`, 'args'));
        let newArgs   = childArgs.concat([ command.data.result ]);

        transaction.exec.push([ 'hset', `${prefix}${childID}`, 'args', JSON.stringify(newArgs) ]);
      }

      await this.queue.__redis__.eval(this.queue.__scripts__.transaction, 1, JSON.stringify(transaction));
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

        // Set progress
        [ 'hset', `${prefix}${this.id}`, 'progress', this.total ]
      ]
    };

    // Save result
    if (typeof command.data.result !== 'undefined') {
      transaction.exec.push([ 'hset', `${prefix}${this.id}`, 'result', JSON.stringify(command.data.result) ]);
    }

    if (this.parent) {
      // Send command with result to parent
      transaction.exec.push([ 'zadd', `${prefix}${this.parent_pool}:commands`, time, Command.fromObject({
        to:     this.parent,
        to_uid: this.parent_uid,
        type:   'result',
        data:   { id: this.id, result: command.data.result }
      }).toJSON() ]);
    }

    let success = await this.queue.__redis__.eval(
      this.queue.__scripts__.transaction,
      1,
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

module.exports = ChainTemplate;
