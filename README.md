ido
===

[![Build Status](https://img.shields.io/travis/nodeca/ido/master.svg?style=flat)](https://travis-ci.org/nodeca/ido)
[![NPM version](https://img.shields.io/npm/v/ido.svg?style=flat)](https://www.npmjs.org/package/ido)
[![Coverage Status](https://coveralls.io/repos/github/nodeca/ido/badge.svg?branch=master)](https://coveralls.io/github/nodeca/ido?branch=master)


> Redis-backed task queue engine with advanced task control and eventual consistency.

- Task grouping, chaining, iterators for huge ranges.
- Postponed & scheduled task run.
- Load distribution + worker pools.
- Easy to embed.


Features in details
-------------------

`ido` provides advanced control to implement so

**Grouping**. Special `group` task execute children tasks and wait until all
complete. Useful for map/reduce logic.

**Chaining**. Special `chain` task execute children one-by-one. Also useful for
map-reduce or splitting very complicated tasks to more simple steps.

**Mapping iterator**. Special feature for huge payloads, to produce chunks on
demand. Benefits:

- No lags on mapping phase, chunks processing starts immediately.
- Easy to optimize DB queries to build chunks of equal size
  (skip + limit queries are very slow on huge data).

**Progress**. When you use groups/chain/map scenarios, it's easy to monitor
total progress via top parent. Long standalone tasks also can notify user about
progress change.

**Worker pools**. You can split tasks by different processes. For example,
if you don't wish heavy tasks to block light ones.

**Sheduler**. Built-in cron allows to execute tasks on given schedule.


## Data consistency

- All data in redis are evertually consistent.
- Task can not be lost, but CAN run twice on edge cases (if process crash when
  task function was about to finish)
- Progress can count "faster" if `task.progressAdd()` used and process crash
  before task complete. But that's not critical, since such info can be used
  only for interface progress bars updates. In most cases you will not see the
  difference.


Install
-------

`node.js` 6+ and `redis` 3.0+ required.

```sh
npm install ido --save
```


API
---

### new Queue({ redisURL, concurrency = 100, name = 'default', ns = 'idoqueue:' })

 - **redisURL** (String) - redis connection url.
 - **concurrency** (Number) - max tasks to consume in parallel
   by single worker, 100 by default.
 - **pool** (String) - worker pool name, "default" if not set. Used if
   this queue instance consumes tasks only (after `.start()`). You
   can route tasks to specific pools of workers to avoid unwanted locks.
   You can set `pool` to Array, `[ 'pool1', 'pool2' ]` to consume tasks from
   several pools (for development/testing purposes).
 - **ns** (String) - data namespace, currently used as redis keys prefix,
   "idoqueue:" by default.

It's a good practice to have separate worker pools for heavy blocking tasks and
non-blocking ones. For example, nobody should block sending urgent emails. So,
create several worker processes, pin those to different pools and set proper
tasks concurrency. Non-blocking tasks can be cunsumed in parallel, and you can
be ok with default `concurrency` = 100. Blocking tasks should be consumed
one-by-one, set `concurrency` = 1 for those workers.

__Note.__ It may happen, that you remove some task types from your app. In this
case orphaned data will be wiped after 3 days.


### .registerTask(options), .registerTask(name [, cron], process)

Options:

 - **name** (String) - the task's name.
 - **baseClass** (Function) - optional, base task's constructor, "Task" by default.
 - **init** (Function) - optional, used for async task initialization, should return `Promise`
   - **this** (Object) - current task (task total is available as `this.total`).
 - **taskID** (Function) - optional, should return new task id. Needed only for
   creating "exclusive" tasks, return random value by default, called as:
   `function (taskData)`. Sugar: if you pass plain string, it will be wrapped to
   function, that always return this string.
 - **process** (Function) - main task function, called as: `task.process(...args)`. Should return `Promise`
   - **this** (Object) - current task.
 - **retry** (Number) - optional, number of retry on error, default 2.
 - **retryDelay** (Number) - optional, delay in ms after retries, default 60000 ms.
 - **timeout** (Number) - optional, execution timeout, default 120000 ms.
 - **total** (Number) - optional, max progress value, default 1. If you don't
   modify behaviour progress starts with 0 and become 1 on task end.
 - **postponeDelay** (Number) - optional, if postpone is called without delay,
   delay is assumed to be equal to this value (in milliseconds).
 - **cron** (String) - optional, cron string ("15 */6 * * *"), default null.
 - **track** (Number) - default 3600000ms (1hr). Time to remember scheduled
   tasks from cron to avoid rerun if several servers in cluster have wrong
   clocks. Don't set too high for very frequent tasks, because it can occupy
   a lot of memory.

### .getTask(id)

Get task by id. Returns a Promise resolved with task or with `null` if task not exist.

__Task fields you can use:__

- **total** - total task progress
- **progress** - current task progress
- **result** - the task result
- **error** - the task error


### .cancel(id)

Cancel task. Returns a Promise resolved with task.

__Note.__ You can cancel only tasks without parent.


### .start()

Start worker and begin task data consume. Return `Promise`, resolved when queue
is ready (call `.ready()` inside).

If `pool` was specified in cunstructor, only tasks routed to this pull will
be consumed.


### .shutdown()

Stop accepting new tasks from queue. Return `Promise`, resolved when all active
tasks in this worker complete.


### .ready()

Return `Promise`, resolved when queue is ready to operate (after 'connect'
event, see below).


### .options(opts)

Update constructor options, except redisURL.


### .on('eventName', handler)

`ido` is an `EventEmitter` instance that fires some events:

- `ready` when redis connection is up and commands can be executed
  (tasks can be registered without connection)
- `error` when an error has occured.
- `task:progress`, `task:progress:<task_id>` - when task update progress.
  Event data is: { id, uid, total, progress }
- `task:end`, `task:end:<task_id>` - when task end. Event data is: { id, uid }


### .\<taskName\>(...params)

Create new Task with optional params.


### task.options({ ... })

Override task properties. For example, you may wish to assign specific
group/chain tasks to another pool.


### task.run()

Run task immediately. Returns a Promise resolved with task id.


### task.postpone([delay])

Postpone task execution to `delay` milliseconds (or to `task.postponeDelay`).

Returns a Promise resolved with task id.


### task.progressAdd(value)

Increment current task progress.

Returns a Promise resolved with task id.


### task.setDeadline(timeLeft)

Update current task deadline.

Returns a Promise resolved with task id.


Special tasks
-------------

### group

Create a new task, executing children in parallel.

```javascript
queue.group([
  queue.children1(),
  queue.children2(),
  queue.children3()
]).run()
```

Group result is unsorted array of children result.

### chain

Create a new task, executing children in series. If any of children fails -
chain fails too.

```javascript
queue.registerTask('multiply', (a, b) => a * b);
queue.registerTask('subtract', (a, b) => a - b);

queue.chain([
  queue.multiply(2, 3), // 2 * 3 = 6
  queue.subtract(10),   // 10 - 6 = 4
  queue.multiply(3)     // 3 * 4 = 12
]).run()
```

Result of previous task pass as last argument of next task. Result of chain
is result of last task in chain.

### iterator

A special way to run huge mapping in lazy style (on demand). See comments below.

```javascript
// register iterator task
queue.registerTask({
  name: 'lazy_mapper',
  baseClass: Queue.Iterator,
  // This method is called on task begin and on every child end. It can be
  // a generator function or function that return `Promise`.
  * iterate(state) {
    // ...

    // Three types of output states possible: ended, do nothing & new data.
    //
    // 1. `null` - end reached, iterator should not be called anymore.
    // 2. `{}`   - idle, there are enougth subtasks in queue, try to call
    //             iterator later (when next child finish).
    // 3. {
    //      state    - new iterator state to remember (for example, offset for
    //                 db query), any serializeable data
    //      tasks    - array of new subtasks to push into queue
    //    }
    //
    // IMPORTANT! Iterator can be called in parallel from different workers. We
    // use input `state` to resolve collisions on redis update. So, if you
    // create new subtasks:
    //
    // 1. new `state` MUST be different (for all previous states)
    // 2. `tasks` array MUST NOT be empty.
    //
    // In other case you should signal about 'end' or 'idle'.
    //
    // Invalid combination will cause 'end' + error event.
    //
    return {
      state: newState,
      tasks: chunksArray
    };
  }
});

// run iterator
queue.lazy_mapper().run();
```

Why this crazy magic was invented?

Imagine that you need to rebuild 10 millions of forum posts. You wish to split
work by equal small chunks, but posts have no sequential integer enumeration,
only mongo IDs. What can you do?

- Direct `skip` + `limit` requests are very expensive on big collections in any
  database.
- You can not split by date intervals, because posts density varies a lot from
  first to last post.
- You can add an indexed field with random number to every post. Then split by
  intervals. That will work, but will cause random disk access - not cool.

Solution is to use iterative mapper, wich can remember "previous position". In
this case, you will do `range` + `limit` requests instead of `skip` + `limit`.
That works well with databases. Additional bonuses are:

- You do not need to keep all subtasks in queue. For example, you can create
  100 chunks and add next 100 when previous are about to finish.
- Mapping phase become distributed and you can start monitoring total progress
  immediately.


Why one more queue?
-------------------

Of cause, we are familiar with [kue](https://github.com/Automattic/kue),
[celery](http://www.celeryproject.org/) and [akka](http://akka.io/).
Our target was have a balance between simplicity and power. So, we don't know
if `ido` works well in cluster with thousands of instances. But it should be ok
in smaller volumes and it's really easy to use.

[kue](https://github.com/Automattic/kue) was not ok for our needs, because:

- it's "priorities" concept is not flexible and does not protect well from
  locks by heavy tasks
- no task grouping/chaining and so on
- no strong guarantees of data consistency

In `ido` we cared about:

- task group/chain operations & pass data between tasks (similar to celery)
- worker pools to isolate task execution by types.
- easy to use & install (only redis needed, can run in existing process)
- eventual consistency of stored data
- essential sugar like built-in scheduler
- iterative mapper for huge payloads (unique feature, very useful for many
  maintenance tasks)
- task progress tracking
- avoid global locks

Redis still can be a point of failure, but that's acceptable price for
simplicity. Of cause you can get a better availability via distributed message
buses like RMQ. But in many cases it's more important to keep things simple.
With `ido` you can reuse existing technologies without additional expences.
