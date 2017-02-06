/* global describe, it, beforeEach, afterEach */
'use strict';


const assert = require('assert');
const bb     = require('bluebird');

const Queue  = require('../index');
const random = require('../lib/utils').random;


const REDIS_URL = 'redis://localhost:6379/3';


function delay(ms) { return bb.delay(ms); }


const clear_namespace = bb.coroutine(function* (ns) {
  const r = require('redis').createClient(REDIS_URL);
  const keys = yield r.keysAsync(`${ns}*`);

  if (keys.length) yield r.delAsync(keys);
});


describe('chain', function () {

  let q, q_ns;

  beforeEach(bb.coroutine(function* () {
    q_ns = `idoit_test_${random(6)}:`;

    q = new Queue({ redisURL: REDIS_URL, ns: q_ns });

    // Helper to wait task finish
    q.wait = bb.coroutine(function* (id) {
      let task = yield this.getTask(id);

      while (task.state !== 'finished') {
        yield delay(50);
        task = yield this.getTask(id);
      }

      return task;
    });

    q.on('error', err => { throw err; });

    yield q.start();
  }));

  afterEach(bb.coroutine(function* () {
    q.shutdown();
    yield clear_namespace(q_ns);
  }));


  it('should run children in correct order', function (done) {
    let run = [ false, false, false ];

    q.registerTask('t1', function () {
      run[0] = true;
      assert.deepEqual(run, [ true, false, false ]);
    });

    q.registerTask('t2', function () {
      run[1] = true;
      assert.deepEqual(run, [ true, true, false ]);
    });

    q.registerTask('t3', function () {
      run[2] = true;
      assert.deepEqual(run, [ true, true, true ]);

      setTimeout(done, 10);
    });

    q.chain([
      q.t1(),
      q.t2(),
      q.t3()
    ]).run();
  });


  it('should pass data between tasks', bb.coroutine(function* () {
    q.registerTask('mult', (a, b) => a * b);
    q.registerTask('add', (a, b) => a + b);
    q.registerTask('sub', (a, b) => a - b);

    let id = yield q.chain([
      q.mult(1, 2),
      q.add(3),
      q.sub(7)
    ]).run();

    let task = yield q.wait(id);

    assert.equal(task.result, 2);
  }));


  it('should set progress in chain', bb.coroutine(function* () {
    q.registerTask({
      name: 't1',
      process() {},
      init() {
        this.total = 3;
      }
    });

    q.registerTask({
      name: 't2',
      process() {},
      init() {
        this.total = 2;
      }
    });


    let id = yield q.chain([
      q.t1(),
      q.t2()
    ]).run();

    let task = yield q.wait(id);

    assert.equal(task.total, 5);
    assert.equal(task.progress, 5);
  }));


  it('should handle subtask error', bb.coroutine(function* () {
    let t1Calls = 0;

    q.removeAllListeners('error');
    // replace existing error throw with filtered one
    q.on('error', err => { if (!String(err).includes('<!test err!>')) throw err; });

    q.registerTask('t1', () => { t1Calls++; });
    q.registerTask({
      name: 't2',
      process() { throw new Error('<!test err!>'); },
      retryDelay: 10
    });

    let t2 = q.t2();
    let id = yield q.chain([
      q.t1(),
      q.t1(),
      t2,
      q.t1(),
      q.t1()
    ]).run();

    let task = yield q.wait(id);

    t2 = yield q.getTask(t2.id);

    assert.equal(t1Calls, 2);
    assert.ok(task.error.message.includes('<!test err!>'));
    assert.ok(t2.error.message.includes('<!test err!>'));
  }));


  it('should pass result of group to next task', bb.coroutine(function* () {
    q.registerTask('t1', (a, b) => a * b);
    q.registerTask('t2', groupResults => groupResults.reduce((acc, curr) => acc + curr, 0));

    let id = yield q.chain([
      q.group([ q.t1(2, 3), q.t1(4, 1), q.t1(7, 2) ]),
      q.t2()
    ]).run();

    let task = yield q.wait(id);

    assert.equal(task.result, 24);
  }));


  it('should fail with empty children', function () {
    return q.chain([]).run().then(
      () => { throw new Error('should fail'); },
      () => {}
    );
  });


  it('should run user init', bb.coroutine(function* () {
    let calls = 0;

    q.registerTask('t1', () => {});

    q.registerTask({
      name: 't2',
      baseClass: Queue.ChainTemplate,
      init: () => { calls++; }
    });

    let id = yield q.t2([ q.t1() ]).run();
    yield q.wait(id);

    assert.equal(calls, 1);
  }));


  it('`.cancel()` should emit "task:end" event for all unfinished tasks in chain', bb.coroutine(function* () {
    q.registerTask({ name: 't1', taskID: () => 't1', process: () => {} });
    q.registerTask({ name: 't2', taskID: () => 't2', process: () => delay(1000000) });
    q.registerTask({ name: 't3', taskID: () => 't3', process: () => {} });

    let id = yield q.chain([ q.t1(), q.t2(), q.t3() ]).run();

    // wait for t1 to finish
    yield new Promise(resolve => q.once('task:end:t1', resolve));

    let finished_tasks = [];

    q.on('task:end', function (task_info) {
      finished_tasks.push(task_info.id);
    });

    yield q.cancel(id);

    assert.deepEqual(finished_tasks, [ 't2', 't3', id ]);
  }));
});
