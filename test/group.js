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


describe('group', function () {

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


  it('should run all children at same time', bb.coroutine(function* () {
    let run = [ false, false, false ];

    q.registerTask('t1', function () {
      run[0] = true;
    });

    q.registerTask('t2', function () {
      run[1] = true;
    });

    q.registerTask('t3', function () {
      run[2] = true;
    });

    let id = yield q.group([
      q.t1(),
      q.t2(),
      q.t3()
    ]).run();

    yield q.wait(id);

    assert.deepEqual(run, [ true, true, true ]);
  }));


  it('should cancel', bb.coroutine(function* () {
    q.registerTask('t1', () => {});
    q.registerTask('t2', () => {});

    let children = [
      q.t1(),
      q.t1(),
      q.t2()
    ];

    let id = yield q.group(children).run();

    yield q.cancel(id);

    let task = yield q.getTask(id);

    assert.equal(task.state, 'finished');
    assert.equal(task.error.code, 'CANCELED');

    for (let i = 0; i < children.length; i++) {
      task = yield q.getTask(children[i].id);
      assert.equal(task.state, 'finished');
      assert.equal(task.error.code, 'CANCELED');
    }
  }));


  it('should save result of children tasks', bb.coroutine(function* () {
    q.registerTask('t1', () => 't1-result');
    q.registerTask('t2', () => 't2-result');
    q.registerTask('t3', () => {});

    let id = yield q.group([ q.t1(), q.t2(), q.t3() ]).run();
    let task = yield q.wait(id);

    assert.ok(task.result.includes('t1-result'));
    assert.ok(task.result.includes('t2-result'));
  }));


  it('should handle subtask error', bb.coroutine(function* () {
    q.removeAllListeners('error');
    // replace existing error throw with filtered one
    q.on('error', err => { if (!String(err).includes('<!test err!>')) throw err; });

    q.registerTask('t1', () => {});
    q.registerTask({
      name: 't2',
      process() { throw new Error('<!test err!>'); },
      retryDelay: 10
    });

    let id = yield q.group([
      q.t1(),
      q.t1(),
      q.t2(),
      q.t1(),
      q.t1()
    ]).run();

    let task = yield q.wait(id);

    assert.ok(task.error.message.includes('<!test err!>'));
  }));


  it('should fail with empty children', function () {
    return q.group([]).run().then(
      () => { throw new Error('should fail'); },
      () => {}
    );
  });


  it('should terminate if children deleted', bb.coroutine(function* () {
    q.removeAllListeners('error');
    q.on('error', () => {});

    q.registerTask({ name: 't1', process() {}, removeDelay: 0 });
    q.registerTask('t2', () => delay(1000));

    let id = yield q.group([ q.t1(), q.t2() ]).run();
    let task = yield q.wait(id);

    assert.equal(task.state, 'finished');
    assert.ok(task.error.message.includes('Group error: terminating task because children deleted'));
  }));


  it('should pop error if children deleted', bb.coroutine(function* () {
    q.removeAllListeners('error');
    q.on('error', () => {});

    q.registerTask({ name: 't1', process() {}, removeDelay: 0 });
    q.registerTask('t2', () => delay(1000));

    let id = yield q.chain([ q.group([ q.t1(), q.t2() ]) ]).run();
    let task = yield q.wait(id);

    assert.equal(task.state, 'finished');
    assert.ok(task.error.message.includes('Group error: terminating task because children deleted'));
  }));


  it('should run user init', bb.coroutine(function* () {
    let calls = 0;

    q.registerTask('t1', () => {});

    q.registerTask({
      name: 't2',
      baseClass: Queue.GroupTemplate,
      init: () => { calls++; }
    });

    let id = yield q.t2([ q.t1() ]).run();
    yield q.wait(id);

    assert.equal(calls, 1);
  }));


  it('should allow custom arguments for task with user init', bb.coroutine(function* () {
    let t1_calls = 0;
    let init_calls = 0;

    q.registerTask('t1', () => { t1_calls++; });

    q.registerTask({
      name: 't2',
      baseClass: Queue.GroupTemplate,
      init() {
        assert.deepEqual(this.args, [ 'foo', 'bar' ]);
        init_calls++;
        return [ q.t1() ];
      }
    });

    let id = yield q.t2('foo', 'bar').run();
    yield q.wait(id);

    assert.equal(t1_calls, 1);
    assert.equal(init_calls, 1);
  }));
});
