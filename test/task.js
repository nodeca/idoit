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


describe('task', function () {

  let q, q_ns;

  beforeEach(bb.coroutine(function* () {
    q_ns = `ido_test_${random(6)}:`;

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


  it('should ignore task if same already added', bb.coroutine(function* () {
    let calls = 0;

    q.registerTask({
      name: 't1',
      process() {
        calls++;
      },
      taskID: () => 't1'
    });

    yield q.t1().run();
    yield q.t1().run();
    let id = yield q.t1().run();

    yield q.wait(id);

    assert.equal(calls, 1);
  }));


  it('should start task if same already finished', bb.coroutine(function* () {
    let calls = 0;

    q.registerTask({
      name: 't1',
      process() {
        calls++;
      },
      taskID: () => 't1'
    });

    let id = yield q.t1().run();

    let task;

    do {
      yield delay(500);
      task = yield q.getTask(id);
    } while (task.state !== 'finished');

    id = yield q.t1().run();

    do {
      yield delay(500);
      task = yield q.getTask(id);
    } while (task.state !== 'finished');

    assert.equal(calls, 2);
  }));


  it('should run with params', function (done) {
    q.registerTask('t1', (arg1, arg2, arg3) => {
      assert.strictEqual(arg1, '123');
      assert.strictEqual(arg2, 'abc');
      assert.strictEqual(arg3, 456);
      done();
    });

    q.t1('123', 'abc', 456).run();
  });


  it('should restart errored', function (done) {
    let retries = 0;

    q.removeAllListeners('error');
    // replace existing error throw with filtered one
    q.on('error', err => { if (!String(err).includes('<!test err!>')) throw err; });

    q.registerTask({
      name: 't1',
      process() {
        if (retries === 0) {
          retries++;
          throw new Error('<!test err!>');
        }

        assert.equal(retries, 1);
        done();
      },
      retryDelay: 10
    });

    q.t1().run();
  });


  it('should finish after 2 retries', function (done) {
    let retries = 0;

    q.removeAllListeners('error');
    // replace existing error throw with filtered one
    q.on('error', err => { if (!String(err).includes('<!test err!>')) throw err; });

    q.registerTask({
      name: 't1',
      process() {
        retries++;

        if (retries >= 2) {
          done();
        }

        throw new Error('<!test err!>');
      },
      retryDelay: 10
    });

    q.t1().run();
  });


  it('should restart suspended', function (done) {
    let retries = 0;

    q.registerTask({
      name: 't1',
      process() {
        if (retries === 0) {
          retries++;
          return delay(1000000);
        }

        assert.equal(retries, 1);
        done();
      },
      timeout: 20
    });

    q.t1().run();
  });


  it('should set deadline', function () {
    let calls = 0;

    q.registerTask({
      name: 't1',
      process: bb.coroutine(function* () {
        calls++;

        if (calls === 1) {
          yield this.setDeadline(10);
          yield delay(1000);
        }
      })
    });

    q.t1().run();

    return delay(2000).then(() => {
      assert.equal(calls, 2);
    });
  });


  it('should set progress', bb.coroutine(function* () {
    q.registerTask({
      name: 't1',
      process: bb.coroutine(function* () {
        yield this.progressAdd(1);
        yield delay(500);
      }),
      init() {
        this.total = 2;
      }
    });

    let id = yield q.t1().run();

    yield delay(300);

    let task = yield q.getTask(id);

    assert.equal(task.progress, 1);

    yield delay(500);

    task = yield q.getTask(id);

    assert.equal(task.progress, 2);
  }));


  it('should cancel', bb.coroutine(function* () {
    q.registerTask('t1', () => {});

    let id = yield q.t1().run();

    yield q.cancel(id);

    let task = yield q.getTask(id);

    assert.equal(task.state, 'finished');
  }));


  it('should postpone', function (done) {
    q.registerTask('t1', () => {
      done();
    });

    q.t1().postpone(1000);
  });


  it('should remove old finished', bb.coroutine(function* () {
    q.registerTask({
      name: 't1',
      process: () => {},
      removeDelay: 1000
    });

    let id = yield q.t1().run();

    yield delay(500);

    let task = yield q.getTask(id);

    assert.notEqual(task, null);

    yield delay(1500);

    task = yield q.getTask(id);

    assert.equal(task, null);
  }));


  it('should set custom id', function (done) {
    q.registerTask({
      name: 't1',
      process() {
        assert.equal(this.id, 'test_id_x_y_z');
        done();
      },
      taskID: (a, b, c) => `test_id_${a}_${b}_${c}`
    });

    q.t1('x', 'y', 'z').run();
  });


  it('should throw error if `process` is not a function', function () {
    assert.throws(() => {
      q.registerTask({
        name: 't1'
      });
    }, /ido error: "process" should be a function/);
  });
});
