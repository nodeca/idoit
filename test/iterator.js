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


describe('iterator', function () {

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


  it('should run children tasks', function (done) {
    q.registerTask('t1', () => {});

    q.registerTask('t2', () => {
      setTimeout(done, 1000);
    });

    q.registerTask({
      name: 't3',
      baseClass: Queue.IteratorTemplate,
      iterate(i = 0) {
        if (i === 0) {
          return Promise.resolve({
            state: 1,
            tasks: [ q.t1() ]
          });
        } else if (i === 1) {
          return Promise.resolve({
            state: 2,
            tasks: [ q.t2() ]
          });
        }

        return Promise.resolve(null);
      }
    });

    q.t3().run();
  });


  it('should increment progress', function (done) {
    let id;

    q.registerTask('t1', () => {});

    q.registerTask('t2', bb.coroutine(function* () {
      let task = yield q.getTask(yield id);

      assert.equal(task.progress, 1);
      done();
    }));

    q.registerTask({
      name: 't3',
      baseClass: Queue.IteratorTemplate,
      iterate(i = 0) {
        if (i === 0) {
          return Promise.resolve({
            state: 1,
            tasks: [ q.t1() ]
          });
        } else if (i === 1) {
          return Promise.resolve({
            state: 2,
            tasks: [ q.t2() ]
          });
        }

        return Promise.resolve(null);
      },
      init() { this.total = 2; }
    });

    id = q.t3().run();
  });


  it('should work as part of chain', function (done) {
    q.registerTask('t1', () => {});

    q.registerTask('t2', () => { done(); });

    q.registerTask({
      name: 't3',
      baseClass: Queue.IteratorTemplate,
      iterate(i = 0) {
        if (i === 0) {
          return Promise.resolve({
            state: 1,
            tasks: [ q.t1(), q.t1(), q.t1() ]
          });
        }

        return Promise.resolve(null);
      }
    });

    q.chain([
      q.t1(),
      q.t1(),
      q.t3(),
      q.t2()
    ]).run();
  });


  it('should handle subtask error', bb.coroutine(function* () {
    q.removeAllListeners('error');
    // replace existing error throw with filtered one
    q.on('error', err => { if (!String(err).includes('<!test err!>')) throw err; });

    let t1Calls = 0;

    q.registerTask('t1', () => { t1Calls++; });
    q.registerTask({
      name: 't2',
      process() { throw new Error('<!test err!>'); },
      retryDelay: 10
    });

    q.registerTask({
      name: 't3',
      baseClass: Queue.IteratorTemplate,
      iterate(i = 0) {
        switch (i++) {
          case 0: return { state: i, tasks: [ q.t1() ] };
          case 1: return { state: i, tasks: [ q.t1() ] };
          case 2: return { state: i, tasks: [ q.t2() ] };
          case 3: return { state: i, tasks: [ q.t1() ] };
          case 4: return { state: i, tasks: [ q.t1() ] };
          default: return null;
        }
      }
    });

    let id = yield q.t3().run();
    let task = yield q.wait(id);

    assert.equal(t1Calls, 2);
    assert.ok(task.error.message.includes('<!test err!>'));
  }));


  it('should throw error if `iterate` is not a function', function () {
    assert.throws(() => {
      q.registerTask({
        name: 't1',
        baseClass: Queue.IteratorTemplate
      });
    }, /ido error: "iterate" should be a function/);
  });


  it('should fail on bad output', bb.coroutine(function* () {
    q.removeAllListeners('error');
    q.on('error', () => {});

    q.registerTask('t1', () => {});

    q.registerTask({
      name: 'iter',
      baseClass: Queue.IteratorTemplate,
      iterate(i = 0) {
        if (i === 5) {
          return Promise.resolve({
            state: i++,
            tasks: []
          });
        }

        return Promise.resolve({
          state: i++,
          tasks: [ q.t1() ]
        });
      }
    });

    let id = yield q.iter().run();
    let task = yield q.wait(id);

    assert.ok(task.error.message.includes('Iterator error: terminating task because bad "iterate" result'));
  }));


  it('should cancel', bb.coroutine(function* () {
    let t3Calls = 0;
    let id;

    q.registerTask('t1', () => {});
    q.registerTask('t2', () => { t3Calls++; });

    let childrenStep2 = [ q.t2(), q.t2(), q.t2(), q.t2(), q.t2() ];

    q.registerTask({
      name: 't3',
      baseClass: Queue.IteratorTemplate,
      iterate(i = 0) {
        switch (i++) {
          case 0: return { state: i, tasks: [ q.t1() ] };
          case 1:
            q.cancel(id);
            return { state: i, tasks: childrenStep2 };
          default: return null;
        }
      }
    });

    id = yield q.t3().run();

    yield q.wait(id);

    for (let t of childrenStep2) {
      yield q.wait(t.id);
    }

    assert.equal(t3Calls, 0);
  }));


  it('should handle error in a nested iterator', bb.coroutine(function* () {
    q.removeAllListeners('error');
    // replace existing error throw with filtered one
    q.on('error', err => { if (!/Iterator error/.test(String(err))) throw err; });

    q.registerTask({
      name: 't1',
      baseClass: Queue.IteratorTemplate,
      iterate() {
        return Promise.resolve(null);
      }
    });

    q.registerTask({
      name: 't2',
      baseClass: Queue.IteratorTemplate,
      iterate(i) {
        // bad iterate result: state not changed
        return Promise.resolve({ state: i });
      }
    });

    q.registerTask({
      name: 't3',
      baseClass: Queue.IteratorTemplate,
      iterate(i = 0) {
        return i === 0 ? { state: i + 1, tasks: [ q.t1(), q.t2() ] } : null;
      }
    });

    let id = yield q.t3().run();
    let task = yield q.wait(id);

    assert.ok(task.error.message.includes('Iterator error'));
  }));


  it('should support idling', bb.coroutine(function* () {
    let iteratorCalls = 0;

    q.registerTask('t1', () => {});

    q.registerTask({
      name: 't2',
      baseClass: Queue.IteratorTemplate,
      iterate(i = 0) {
        if (i === 0) {
          return Promise.resolve({ state: i + 1, tasks: [ q.t1(), q.t1(), q.t1() ] });
        }

        if (iteratorCalls++ < 2) {
          return Promise.resolve({});
        }

        return Promise.resolve(null);
      }
    });

    yield q.wait(yield q.t2().run());
  }));


  it('should allow iterator to end before child tasks', bb.coroutine(function* () {
    q.registerTask('t1', arg => delay(arg));

    q.registerTask({
      name: 't2',
      baseClass: Queue.IteratorTemplate,
      iterate(i = 0) {
        if (i === 0) {
          return Promise.resolve({ state: i + 1, tasks: [ q.t1(20), q.t1(40) ] });
        }

        return Promise.resolve(null);
      }
    });

    yield q.wait(yield q.t2().run());
  }));
});
