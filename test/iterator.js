/* global describe, it, beforeEach, afterEach */
'use strict';


const assert = require('assert');
const Redis  = require('ioredis');


const Queue  = require('../index');
const random = require('../lib/utils').random;


const REDIS_URL = 'redis://localhost:6379/3';


function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }


async function clear_namespace(ns) {
  const r = new Redis(REDIS_URL);
  const keys = await r.keys(`${ns}*`);

  if (keys.length) await r.del(keys);
  await r.quit();
}


describe('iterator', function () {

  let q, q_ns;

  beforeEach(async function () {
    q_ns = `idoit_test_${random(6)}:`;

    q = new Queue({ redisURL: REDIS_URL, ns: q_ns });

    // Helper to wait task finish
    q.wait = async function (id) {
      let task = await this.getTask(id);

      while (task.state !== 'finished') {
        await delay(50);
        task = await this.getTask(id);
      }

      return task;
    };

    q.on('error', err => { throw err; });

    await q.start();
  });

  afterEach(async function () {
    await q.shutdown();
    clearTimeout(q.__timer__);
    await delay(100);
    await q.__redis__.quit();
    await clear_namespace(q_ns);
  });


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

    q.registerTask('t2', async function () {
      let task = await q.getTask(await id);

      assert.equal(task.progress, 1);
      done();
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


  it('should handle subtask error', async function () {
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

    let id = await q.t3().run();
    let task = await q.wait(id);

    assert.equal(t1Calls, 2);
    assert.ok(task.error.message.includes('<!test err!>'));
  });


  it('should throw error if `iterate` is not a function', function () {
    assert.throws(() => {
      q.registerTask({
        name: 't1',
        baseClass: Queue.IteratorTemplate
      });
    }, /idoit error: "iterate" should be a function/);
  });


  it('should fail on bad output', async function () {
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

    let id = await q.iter().run();
    let task = await q.wait(id);

    assert.ok(task.error.message.includes('Iterator error: terminating task because bad "iterate" result'));
  });


  it('should cancel', async function () {
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

    id = await q.t3().run();

    await q.wait(id);

    for (let t of childrenStep2) {
      await q.wait(t.id);
    }

    assert.equal(t3Calls, 0);
  });


  it('should handle error in a nested iterator', async function () {
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

    let id = await q.t3().run();
    let task = await q.wait(id);

    assert.ok(task.error.message.includes('Iterator error'));
  });


  it('should support idling', async function () {
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

    await q.wait(await q.t2().run());
  });


  it('should allow iterator to end before child tasks', async function () {
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

    await q.wait(await q.t2().run());
  });
});
