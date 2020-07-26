/* global describe, it, beforeEach, afterEach */
'use strict';


const assert   = require('assert');

const Queue    = require('../index');
const random   = require('../lib/utils').random;


const REDIS_URL = 'redis://localhost:6379/3';


function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }


async function clear_namespace(ns) {
  const r = require('redis').createClient(REDIS_URL);
  const keys = await r.keysAsync(`${ns}*`);

  if (keys.length) await r.delAsync(keys);
  await r.quitAsync();
}


describe('task', function () {

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


  it('should ignore task if same already added', async function () {
    let calls = 0;

    q.registerTask({
      name: 't1',
      process() {
        calls++;
      },
      taskID: () => 't1'
    });

    await q.t1().run();
    await q.t1().run();
    let id = await q.t1().run();

    await q.wait(id);

    assert.equal(calls, 1);
  });


  it('should start task if same already finished', async function () {
    let calls = 0;

    q.registerTask({
      name: 't1',
      process() {
        calls++;
      },
      taskID: () => 't1'
    });

    let id = await q.t1().run();

    let task;

    do {
      await delay(500);
      task = await q.getTask(id);
    } while (task.state !== 'finished');

    id = await q.t1().run();

    do {
      await delay(500);
      task = await q.getTask(id);
    } while (task.state !== 'finished');

    assert.equal(calls, 2);
  });


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


  it('should restart suspended', function (_done) {
    let done;
    let wait_for_task_finish = new Promise(resolve => {
      done = () => { resolve(); _done(); };
    });
    let retries = 0;

    q.registerTask({
      name: 't1',
      process() {
        if (retries === 0) {
          retries++;
          return wait_for_task_finish;
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
      async process() {
        calls++;

        if (calls === 1) {
          await this.setDeadline(10);
          await delay(1000);
        }
      }
    });

    q.t1().run();

    return delay(2000).then(() => {
      assert.equal(calls, 2);
    });
  });


  it('should set progress', async function () {
    q.registerTask({
      name: 't1',
      async process() {
        await this.progressAdd(1);
        await delay(500);
      },
      init() {
        this.total = 2;
      }
    });

    let id = await q.t1().run();

    await delay(300);

    let task = await q.getTask(id);

    assert.equal(task.progress, 1);

    await delay(500);

    task = await q.getTask(id);

    assert.equal(task.progress, 2);
  });


  it('should cancel', async function () {
    let done;
    let wait_for_task_finish = new Promise(resolve => {
      done = resolve;
    });
    q.registerTask('t1', () => wait_for_task_finish);

    let id = await q.t1().run();

    await q.cancel(id);

    let task = await q.wait(id);

    assert.equal(task.state, 'finished');
    done();
  });


  it('should postpone', function (done) {
    q.registerTask('t1', () => {
      done();
    });

    q.t1().postpone(1000);
  });


  it('should be able to restart itself', async function () {
    let t1_calls = 0;

    q.registerTask({
      name: 't1',
      retry: 3,
      retryDelay: 10,
      process() {
        t1_calls++;
        return this.restart(true);
      }
    });

    let id = await q.t1().run();

    await q.wait(id);

    // task restarts itself twice, executed 3 times total
    assert.equal(t1_calls, 3);
  });


  it('should remove old finished', async function () {
    q.registerTask({
      name: 't1',
      process: () => {},
      removeDelay: 1000
    });

    let id = await q.t1().run();

    await delay(500);

    let task = await q.getTask(id);

    assert.notEqual(task, null);

    await delay(1500);

    task = await q.getTask(id);

    assert.equal(task, null);
  });


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
    }, /idoit error: "process" should be a function/);
  });


  it('should emit task errors', async function () {
    let errors_catched = 0;

    q.removeAllListeners('error');
    // replace existing error throw with filtered one
    q.on('error', err => {
      if (String(err).includes('<!test err!>')) errors_catched++;
      else throw err;
    });

    q.registerTask({
      name: 't1',
      process() {
        return Promise.reject(new Error('<!test err!>'));
      },
      retry: 0
    });

    let id = await q.t1().run();
    let task = await q.wait(id);

    assert.equal(errors_catched, 1);
    assert.ok(task.error.message.includes('<!test err!>'));
  });


  it('should not emit errors from canceled tasks', async function () {
    let errors_catched = 0;

    q.removeAllListeners('error');
    // replace existing error throw with filtered one
    q.on('error', err => {
      if (String(err).includes('<!test err!>')) errors_catched++;
      else throw err;
    });

    q.registerTask({
      name: 't1',
      process() {
        return q.cancel(this.id).then(() => { throw new Error('<!test err!>'); });
      },
      retry: 0
    });

    let id = await q.t1().run();
    let task = await q.wait(id);

    assert.equal(errors_catched, 0);
    assert.equal(task.error.code, 'CANCELED');
  });


  it('should propagate errors', async function () {
    q.removeAllListeners('error');
    // replace existing error throw with filtered one
    q.on('error', err => { if (!String(err).includes('<!test err!>')) throw err; });

    q.registerTask({
      name: 't1',
      process() {
        return Promise.reject(new Error('<!test err!>'));
      },
      retry: 0
    });

    let id = await q.chain([ q.chain([ q.t1() ]) ]).run();
    let task = await q.wait(id);

    assert.ok(task.error.message.includes('<!test err!>'));
  });


  it('should pass progress to parent task', async function () {
    let p1 = new Promise(resolve => {
      q.registerTask({
        name: 't1',
        async process() {
          await this.progressAdd(1);
          resolve();
        },
        init() {
          this.total = 1;
        }
      });
    });

    let p2 = new Promise(resolve => {
      q.registerTask({
        name: 't2',
        async process() {
          await this.progressAdd(1);
          resolve();
        },
        init() {
          this.total = 1;
        }
      });
    });

    let id = await q.group([ q.t1(), q.t2() ]).run();

    await p1;
    await p2;
    await delay(20);

    let task = await q.getTask(id);

    assert.equal(task.progress, 2);

    await q.wait(id);
  });


  it('should handle invalid commands', async function () {
    let errored = false;

    q.removeAllListeners('error');
    // replace existing error throw with filtered one
    q.on('error', err => {
      if (!String(err).includes('ignored unknown command')) throw err;
      else errored = true;
    });

    // test TaskTemplate.handleCommand, only using group as an example
    class IncompleteGroup extends Queue.GroupTemplate {
      static extend(options) {
        class T extends IncompleteGroup {}

        Object.assign(T.prototype, options);

        return T;
      }
    }

    // unset this command for testing purposes
    IncompleteGroup.prototype.handleCommand_progress = null;

    q.registerTask('t1', async function () {
      await this.progressAdd(1);
    });

    q.registerTask({
      name: 't2',
      baseClass: IncompleteGroup
    });

    let id = await q.t2([ q.t1() ]).run();

    await q.wait(id);

    assert.ok(errored);
  });


  it('should keep track of tasks if they time out', async function () {
    let p, p2;

    p = new Promise(resolve => {
      p2 = new Promise(resolve2 => {
        q.registerTask({
          name: 't',
          timeout: 20,
          retry: 0,
          process() {
            resolve();

            return delay(60).then(resolve2);
          }
        });
      });
    });

    assert.equal(q.__tasksTracker__, 0);

    await q.t().run();

    // wait for task to start
    await p;
    assert.equal(q.__tasksTracker__, 1);

    // task is timed out, not yet finished
    await delay(40);
    assert.equal(q.__tasksTracker__, 0);

    // task resulting promise resolved, make sure
    // task tracker wasn't decremented twice
    await p2;
    await delay(1);
  });
});
