'use strict';


const Promise = require('bluebird');
const Queue   = require('../');


Promise.coroutine(function* () {
  const q = new Queue({
    redisURL: 'redis://localhost'
  });

  q.on('error', err => {
    // Split task errors and internal errors
    if (err instanceof Queue.Error) {
      console.log(`Error in task "process" function: ${err}`);
    } else {
      console.log(`Ido internal error: ${err}`);
    }
  });

  q.registerTask('mytask', val => console.log(val));

  // Wait until ready & start task processing.
  yield q.start();

  // Put tasks to queue
  yield q.mytask(1).run();
  yield q.mytask(2).run();
  yield q.mytask(3).run();
  yield q.mytask(4).run();
  yield q.mytask(5).run();
})().catch(err => console.log(err));
