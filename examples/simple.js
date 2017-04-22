'use strict';


const Queue   = require('../');


(async function () {
  const q = new Queue({
    redisURL: 'redis://localhost'
  });

  q.on('error', err => {
    // Split task errors and internal errors
    if (err instanceof Queue.Error) {
      console.log(`Error in task "process" function: ${err}`);
    } else {
      console.log(`idoit internal error: ${err}`);
    }
  });

  q.registerTask('mytask', val => console.log(val));

  // Wait until ready & start task processing.
  await q.start();

  // Put tasks to queue
  await q.mytask(1).run();
  await q.mytask(2).run();
  await q.mytask(3).run();
  await q.mytask(4).run();
  await q.mytask(5).run();
})().catch(err => console.log(err));
