'use strict';


const crypto  = require('crypto');


// Redis time to milliseconds
//
// - time (Array) - time from redis
//
exports.redisToMs = function redisToMs(time) {
  // Redis reply containing two elements: unix time in seconds, microseconds
  return time[0] * 1000 + Math.round(time[1] / 1000);
};

// Random bytes, base64-encoded
//
exports.random = function random(bytes = 21) {
  return crypto.randomBytes(bytes).toString('base64');
};


// Pool name to pool id. Based on first 52 bit of sha256.
//
exports.poolToId = function (name) {
  let sha256 = crypto.createHash('sha256').update(name).digest('hex');

  /* eslint-disable no-bitwise */
  return parseInt(sha256.substr(0, 13), 16);
};
