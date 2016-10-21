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
