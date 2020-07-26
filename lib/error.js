'use strict';


// Error returned from task
//
// - wrappedError (String|Object) - original error
// - type (String)                - the task name
// - state (String)               - current task state
// - taskID (String)              - the task id
// - payload (Object)             - data passed as payload
//
class QueueError extends Error {
  constructor(wrappedError, type, state, taskID, payload) {
    super();
    Error.captureStackTrace(this, this.constructor);
    this.name = this.constructor.name;
    this.message = `${wrappedError.toString()} (task: ${type}, state: ${state}, task ID: ${taskID})`;
    this.wrappedError = wrappedError;
    this.type = type;
    this.state = state;
    this.taskID = taskID;
    this.payload = payload;
  }
}

module.exports = QueueError;
