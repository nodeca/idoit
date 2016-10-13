// Command class
//

'use strict';


const random = require('./utils').random;


function Command(obj) {
  this.to     = obj.to;
  this.to_uid = obj.to_uid;
  this.type   = obj.type;
  this.data   = obj.data || null;
  this.rnd    = obj.rnd || random();
}


Command.prototype.toJSON = function () {
  if (this.data !== null) {
    return JSON.stringify({
      to:     this.to,
      to_uid: this.to_uid,
      type:   this.type,
      data:   this.data,
      rnd:    this.rnd
    });
  }

  return JSON.stringify({
    to:     this.to,
    to_uid: this.to_uid,
    type:   this.type,
    rnd:    this.rnd
  });
};


Command.fromObject = function (obj) { return new Command(obj); };


Command.fromJSON   = function (str) { return new Command(JSON.parse(str)); };


module.exports = Command;
