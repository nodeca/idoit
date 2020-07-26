// Command class
//

'use strict';


const random = require('./utils').random;


class Command {
  constructor(obj) {
    this.to     = obj.to;
    this.to_uid = obj.to_uid;
    this.type   = obj.type;
    this.data   = obj.data || null;
    this.rnd    = obj.rnd || random();
  }

  toJSON() {
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
  }

  static fromObject(obj) { return new Command(obj); }

  static fromJSON(str) { return new Command(JSON.parse(str)); }
}


module.exports = Command;
