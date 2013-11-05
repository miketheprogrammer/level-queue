/*
* The general flow here is borrowed from Julian Gruber's Sum module.
* Since the goals of this module are somewhat similar.
*/
var MapReduce = require('map-reduce');
var events = require('events');
var EventEmitter = events.EventEmitter || events.EventEmmiter2;
var inherits = require('util').inherits;
var through = require('through');
var microtime = require('microtime');
module.exports = Queue;

function Queue(db) {
    if (!(this instanceof Queue)) return new Queue(db);

    EventEmitter.call(this);

    this.db = db;
}

inherits(Queue, EventEmitter);

Queue.prototype.follow = function (key) {
    if (!Array.isArray(key)) key = [key];

    var tr = through(null, null);

    tr.writable = false;

    var stream = this.db.createReadStream()
        .on('data', function(data) {
            data.key = data.key.split('!');
            for (var i = 0; i < key.length; i++) {
                if (key[i] != data.key[i]) return;
            }
            
            tr.queue(data);
        })
        .on('end', function () {
            tr.end();
        });

    return tr;
};

Queue.prototype.toArray = function(key, cb) {
    var array = [];

    this.follow(key)
        .on('data', function(d) {
            array.push(d);
        })
        .on('error', function(e) {
            cb(e);
        })
        .on('end', function() {
            cb(null, array);
        });
};

/*
Cannot maintain unicode order using timestamps.
Need a better way to manage the queue order.
Maybe some internal atomic inc op
*/
Queue.prototype.enqueue = function (key, value, cb) {
    if (Array.isArray(key)) key = key.join('!');
    var suffix  = microtime.now();
    var complexKey = [key, suffix].join('!');
    this.db.put(complexKey, value, cb);
};

Queue.prototype.dequeue = function (key, cb) {
    var ref = this;
    this.toArray(key, function (err, arr) {
        var key = arr[arr.length - 1].key.join('!');
        ref.db.del(key, function(err) {
            if (err) cb(err, false);
            else cb(null, true);
        });
    });
};
