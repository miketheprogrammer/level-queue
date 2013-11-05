var assert = require('assert');
var LevelQueue = require('./index.js');
var level = require('memdb');
var SubLevel = require('level-sublevel');


describe('level-queue', function () {
    describe('enqueue', function () {
        it('Should add an item to the queue', function (done) {
            var db = SubLevel(level());
            var queue = LevelQueue(db);
            var key = ['worker', 'task'];
            var follow = queue.follow(key);
            follow.on('data', function data(msg) {
                if (msg.value.indexOf('hello world') !== -1) 
                    done()
                else done(new Error)
            });
            queue.enqueue(key, 'hello world');

        });
        it('Should retain order', function(done) {
            var msg1 = 'Hello';
            var msg2 = 'World';

            var db = SubLevel(level());
            var queue = LevelQueue(db);
            var key = ['worker', 'tasks'];

            queue.enqueue(key, msg1);
            queue.enqueue(key, msg2);

            queue.toArray(key, function(err, res) {
                assert(res[0].key[2] < res[1].key[2]);
                done();
            });
        });
    });
    
    describe('toArray', function () {
        it('Should return an array', function(done) {
            var msg1 = 'Hello';
            var msg2 = 'World';

            var db = SubLevel(level());
            var queue = LevelQueue(db);
            var key = ['worker', 'tasks'];

            queue.enqueue(key, msg1);
            queue.enqueue(key, msg2);

            queue.toArray(key, function(err, res) {
                assert(Array.isArray(res));
                done();
            });
        });
    });

    describe('follow', function () {
        it('Should return a stream', function (done) {
            var db = SubLevel(level());
            var queue = LevelQueue(db);
            var key = ['worker', 'tasks'];
            var follow = queue.follow(key);
            follow.on('data', function data(msg) {
                done();
            });

            queue.enqueue(key, 'hello world');
        });

        describe('- stream', function() {
            it('Should not be writable', function (done) {
                var db = SubLevel(level());
                var queue = LevelQueue(db);
                var key = ['worker', 'tasks'];
                var follow = queue.follow(key);
                assert(follow.writable === false);
                done();
            });
        });

        it('Should use keys properly', function (done) {
            var db = SubLevel(level());
            var queue = LevelQueue(db);
            var key = ['worker', 'tasks'];
            var key2 = ['worker-1', 'tasks'];
            var follow = queue.follow(key2);

            queue.enqueue(key, 'hello world');
            
            follow.on('data', function() {
                var err = new Error('There should be no data event');
                done(err);
                done = function () {};
            });
            follow.on('end', function () {
                done();
            });
        });
    });

    describe('dequeue', function () {
        it('Should remove 1 item from the queue retaining order', function (done) {
            var db = SubLevel(level());
            var queue = LevelQueue(db);
            var key = ['worker', 'tasks'];

            queue.enqueue(key, 'hello');
            queue.enqueue(key, 'world');
            queue.dequeue(key, function (err, res) {
                if (err) done(err);

                if (res) {
                    queue.toArray(key, function (err, arr) {
                        assert(arr.length === 1);
                        assert(arr[0].value === 'hello');
                        done();
                    });
                }
            });
        });
    });
});
