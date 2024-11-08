import fs from 'fs';
import path from 'path';
import async from 'async';
import {Datastore} from '../lib/datastore.js';
import {assert, expect, pp} from './utils.js';
import {ensureDirectoryExists} from '../lib/storage.js';
import {deserialize, serialize} from '../lib/model.js';

const reloadTimeUpperBound = 60;   // In ms, an upper bound for the reload time used to check createdAt and updatedAt

const testDb = pp('test.db');

describe('Database', function () {
  var d;

  beforeEach(function (done) {
    d = new Datastore({ filename: testDb });
     expect(d.filename).to.equal(testDb);
     expect(d.inMemoryOnly).to.equal(false);

    async.waterfall([
      function (cb) {
        ensureDirectoryExists(path.dirname(testDb), function () {
          fs.exists(testDb, function (exists) {
            if (exists) {
              fs.unlink(testDb, cb);
            } else { return cb(); }
          });
        });
      }
    , function (cb) {
        d.loadDatabase(function (err) {
          assert.isNull(err);
           expect(d.getAllData().length).to.equal(0);
          return cb();
        });
      }
    ], done);
  });

  it('No more constructor compatibility with v0.6-', function () {
    expect(() => void new Datastore('somefile')).to.throw();
    expect(() => void new Datastore('')).to.throw();
  });

  describe('Autoloading', function () {

    it('Can autoload a database and query it right away', function (done) {
      var fileStr = serialize({ _id: '1', a: 5, planet: 'Earth' }) + '\n' + serialize({ _id: '2', a: 5, planet: 'Mars' }) + '\n'
        , autoDb = pp('auto.db')
        , db
        ;

      fs.writeFileSync(autoDb, fileStr, 'utf8');
      db = new Datastore({ filename: autoDb, autoload: true })

      db.find({}, function (err, docs) {
        assert.isNull(err);
         expect(docs.length).to.equal(2);
        done();
      });
    });

    it('Throws if autoload fails', function (done) {
      var fileStr = serialize({ _id: '1', a: 5, planet: 'Earth' }) + '\n' + serialize({ _id: '2', a: 5, planet: 'Mars' }) + '\n' + '{"$$indexCreated":{"fieldName":"a","unique":true}}'
        , autoDb = pp('auto.db')
        , db
        ;

      fs.writeFileSync(autoDb, fileStr, 'utf8');

      // Check the loadDatabase generated an error
      function onload (err) {
         expect(err.errorType).to.equal('uniqueViolated');
        done();
      }

      db = new Datastore({ filename: autoDb, autoload: true, onload: onload })

      db.find({}, function (err, docs) {
        done(new Error("Find should not be executed since autoload failed"));
      });
    });

  });

  describe('Insert', function () {

    it('Able to insert a document in the database, setting an _id if none provided, and retrieve it even after a reload', function (done) {
      d.find({}, function (err, docs) {
         expect(docs.length).to.equal(0);

        d.insert({ somedata: 'ok' }, function (err) {
          // The data was correctly updated
          d.find({}, function (err, docs) {
            assert.isNull(err);
             expect(docs.length).to.equal(1);
             expect(Object.keys(docs[0]).length).to.equal(2);
             expect(docs[0].somedata).to.equal('ok');
            assert.isDefined(docs[0]._id);

            // After a reload the data has been correctly persisted
            d.loadDatabase(function (err) {
              d.find({}, function (err, docs) {
                assert.isNull(err);
                 expect(docs.length).to.equal(1);
                 expect(Object.keys(docs[0]).length).to.equal(2);
                 expect(docs[0].somedata).to.equal('ok');
                assert.isDefined(docs[0]._id);

                done();
              });
            });
          });
        });
      });
    });

    it('Can insert multiple documents in the database', function (done) {
      d.find({}, function (err, docs) {
         expect(docs.length).to.equal(0);

        d.insert({ somedata: 'ok' }, function (err) {
          d.insert({ somedata: 'another' }, function (err) {
            d.insert({ somedata: 'again' }, function (err) {
              d.find({}, function (err, docs) {
                 expect(docs.length).to.equal(3);
                 expect(docs.map(doc => doc.somedata)).to.contain('ok');
                 expect(docs.map(doc => doc.somedata)).to.contain('another');
                 expect(docs.map(doc => doc.somedata)).to.contain('again');
                done();
              });
            });
          });
        });
      });
    });

    it('Can insert and get back from DB complex objects with all primitive and secondary types', function (done) {
      var da = new Date()
        , obj = { a: ['ee', 'ff', 42], date: da, subobj: { a: 'b', b: 'c' } }
        ;

      d.insert(obj, function (err) {
        d.findOne({}, function (err, res) {
          assert.isNull(err);
           expect(res.a.length).to.equal(3);
           expect(res.a[0]).to.equal('ee');
           expect(res.a[1]).to.equal('ff');
           expect(res.a[2]).to.equal(42);
           expect(res.date.getTime()).to.equal(da.getTime());
           expect(res.subobj.a).to.equal('b');
           expect(res.subobj.b).to.equal('c');

          done();
        });
      });
    });

    it('If an object returned from the DB is modified and refetched, the original value should be found', function (done) {
      d.insert({ a: 'something' }, function () {
        d.findOne({}, function (err, doc) {
           expect(doc.a).to.equal('something');
          doc.a = 'another thing';
           expect(doc.a).to.equal('another thing');

          // Re-fetching with findOne should yield the persisted value
          d.findOne({}, function (err, doc) {
             expect(doc.a).to.equal('something');
            doc.a = 'another thing';
             expect(doc.a).to.equal('another thing');

            // Re-fetching with find should yield the persisted value
            d.find({}, function (err, docs) {
               expect(docs[0].a).to.equal('something');

              done();
            });
          });
        });
      });
    });

    it('Cannot insert a doc that has a field beginning with a $ sign', function (done) {
      d.insert({ $something: 'atest' }, function (err) {
        assert.isDefined(err);
        done();
      });
    });

    it('If an _id is already given when we insert a document, use that instead of generating a random one', function (done) {
      d.insert({ _id: 'test', stuff: true }, function (err, newDoc) {
        if (err) { return done(err); }
 
        expect(newDoc.stuff).to.equal(true);
         expect(newDoc._id).to.equal('test');

        d.insert({ _id: 'test', otherstuff: 42 }, function (err) {
           expect(err.errorType).to.equal('uniqueViolated');

          done();
        });
      });
    });

    it('Modifying the insertedDoc after an insert doesnt change the copy saved in the database', function (done) {
      d.insert({ a: 2, hello: 'world' }, function (err, newDoc) {
        newDoc.hello = 'changed';

        d.findOne({ a: 2 }, function (err, doc) {
           expect(doc.hello).to.equal('world');
          done();
        });
      });
    });

    it('Can insert an array of documents at once', function (done) {
      var docs = [{ a: 5, b: 'hello' }, { a: 42, b: 'world' }];

      d.insert(docs, function (err) {
        d.find({}, function (err, docs) {
          var data;
 
          expect(docs.length).to.equal(2);
           expect(docs.find(doc => doc.a === 5).b).to.equal('hello');
           expect(docs.find(doc => doc.a === 42).b).to.equal('world');

           // The data has been persisted correctly
           data = fs.readFileSync(testDb, 'utf8').split('\n').filter(line => line.length > 0);
           expect(data.length).to.equal(2);
           expect(deserialize(data[0]).a).to.equal(5);
           expect(deserialize(data[0]).b).to.equal('hello');
           expect(deserialize(data[1]).a).to.equal(42);
           expect(deserialize(data[1]).b).to.equal('world');

          done();
        });
      });
    });

    it('If a bulk insert violates a constraint, all changes are rolled back', function (done) {
      var docs = [{ a: 5, b: 'hello' }, { a: 42, b: 'world' }, { a: 5, b: 'bloup' }, { a: 7 }];

      d.ensureIndex({ fieldName: 'a', unique: true }, function () {   // Important to specify callback here to make sure filesystem synced
        d.insert(docs, function (err) {
           expect(err.errorType).to.equal('uniqueViolated');

          d.find({}, function (err, docs) {
            // Datafile only contains index definition
            var datafileContents = deserialize(fs.readFileSync(testDb, 'utf8'));
            assert.deepEqual(datafileContents, { $$indexCreated: { fieldName: 'a', unique: true } });
 
            expect(docs.length).to.equal(0);

            done();
          });
        });
      });
    });

    it("If timestampData option is set, a createdAt field is added and persisted", function (done) {
      var newDoc = { hello: 'world' }, beginning = Date.now();
      d = new Datastore({ filename: testDb, timestampData: true, autoload: true });
      d.find({}, function (err, docs) {
        assert.isNull(err);
         expect(docs.length).to.equal(0);

        d.insert(newDoc, function (err, insertedDoc) {
          // No side effect on given input
          assert.deepEqual(newDoc, { hello: 'world' });
          // Insert doc has two new fields, _id and createdAt
           expect(insertedDoc.hello).to.equal('world');
          assert.isDefined(insertedDoc.createdAt);
          assert.isDefined(insertedDoc.updatedAt);
           expect(insertedDoc.createdAt).to.equal(insertedDoc.updatedAt);
          assert.isDefined(insertedDoc._id);
           expect(Object.keys(insertedDoc).length).to.equal(4);
          assert.isBelow(Math.abs(insertedDoc.createdAt.getTime() - beginning), reloadTimeUpperBound);   // No more than 30ms should have elapsed (worst case, if there is a flush)

          // Modifying results of insert doesn't change the cache
          insertedDoc.bloup = "another";
           expect(Object.keys(insertedDoc).length).to.equal(5);

          d.find({}, function (err, docs) {
             expect(docs.length).to.equal(1);
            assert.deepEqual(newDoc, { hello: 'world' });
            assert.deepEqual({ hello: 'world', _id: insertedDoc._id, createdAt: insertedDoc.createdAt, updatedAt: insertedDoc.updatedAt }, docs[0]);

            // All data correctly persisted on disk
            d.loadDatabase(function () {
              d.find({}, function (err, docs) {
                 expect(docs.length).to.equal(1);
                assert.deepEqual(newDoc, { hello: 'world' });
                assert.deepEqual({ hello: 'world', _id: insertedDoc._id, createdAt: insertedDoc.createdAt, updatedAt: insertedDoc.updatedAt }, docs[0]);

                done();
              });
            });
          });
        });
      });
    });

    it("If timestampData option not set, don't create a createdAt and a updatedAt field", function (done) {
      d.insert({ hello: 'world' }, function (err, insertedDoc) {
         expect(Object.keys(insertedDoc).length).to.equal(2);
        assert.isUndefined(insertedDoc.createdAt);
        assert.isUndefined(insertedDoc.updatedAt);

        d.find({}, function (err, docs) {
           expect(docs.length).to.equal(1);
          assert.deepEqual(docs[0], insertedDoc);

          done();
        });
      });
    });

    it("If timestampData is set but createdAt is specified by user, don't change it", function (done) {
      var newDoc = { hello: 'world', createdAt: new Date(234) }, beginning = Date.now();
      d = new Datastore({ filename: testDb, timestampData: true, autoload: true });
      d.insert(newDoc, function (err, insertedDoc) {
         expect(Object.keys(insertedDoc).length).to.equal(4);
         expect(insertedDoc.createdAt.getTime()).to.equal(234);   // Not modified
        assert.isBelow(insertedDoc.updatedAt.getTime() - beginning, reloadTimeUpperBound);   // Created

        d.find({}, function (err, docs) {
          assert.deepEqual(insertedDoc, docs[0]);

          d.loadDatabase(function () {
            d.find({}, function (err, docs) {
              assert.deepEqual(insertedDoc, docs[0]);

              done();
            });
          });
        });
      });
    });

    it("If timestampData is set but updatedAt is specified by user, don't change it", function (done) {
      var newDoc = { hello: 'world', updatedAt: new Date(234) }, beginning = Date.now();
      d = new Datastore({ filename: testDb, timestampData: true, autoload: true });
      d.insert(newDoc, function (err, insertedDoc) {
         expect(Object.keys(insertedDoc).length).to.equal(4);
         expect(insertedDoc.updatedAt.getTime()).to.equal(234);   // Not modified
        assert.isBelow(insertedDoc.createdAt.getTime() - beginning, reloadTimeUpperBound);   // Created

        d.find({}, function (err, docs) {
          assert.deepEqual(insertedDoc, docs[0]);

          d.loadDatabase(function () {
            d.find({}, function (err, docs) {
              assert.deepEqual(insertedDoc, docs[0]);

              done();
            });
          });
        });
      });
    });

    it('Can NOT insert a doc with id 0, but with "0"', function (done) {
      
      d.insert({ _id: 0, hello: 'world' }, function (err, doc) {
         expect(err).to.be.instanceof(Error);
         expect(err.message).to.equal('Invalid document or _id value');

          d.insert({ _id: "0", hello: 'world' }, function (err, doc) {
              expect(doc._id).to.equal('0');
              expect(doc.hello).to.equal('world');
              done();
          });
      });
    });

    /**
     * Complicated behavior here. Basically we need to test that when a user function throws an exception, it is not caught
     * in NeDB and the callback called again, transforming a user error into a NeDB error.
     *
     * So we need a way to check that the callback is called only once and the exception thrown is indeed the client exception
     * Mocha's exception handling mechanism interferes with this since it already registers a listener on uncaughtException
     * which we need to use since findOne is not called in the same turn of the event loop (so no try/catch)
     * So we remove all current listeners, put our own which when called will register the former listeners (incl. Mocha's) again.
     *
     * Note: maybe using an in-memory only NeDB would give us an easier solution
     */
    it('If the callback throws an uncaught exception, do not catch it inside findOne, this is userspace concern', function (done) {
      var tryCount = 0
        , currentUncaughtExceptionHandlers = process.listeners('uncaughtException')
        , i
        ;

      process.removeAllListeners('uncaughtException');

      process.on('uncaughtException', function MINE (ex) {
        process.removeAllListeners('uncaughtException');

        for (i = 0; i < currentUncaughtExceptionHandlers.length; i += 1) {
          process.on('uncaughtException', currentUncaughtExceptionHandlers[i]);
        }
 
        expect(ex.message).to.equal('SOME EXCEPTION');
        done();
      });

      d.insert({ a: 5 }, function () {
        d.findOne({ a : 5}, function (err, doc) {
          if (tryCount === 0) {
            tryCount += 1;
            throw new Error('SOME EXCEPTION');
          } else {
            done(new Error('Callback was called twice'));
          }
        });
      });
    });

  });   // ==== End of 'Insert' ==== //


  describe('#getCandidates', function () {

    it('Can use an index to get docs with a basic match', function (done) {
      d.ensureIndex({ fieldName: 'tf' }, function (err) {
        d.insert({ tf: 4 }, function (err, _doc1) {
          d.insert({ tf: 6 }, function () {
            d.insert({ tf: 4, an: 'other' }, function (err, _doc2) {
              d.insert({ tf: 9 }, function () {
                d.getCandidates({ r: 6, tf: 4 }, function (err, data) {
                  var doc1 = data.find(d => d._id === _doc1._id)
                    , doc2 = data.find(d => d._id === _doc2._id)
                    ;
 
                  expect(data.length).to.equal(2);
                  assert.deepEqual(doc1, { _id: doc1._id, tf: 4 });
                  assert.deepEqual(doc2, { _id: doc2._id, tf: 4, an: 'other' });

                  done();
                });
              });
            });
          });
        });
      });
    });

    it('Can use an index to get docs with a $in match', function (done) {
      d.ensureIndex({ fieldName: 'tf' }, function (err) {
        d.insert({ tf: 4 }, function (err) {
          d.insert({ tf: 6 }, function (err, _doc1) {
            d.insert({ tf: 4, an: 'other' }, function (err) {
              d.insert({ tf: 9 }, function (err, _doc2) {
                d.getCandidates({ r: 6, tf: { $in: [6, 9, 5] } }, function (err, data) {
                  var doc1 = data.find(d => d._id === _doc1._id)
                    , doc2 = data.find(d => d._id === _doc2._id)
                    ;
 
                  expect(data.length).to.equal(2);
                  assert.deepEqual(doc1, { _id: doc1._id, tf: 6 });
                  assert.deepEqual(doc2, { _id: doc2._id, tf: 9 });

                  done();
                });
              });
            });
          });
        });
      });
    });

    it('Can use an index to get docs with a {_id: $in} match', function (done) {
      d.insert([{num:1}, {num:2}, {num:3}], function (err) {
          d.find({}, (err, docs) => {
              expect(docs.length).to.equal(3);
              const [doc1, doc2, doc3] = docs.sort((a,b) => a.num - b.num);
              
              assert.equal(doc1.num, 1);
              assert.equal(doc2.num, 2);
              assert.equal(doc3.num, 3);
              
              d.find({_id: {$in: [doc1._id, doc3._id, doc2._id]}}, (err, newDocs) => {
                  expect(newDocs.length).to.equal(3);
                  const [doc1, doc2, doc3] = newDocs.sort((a,b) => a.num - b.num);

                  assert.equal(doc1.num, 1);
                  assert.equal(doc2.num, 2);
                  assert.equal(doc3.num, 3);
                  done();
              });
          });
      });
    });
    
    it('If no index can be used, return the whole database', function (done) {
      d.ensureIndex({ fieldName: 'tf' }, function (err) {
        d.insert({ tf: 4 }, function (err, _doc1) {
          d.insert({ tf: 6 }, function (err, _doc2) {
            d.insert({ tf: 4, an: 'other' }, function (err, _doc3) {
              d.insert({ tf: 9 }, function (err, _doc4) {
                d.getCandidates({ r: 6, notf: { $in: [6, 9, 5] } }, function (err, data) {
                  var doc1 = data.find(d => d._id === _doc1._id )
                    , doc2 = data.find(d => d._id === _doc2._id )
                    , doc3 = data.find(d => d._id === _doc3._id )
                    , doc4 = data.find(d => d._id === _doc4._id )
                    ;
 
                  expect(data.length).to.equal(4);
                  assert.deepEqual(doc1, { _id: doc1._id, tf: 4 });
                  assert.deepEqual(doc2, { _id: doc2._id, tf: 6 });
                  assert.deepEqual(doc3, { _id: doc3._id, tf: 4, an: 'other' });
                  assert.deepEqual(doc4, { _id: doc4._id, tf: 9 });

                  done();
                });
              });
            });
          });
        });
      });
    });

    it('Can use indexes for comparison matches', function (done) {
      d.ensureIndex({ fieldName: 'tf' }, function (err) {
        d.insert({ tf: 4 }, function (err, _doc1) {
          d.insert({ tf: 6 }, function (err, _doc2) {
            d.insert({ tf: 4, an: 'other' }, function (err, _doc3) {
              d.insert({ tf: 9 }, function (err, _doc4) {
                d.getCandidates({ r: 6, tf: { $lte: 9, $gte: 6 } }, function (err, data) {
                  var doc2 = data.find(d => d._id === _doc2._id)
                    , doc4 = data.find(d => d._id === _doc4._id)
                    ;
 
                  expect(data.length).to.equal(2);
                  assert.deepEqual(doc2, { _id: doc2._id, tf: 6 });
                  assert.deepEqual(doc4, { _id: doc4._id, tf: 9 });

                  done();
                });
              });
            });
          });
        });
      });
    });

    it("Can set a TTL index that expires documents", function (done) {
      d.ensureIndex({ fieldName: 'exp', expireAfterSeconds: 0.2 }, function () {
        d.insert({ hello: 'world', exp: new Date() }, function () {
          setTimeout(function () {
            d.findOne({}, function (err, doc) {
              assert.isNull(err);
               expect(doc.hello).to.equal('world');

              setTimeout(function () {
                d.findOne({}, function (err, doc) {
                  assert.isNull(err);
                  assert.isNull(doc);

                  d.on('compaction.done', function () {
                    // After compaction, no more mention of the document, correctly removed
                    var datafileContents = fs.readFileSync(testDb, 'utf8');
                     expect(datafileContents.split('\n').length).to.equal(2);
                    assert.isNull(datafileContents.match(/world/));

                    // New datastore on same datafile is empty
                    var d2 = new Datastore({ filename: testDb, autoload: true });
                    d2.findOne({}, function (err, doc) {
                      assert.isNull(err);
                      assert.isNull(doc);

                      done();
                    });
                  });

                  d.persistence.compactDatafile();
                });
              }, 101);
            });
          }, 100);
        });
      });
    });

    it("TTL indexes can expire multiple documents and only what needs to be expired", function (done) {
      d.ensureIndex({ fieldName: 'exp', expireAfterSeconds: 0.2 }, function () {
        d.insert({ hello: 'world1', exp: new Date() }, function () {
          d.insert({ hello: 'world2', exp: new Date() }, function () {
            d.insert({ hello: 'world3', exp: new Date((new Date()).getTime() + 100) }, function () {
              setTimeout(function () {
                d.find({}, function (err, docs) {
                  assert.isNull(err);
                   expect(docs.length).to.equal(3);

                  setTimeout(function () {
                    d.find({}, function (err, docs) {
                      assert.isNull(err);
                       expect(docs.length).to.equal(1);
                       expect(docs[0].hello).to.equal('world3');

                      setTimeout(function () {
                        d.find({}, function (err, docs) {
                          assert.isNull(err);
                           expect(docs.length).to.equal(0);

                          done();
                        });
                      }, 101);
                    });
                  }, 101);
                });
              }, 100);
            });
          });
        });
      });
    });

    it("Document where indexed field is absent or not a date are ignored", function (done) {
      d.ensureIndex({ fieldName: 'exp', expireAfterSeconds: 0.2 }, function () {
        d.insert({ hello: 'world1', exp: new Date() }, function () {
          d.insert({ hello: 'world2', exp: "not a date" }, function () {
            d.insert({ hello: 'world3' }, function () {
              setTimeout(function () {
                d.find({}, function (err, docs) {
                  assert.isNull(err);
                   expect(docs.length).to.equal(3);

                  setTimeout(function () {
                    d.find({}, function (err, docs) {
                      assert.isNull(err);
                       expect(docs.length).to.equal(2);


                      docs[0].hello.should.not.equal('world1');
                      docs[1].hello.should.not.equal('world1');

                      done();
                    });
                  }, 101);
                });
              }, 100);
            });
          });
        });
      });
    });

  });   // ==== End of '#getCandidates' ==== //


  describe('Find', function () {

    it('Can find all documents if an empty query is used', function (done) {
      async.waterfall([
      function (cb) {
        d.insert({ somedata: 'ok' }, function (err) {
          d.insert({ somedata: 'another', plus: 'additional data' }, function (err) {
            d.insert({ somedata: 'again' }, function (err) { return cb(err); });
          });
        });
      }
      , function (cb) {   // Test with empty object
        d.find({}, function (err, docs) {
          assert.isNull(err);
          expect(docs.length).to.equal(3);
          expect(docs.map(d => d.somedata)).to.contain('ok');
          expect(docs.map(d => d.somedata)).to.contain('another');
          expect(docs.find(d => d.somedata === 'another').plus).to.equal('additional data');
          expect(docs.map(d => d.somedata)).to.contain('again');
          return cb();
        });
      }
      ], done);
    });

    it('Can find all documents matching a basic query', function (done) {
      async.waterfall([
      function (cb) {
        d.insert({ somedata: 'ok' }, function (err) {
          d.insert({ somedata: 'again', plus: 'additional data' }, function (err) {
            d.insert({ somedata: 'again' }, function (err) { return cb(err); });
          });
        });
      }
      , function (cb) {   // Test with query that will return docs
        d.find({ somedata: 'again' }, function (err, docs) {
          assert.isNull(err);
          expect(docs.length).to.equal(2);
          expect(docs.map(d => d.somedata)).to.not.contain('ok');
          return cb();
        });
      }
      , function (cb) {   // Test with query that doesn't match anything
        d.find({ somedata: 'nope' }, function (err, docs) {
          assert.isNull(err);
          expect(docs.length).to.equal(0);
          return cb();
        });
      }
      ], done);
    });

    it('Can find one document matching a basic query and return null if none is found', function (done) {
      async.waterfall([
      function (cb) {
        d.insert({ somedata: 'ok' }, function (err) {
          d.insert({ somedata: 'again', plus: 'additional data' }, function (err) {
            d.insert({ somedata: 'again' }, function (err) { return cb(err); });
          });
        });
      }
      , function (cb) {   // Test with query that will return docs
        d.findOne({ somedata: 'ok' }, function (err, doc) {
          assert.isNull(err);
           expect(Object.keys(doc).length).to.equal(2);
           expect(doc.somedata).to.equal('ok');
          assert.isDefined(doc._id);
          return cb();
        });
      }
      , function (cb) {   // Test with query that doesn't match anything
        d.findOne({ somedata: 'nope' }, function (err, doc) {
          assert.isNull(err);
          assert.isNull(doc);
          return cb();
        });
      }
      ], done);
    });

    it('Can find dates and objects (non JS-native types)', function (done) {
      var date1 = new Date(1234543)
        , date2 = new Date(9999)
        ;

      d.insert({ now: date1, sth: { name: 'nedb' } }, function () {
        d.findOne({ now: date1 }, function (err, doc) {
          assert.isNull(err);
           expect(doc.sth.name).to.equal('nedb');

          d.findOne({ now: date2 }, function (err, doc) {
            assert.isNull(err);
            assert.isNull(doc);

            d.findOne({ sth: { name: 'nedb' } }, function (err, doc) {
              assert.isNull(err);
               expect(doc.sth.name).to.equal('nedb');

              d.findOne({ sth: { name: 'other' } }, function (err, doc) {
                assert.isNull(err);
                assert.isNull(doc);

                done();
              });
            });
          });
        });
      });
    });

    it('Can use dot-notation to query subfields', function (done) {
      d.insert({ greeting: { english: 'hello' } }, function () {
        d.findOne({ "greeting.english": 'hello' }, function (err, doc) {
          assert.isNull(err);
           expect(doc.greeting.english).to.equal('hello');

          d.findOne({ "greeting.english": 'hellooo' }, function (err, doc) {
            assert.isNull(err);
            assert.isNull(doc);

            d.findOne({ "greeting.englis": 'hello' }, function (err, doc) {
              assert.isNull(err);
              assert.isNull(doc);

              done();
            });
          });
        });
      });
    });

    it('Array fields match if any element matches', function (done) {
      d.insert({ fruits: ['pear', 'apple', 'banana'] }, function (err, doc1) {
        d.insert({ fruits: ['coconut', 'orange', 'pear'] }, function (err, doc2) {
          d.insert({ fruits: ['banana'] }, function (err, doc3) {
            d.find({ fruits: 'pear' }, function (err, docs) {
              assert.isNull(err);
              expect(docs.length).to.equal(2);
              expect(docs.map(d => d._id)).to.contain(doc1._id);
              expect(docs.map(d => d._id)).to.contain(doc2._id);

              d.find({ fruits: 'banana' }, function (err, docs) {
                assert.isNull(err);
                expect(docs.length).to.equal(2);
                expect(docs.map(d => d._id)).to.contain(doc1._id);
                expect(docs.map(d => d._id)).to.contain(doc3._id);

                d.find({ fruits: 'doesntexist' }, function (err, docs) {
                assert.isNull(err);
                expect(docs.length).to.equal(0);

                done();
               });
              });
            });
          });
        });
      });
    });

    it('Returns an error if the query is not well formed', function (done) {
      d.insert({ hello: 'world' }, function () {
        d.find({ $or: { hello: 'world' } }, function (err, docs) {
          assert.isDefined(err);
          assert.isUndefined(docs);

          d.findOne({ $or: { hello: 'world' } }, function (err, doc) {
            assert.isDefined(err);
            assert.isUndefined(doc);

            done();
          });
        });
      });
    });

    it('Changing the documents returned by find or findOne do not change the database state', function (done) {
      d.insert({ a: 2, hello: 'world' }, function () {
        d.findOne({ a: 2 }, function (err, doc) {
          doc.hello = 'changed';

          d.findOne({ a: 2 }, function (err, doc) {
             expect(doc.hello).to.equal('world');

            d.find({ a: 2 }, function (err, docs) {
              docs[0].hello = 'changed';

              d.findOne({ a: 2 }, function (err, doc) {
                 expect(doc.hello).to.equal('world');

                done();
              });
            });
          });
        });
      });
    });
    
    it('Can use sort, skip and limit if the callback is not passed to find but to exec', function (done) {
      d.insert({ a: 2, hello: 'world' }, function () {
        d.insert({ a: 24, hello: 'earth' }, function () {
          d.insert({ a: 13, hello: 'blueplanet' }, function () {
            d.insert({ a: 15, hello: 'home' }, function () {
              d.find({}).sort({ a: 1 }).limit(2).exec(function (err, docs) {
                assert.isNull(err);
                 expect(docs.length).to.equal(2);
                 expect(docs[0].hello).to.equal('world');
                 expect(docs[1].hello).to.equal('blueplanet');
                done();
              });
            });
          });
        });      
      });
    });

     it('Can use sort and skip if the callback is not passed to findOne but to exec', function (done) {
      d.insert({ a: 2, hello: 'world' }, function () {
        d.insert({ a: 24, hello: 'earth' }, function () {
          d.insert({ a: 13, hello: 'blueplanet' }, function () {
            d.insert({ a: 15, hello: 'home' }, function () {
              // No skip no query
              d.findOne({}).sort({ a: 1 }).exec(function (err, doc) {
                assert.isNull(err);
                 expect(doc.hello).to.equal('world');
                
                // A query
                d.findOne({ a: { $gt: 14 } }).sort({ a: 1 }).exec(function (err, doc) {
                  assert.isNull(err);
                   expect(doc.hello).to.equal('home');

                  // And a skip
                  d.findOne({ a: { $gt: 14 } }).sort({ a: 1 }).skip(1).exec(function (err, doc) {
                    assert.isNull(err);
                     expect(doc.hello).to.equal('earth');

                    // No result
                    d.findOne({ a: { $gt: 14 } }).sort({ a: 1 }).skip(2).exec(function (err, doc) {
                      assert.isNull(err);
                      assert.isNull(doc);

                      done();
                    });
                  });
                });
              });
            });
          });
        });
      });
    });

    it('Can use projections in find, normal or cursor way', function (done) {
      d.insert({ a: 2, hello: 'world' }, function (err, doc0) {
        d.insert({ a: 24, hello: 'earth' }, function (err, doc1) {
          d.find({ a: 2 }, { a: 0, _id: 0 }, function (err, docs) {
            assert.isNull(err);
             expect(docs.length).to.equal(1);
            assert.deepEqual(docs[0], { hello: 'world' });

            d.find({ a: 2 }, { a: 0, _id: 0 }).exec(function (err, docs) {
              assert.isNull(err);
               expect(docs.length).to.equal(1);
              assert.deepEqual(docs[0], { hello: 'world' });

              // Can't use both modes at once if not _id
              d.find({ a: 2 }, { a: 0, hello: 1 }, function (err, docs) {
                assert.isNotNull(err);
                assert.isUndefined(docs);

                d.find({ a: 2 }, { a: 0, hello: 1 }).exec(function (err, docs) {
                  assert.isNotNull(err);
                  assert.isUndefined(docs);

                  done();
                });
              });
            });
          });
        });
      });
    });

    it('Can use projections in findOne, normal or cursor way', function (done) {
      d.insert({ a: 2, hello: 'world' }, function (err, doc0) {
        d.insert({ a: 24, hello: 'earth' }, function (err, doc1) {
          d.findOne({ a: 2 }, { a: 0, _id: 0 }, function (err, doc) {
            assert.isNull(err);
            assert.deepEqual(doc, { hello: 'world' });

            d.findOne({ a: 2 }, { a: 0, _id: 0 }).exec(function (err, doc) {
              assert.isNull(err);
              assert.deepEqual(doc, { hello: 'world' });

              // Can't use both modes at once if not _id
              d.findOne({ a: 2 }, { a: 0, hello: 1 }, function (err, doc) {
                assert.isNotNull(err);
                assert.isUndefined(doc);

                d.findOne({ a: 2 }, { a: 0, hello: 1 }).exec(function (err, doc) {
                  assert.isNotNull(err);
                  assert.isUndefined(doc);

                  done();
                });
              });
            });
          });
        });
      });
    });

  });   // ==== End of 'Find' ==== //

  describe('Count', function() {

    it('Count all documents if an empty query is used', function (done) {
      async.waterfall([
      function (cb) {
        d.insert({ somedata: 'ok' }, function (err) {
          d.insert({ somedata: 'another', plus: 'additional data' }, function (err) {
            d.insert({ somedata: 'again' }, function (err) { return cb(err); });
          });
        });
      }
      , function (cb) {   // Test with empty object
        d.count({}, function (err, docs) {
          assert.isNull(err);
           expect(docs).to.equal(3);
          return cb();
        });
      }
      ], done);
    });

    it('Count all documents matching a basic query', function (done) {
      async.waterfall([
      function (cb) {
        d.insert({ somedata: 'ok' }, function (err) {
          d.insert({ somedata: 'again', plus: 'additional data' }, function (err) {
            d.insert({ somedata: 'again' }, function (err) { return cb(err); });
          });
        });
      }
      , function (cb) {   // Test with query that will return docs
        d.count({ somedata: 'again' }, function (err, docs) {
          assert.isNull(err);
           expect(docs).to.equal(2);
          return cb();
        });
      }
      , function (cb) {   // Test with query that doesn't match anything
        d.count({ somedata: 'nope' }, function (err, docs) {
          assert.isNull(err);
           expect(docs).to.equal(0);
          return cb();
        });
      }
      ], done);
    });

    it('Array fields match if any element matches', function (done) {
      d.insert({ fruits: ['pear', 'apple', 'banana'] }, function (err, doc1) {
        d.insert({ fruits: ['coconut', 'orange', 'pear'] }, function (err, doc2) {
          d.insert({ fruits: ['banana'] }, function (err, doc3) {
            d.count({ fruits: 'pear' }, function (err, docs) {
              assert.isNull(err);
               expect(docs).to.equal(2);

              d.count({ fruits: 'banana' }, function (err, docs) {
                assert.isNull(err);
                 expect(docs).to.equal(2);

                d.count({ fruits: 'doesntexist' }, function (err, docs) {
                  assert.isNull(err);
                   expect(docs).to.equal(0);

                  done();
                });
              });
            });
          });
        });
      });
    });

    it('Returns an error if the query is not well formed', function (done) {
      d.insert({ hello: 'world' }, function () {
        d.count({ $or: { hello: 'world' } }, function (err, docs) {
          assert.isDefined(err);
          assert.isUndefined(docs);

          done();
        });
      });
    });

  });

  describe('Update', function () {

    it("If the query doesn't match anything, database is not modified", function (done) {
      async.waterfall([
      function (cb) {
        d.insert({ somedata: 'ok' }, function (err) {
          d.insert({ somedata: 'again', plus: 'additional data' }, function (err) {
            d.insert({ somedata: 'another' }, function (err) { return cb(err); });
          });
        });
      }
      , function (cb) {   // Test with query that doesn't match anything
        d.update({ somedata: 'nope' }, { newDoc: 'yes' }, { multi: true }, function (err, n) {
          assert.isNull(err);
           expect(n).to.equal(0);

          d.find({}, function (err, docs) {
            var doc1 = docs.find(d => d.somedata === 'ok')
              , doc2 = docs.find(d => d.somedata === 'again')
              , doc3 = docs.find(d => d.somedata === 'another')
              ;
 
            expect(docs.length).to.equal(3);
            assert.isUndefined(docs.find(d => d.newDoc === 'yes'));

            assert.deepEqual(doc1, { _id: doc1._id, somedata: 'ok' });
            assert.deepEqual(doc2, { _id: doc2._id, somedata: 'again', plus: 'additional data' });
            assert.deepEqual(doc3, { _id: doc3._id, somedata: 'another' });

            return cb();
          });
        });
      }
      ], done);
    });

    it("If timestampData option is set, update the updatedAt field", function (done) {
      var beginning = Date.now();
      d = new Datastore({ filename: testDb, autoload: true, timestampData: true });
      d.insert({ hello: 'world' }, function (err, insertedDoc) {
        assert.isBelow(insertedDoc.updatedAt.getTime() - beginning, reloadTimeUpperBound);
        assert.isBelow(insertedDoc.createdAt.getTime() - beginning, reloadTimeUpperBound);
         expect(Object.keys(insertedDoc).length).to.equal(4);

        // Wait 100ms before performing the update
        setTimeout(function () {
          var step1 = Date.now();
          d.update({ _id: insertedDoc._id }, { $set: { hello: 'mars' } }, {}, function () {
            d.find({ _id: insertedDoc._id }, function (err, docs) {
               expect(docs.length).to.equal(1);
               expect(Object.keys(docs[0]).length).to.equal(4);
               expect(docs[0]._id).to.equal(insertedDoc._id);
               expect(docs[0].createdAt).to.equal(insertedDoc.createdAt);
               expect(docs[0].hello).to.equal('mars');
              assert.isAbove(docs[0].updatedAt.getTime() - beginning, 99);   // updatedAt modified
              assert.isBelow(docs[0].updatedAt.getTime() - step1, reloadTimeUpperBound);   // updatedAt modified

              done();
            });
          })
        }, 100);
      });
    });

    it("Can update multiple documents matching the query", function (done) {
      var id1, id2, id3;

      // Test DB state after update and reload
      function testPostUpdateState (cb) {
        d.find({}, function (err, docs) {
          var doc1 = docs.find(d =>d._id === id1)
            , doc2 = docs.find(d =>d._id === id2)
            , doc3 = docs.find(d =>d._id === id3)
            ;
 
          expect(docs.length).to.equal(3);
 
          expect(Object.keys(doc1).length).to.equal(2);
           expect(doc1.somedata).to.equal('ok');
           expect(doc1._id).to.equal(id1);
 
          expect(Object.keys(doc2).length).to.equal(2);
           expect(doc2.newDoc).to.equal('yes');
           expect(doc2._id).to.equal(id2);
 
          expect(Object.keys(doc3).length).to.equal(2);
           expect(doc3.newDoc).to.equal('yes');
           expect(doc3._id).to.equal(id3);

          return cb();
        });
      }

      // Actually launch the tests
      async.waterfall([
      function (cb) {
        d.insert({ somedata: 'ok' }, function (err, doc1) {
          id1 = doc1._id;
          d.insert({ somedata: 'again', plus: 'additional data' }, function (err, doc2) {
            id2 = doc2._id;
            d.insert({ somedata: 'again' }, function (err, doc3) {
              id3 = doc3._id;
              return cb(err);
            });
          });
        });
      }
      , function (cb) {
        d.update({ somedata: 'again' }, { newDoc: 'yes' }, { multi: true }, function (err, n) {
          assert.isNull(err);
           expect(n).to.equal(2);
          return cb();
        });
      }
      , async.apply(testPostUpdateState)
      , function (cb) {
        d.loadDatabase(function (err) { cb(err); });
      }
      , async.apply(testPostUpdateState)
      ], done);
    });

    it("Can update only one document matching the query", function (done) {
      var id1, id2, id3;

      // Test DB state after update and reload
      function testPostUpdateState (cb) {
        d.find({}, function (err, docs) {
          var doc1 = docs.find(d =>d._id === id1)
            , doc2 = docs.find(d =>d._id === id2)
            , doc3 = docs.find(d =>d._id === id3)
            ;
 
          expect(docs.length).to.equal(3);

          assert.deepEqual(doc1, { somedata: 'ok', _id: doc1._id });

          // doc2 or doc3 was modified. Since we sort on _id and it is random
          // it can be either of two situations
          try {
            assert.deepEqual(doc2, { newDoc: 'yes', _id: doc2._id });
            assert.deepEqual(doc3, { somedata: 'again', _id: doc3._id });
          } catch (e) {
            assert.deepEqual(doc2, { somedata: 'again', plus: 'additional data', _id: doc2._id });
            assert.deepEqual(doc3, { newDoc: 'yes', _id: doc3._id });
          }

          return cb();
        });
      }

      // Actually launch the test
      async.waterfall([
      function (cb) {
        d.insert({ somedata: 'ok' }, function (err, doc1) {
          id1 = doc1._id;
          d.insert({ somedata: 'again', plus: 'additional data' }, function (err, doc2) {
            id2 = doc2._id;
            d.insert({ somedata: 'again' }, function (err, doc3) {
              id3 = doc3._id;
              return cb(err);
            });
          });
        });
      }
      , function (cb) {   // Test with query that doesn't match anything
        d.update({ somedata: 'again' }, { newDoc: 'yes' }, { multi: false }, function (err, n) {
          assert.isNull(err);
           expect(n).to.equal(1);
          return cb();
        });
      }
      , async.apply(testPostUpdateState)
      , function (cb) {
        d.loadDatabase(function (err) { return cb(err); });
      }
      , async.apply(testPostUpdateState)   // The persisted state has been updated
      ], done);
    });

    describe('Upserts', function () {

      it('Can perform upserts if needed', function (done) {
        d.update({ impossible: 'db is empty anyway' }, { newDoc: true }, {}, function (err, nr, upsert) {
          assert.isNull(err);
           expect(nr).to.equal(0);
          assert.isUndefined(upsert);

          d.find({}, function (err, docs) {
             expect(docs.length).to.equal(0);   // Default option for upsert is false

            d.update({ impossible: 'db is empty anyway' }, { something: "created ok" }, { upsert: true }, function (err, nr, newDoc) {
              assert.isNull(err);
               expect(nr).to.equal(1);
               expect(newDoc.something).to.equal("created ok");
              assert.isDefined(newDoc._id);

              d.find({}, function (err, docs) {
                 expect(docs.length).to.equal(1);   // Default option for upsert is false
                 expect(docs[0].something).to.equal("created ok");
                
                // Modifying the returned upserted document doesn't modify the database
                newDoc.newField = true;
                d.find({}, function (err, docs) {
                   expect(docs[0].something).to.equal("created ok");
                  assert.isUndefined(docs[0].newField);
                
                  done();
                });
              });
            });
          });
        });
      });
      
      it('If the update query is a normal object with no modifiers, it is the doc that will be upserted', function (done) {
        d.update({ $or: [{ a: 4 }, { a: 5 }] }, { hello: 'world', bloup: 'blap' }, { upsert: true }, function (err) {
          d.find({}, function (err, docs) {
            assert.isNull(err);
             expect(docs.length).to.equal(1);
            var doc = docs[0];
             expect(Object.keys(doc).length).to.equal(3);
             expect(doc.hello).to.equal('world');
             expect(doc.bloup).to.equal('blap');
            done();
          });
        });
      });
      
      it('If the update query contains modifiers, it is applied to the object resulting from removing all operators from the find query 1', function (done) {
        d.update({ $or: [{ a: 4 }, { a: 5 }] }, { $set: { hello: 'world' }, $inc: { bloup: 3 } }, { upsert: true }, function (err) {
          d.find({ hello: 'world' }, function (err, docs) {
            assert.isNull(err);
             expect(docs.length).to.equal(1);
            var doc = docs[0];
             expect(Object.keys(doc).length).to.equal(3);
             expect(doc.hello).to.equal('world');
             expect(doc.bloup).to.equal(3);
            done();
          });
        });
      });
      
      it('If the update query contains modifiers, it is applied to the object resulting from removing all operators from the find query 2', function (done) {
        d.update({ $or: [{ a: 4 }, { a: 5 }], cac: 'rrr' }, { $set: { hello: 'world' }, $inc: { bloup: 3 } }, { upsert: true }, function (err) {
          d.find({ hello: 'world' }, function (err, docs) {
            assert.isNull(err);
             expect(docs.length).to.equal(1);
            var doc = docs[0];
             expect(Object.keys(doc).length).to.equal(4);
             expect(doc.cac).to.equal('rrr');
             expect(doc.hello).to.equal('world');
             expect(doc.bloup).to.equal(3);
            done();
          });
        });
      });
      
      it('Performing upsert with badly formatted fields yields a standard error not an exception', function(done) {
        d.update({_id: '1234'}, { $set: { $$badfield: 5 }}, { upsert: true }, function(err, doc) {
          assert.isDefined(err);
          done();
        })
      });


    });   // ==== End of 'Upserts' ==== //

    it('Cannot perform update if the update query is not either registered-modifiers-only or copy-only, or contain badly formatted fields', function (done) {
      d.insert({ something: 'yup' }, function () {
        d.update({}, { boom: { $badfield: 5 } }, { multi: false }, function (err) {
          assert.isDefined(err);

          d.update({}, { boom: { "bad.field": 5 } }, { multi: false }, function (err) {
            assert.isDefined(err);

            d.update({}, { $inc: { test: 5 }, mixed: 'rrr' }, { multi: false }, function (err) {
              assert.isDefined(err);

              d.update({}, { $inexistent: { test: 5 } }, { multi: false }, function (err) {
                assert.isDefined(err);

                done();
              });
            });
          });
        });
      });
    });

    it('Can update documents using multiple modifiers', function (done) {
      var id;

      d.insert({ something: 'yup', other: 40 }, function (err, newDoc) {
        id = newDoc._id;

        d.update({}, { $set: { something: 'changed' }, $inc: { other: 10 } }, { multi: false }, function (err, nr) {
          assert.isNull(err);
           expect(nr).to.equal(1);

          d.findOne({ _id: id }, function (err, doc) {
             expect(Object.keys(doc).length).to.equal(3);
             expect(doc._id).to.equal(id);
             expect(doc.something).to.equal('changed');
             expect(doc.other).to.equal(50);

            done();
          });
        });
      });
    });

    it('Can upsert a document even with modifiers', function (done) {
      d.update({ bloup: 'blap' }, { $set: { hello: 'world' } }, { upsert: true }, function (err, nr, newDoc) {
        assert.isNull(err);
         expect(nr).to.equal(1);
         expect(newDoc.bloup).to.equal('blap');
         expect(newDoc.hello).to.equal('world');
        assert.isDefined(newDoc._id);

        d.find({}, function (err, docs) {
           expect(docs.length).to.equal(1);
           expect(Object.keys(docs[0]).length).to.equal(3);
           expect(docs[0].hello).to.equal('world');
           expect(docs[0].bloup).to.equal('blap');
          assert.isDefined(docs[0]._id);

          done();
        });
      });
    });

    it('When using modifiers, the only way to update subdocs is with the dot-notation', function (done) {
      d.insert({ bloup: { blip: "blap", other: true } }, function () {
        // Correct methos
        d.update({}, { $set: { "bloup.blip": "hello" } }, {}, function () {
          d.findOne({}, function (err, doc) {
             expect(doc.bloup.blip).to.equal("hello");
             expect(doc.bloup.other).to.equal(true);

            // Wrong
            d.update({}, { $set: { bloup: { blip: "ola" } } }, {}, function () {
              d.findOne({}, function (err, doc) {
                 expect(doc.bloup.blip).to.equal("ola");
                assert.isUndefined(doc.bloup.other);   // This information was lost

                done();
              });
            });
          });
        });
      });
    });

    it('Returns an error if the query is not well formed', function (done) {
      d.insert({ hello: 'world' }, function () {
        d.update({ $or: { hello: 'world' } }, { a: 1 }, {}, function (err, nr, upsert) {
          assert.isDefined(err);
          assert.isUndefined(nr);
          assert.isUndefined(upsert);

          done();
        });
      });
    });

    it('If an error is thrown by a modifier, the database state is not changed', function (done) {
      d.insert({ hello: 'world' }, function (err, newDoc) {
        d.update({}, { $inc: { hello: 4 } }, {}, function (err, nr) {
          assert.isDefined(err);
          assert.isUndefined(nr);

          d.find({}, function (err, docs) {
            assert.deepEqual(docs, [ { _id: newDoc._id, hello: 'world' } ]);

            done();
          });
        });
      });
    });

    it('Cant change the _id of a document', function (done) {
      d.insert({ a: 2 }, function (err, newDoc) {
        d.update({ a: 2 }, { a: 2, _id: 'nope' }, {}, function (err) {
          assert.isDefined(err);

          d.find({}, function (err, docs) {
             expect(docs.length).to.equal(1);
             expect(Object.keys(docs[0]).length).to.equal(2);
             expect(docs[0].a).to.equal(2);
             expect(docs[0]._id).to.equal(newDoc._id);

            d.update({ a: 2 }, { $set: { _id: 'nope' } }, {}, function (err) {
              assert.isDefined(err);

              d.find({}, function (err, docs) {
                 expect(docs.length).to.equal(1);
                 expect(Object.keys(docs[0]).length).to.equal(2);
                 expect(docs[0].a).to.equal(2);
                 expect(docs[0]._id).to.equal(newDoc._id);

                done();
              });
            });
          });
        });
      });
    });

    it('Non-multi updates are persistent', function (done) {
      d.insert({ a:1, hello: 'world' }, function (err, doc1) {
        d.insert({ a:2, hello: 'earth' }, function (err, doc2) {
          d.update({ a: 2 }, { $set: { hello: 'changed' } }, {}, function (err) {
            assert.isNull(err);

            d.find({}, function (err, docs) {
              docs.sort(function (a, b) { return a.a - b.a; });
               expect(docs.length).to.equal(2);
               expect(docs[0]).to.deep.equal({ _id: doc1._id, a:1, hello: 'world' });
               expect(docs[1]).to.deep.equal({ _id: doc2._id, a:2, hello: 'changed' });

              // Even after a reload the database state hasn't changed
              d.loadDatabase(function (err) {
                assert.isNull(err);

                d.find({}, function (err, docs) {
                  docs.sort(function (a, b) { return a.a - b.a; });
                   expect(docs.length).to.equal(2);
                   expect(docs[0]).to.deep.equal({ _id: doc1._id, a:1, hello: 'world' });
                   expect(docs[1]).to.deep.equal({ _id: doc2._id, a:2, hello: 'changed' });

                  done();
                });
              });
            });
          });
        });
      });
    });

    it('Multi updates are persistent', function (done) {
      d.insert({ a:1, hello: 'world' }, function (err, doc1) {
        d.insert({ a:2, hello: 'earth' }, function (err, doc2) {
          d.insert({ a:5, hello: 'pluton' }, function (err, doc3) {
            d.update({ a: { $in: [1, 2] } }, { $set: { hello: 'changed' } }, { multi: true }, function (err) {
              assert.isNull(err);

              d.find({}, function (err, docs) {
                docs.sort(function (a, b) { return a.a - b.a; });
                 expect(docs.length).to.equal(3);
                 expect(docs[0]).to.deep.equal({ _id: doc1._id, a:1, hello: 'changed' });
                 expect(docs[1]).to.deep.equal({ _id: doc2._id, a:2, hello: 'changed' });
                 expect(docs[2]).to.deep.equal({ _id: doc3._id, a:5, hello: 'pluton' });

                // Even after a reload the database state hasn't changed
                d.loadDatabase(function (err) {
                  assert.isNull(err);

                  d.find({}, function (err, docs) {
                    docs.sort(function (a, b) { return a.a - b.a; });
                     expect(docs.length).to.equal(3);
                     expect(docs[0]).to.deep.equal({ _id: doc1._id, a:1, hello: 'changed' });
                     expect(docs[1]).to.deep.equal({ _id: doc2._id, a:2, hello: 'changed' });
                     expect(docs[2]).to.deep.equal({ _id: doc3._id, a:5, hello: 'pluton' });

                    done();
                  });
                });
              });
            });
          });
        });
      });
    });
    
    it('Can update without the options arg (will use defaults then)', function (done) {
      d.insert({ a:1, hello: 'world' }, function (err, doc1) {
        d.insert({ a:2, hello: 'earth' }, function (err, doc2) {
          d.insert({ a:5, hello: 'pluton' }, function (err, doc3) {
            d.update({ a: 2 }, { $inc: { a: 10 } }, function (err, nr) {
              assert.isNull(err);
               expect(nr).to.equal(1);
              d.find({}, function (err, docs) {
                var d1 = docs.find(doc => doc._id === doc1._id)
                  , d2 = docs.find(doc => doc._id === doc2._id)
                  , d3 = docs.find(doc => doc._id === doc3._id)
                  ;
                   
                expect(d1.a).to.equal(1);
                 expect(d2.a).to.equal(12);
                 expect(d3.a).to.equal(5);
                
                done();
              });
            });
          });
        });
      });
    });

    it('If a multi update fails on one document, previous updates should be rolled back', function (done) {
      d.ensureIndex({ fieldName: 'a' });
      d.insert({ a: 4 }, function (err, doc1) {
        d.insert({ a: 5 }, function (err, doc2) {
          d.insert({ a: 'abc' }, function (err, doc3) {
            // With this query, candidates are always returned in the order 4, 5, 'abc' so it's always the last one which fails
            d.update({ a: { $in: [4, 5, 'abc'] } }, { $inc: { a: 10 } }, { multi: true }, function (err) {
              assert.isDefined(err);

              // No index modified
              Object.values(d.indexes).forEach(index => {
                var docs = index.getAll()
                  , d1 = docs.find(doc => doc._id === doc1._id)
                  , d2 = docs.find(doc => doc._id === doc2._id)
                  , d3 = docs.find(doc => doc._id === doc3._id)
                  ;

                // All changes rolled back, including those that didn't trigger an error
                 expect(d1.a).to.equal(4);
                 expect(d2.a).to.equal(5);
                 expect(d3.a).to.equal('abc');
              });

              done();
            });
          });
        });
      });
    });

    it('If an index constraint is violated by an update, all changes should be rolled back', function (done) {
      d.ensureIndex({ fieldName: 'a', unique: true });
      d.insert({ a: 4 }, function (err, doc1) {
        d.insert({ a: 5 }, function (err, doc2) {
          // With this query, candidates are always returned in the order 4, 5, 'abc' so it's always the last one which fails
           d.update({ a: { $in: [4, 5, 'abc'] } }, { $set: { a: 10 } }, { multi: true }, function (err) {
            assert.isDefined(err);

            // Check that no index was modified
            Object.values(d.indexes).forEach(index => {
              var docs = index.getAll()
              , d1 = docs.find(doc => doc._id === doc1._id)
              , d2 = docs.find(doc => doc._id === doc2._id)
              ;
 
              expect(d1.a).to.equal(4);
              expect(d2.a).to.equal(5);
            });

            done();
          });
        });
      });
    });

    it("If options.returnUpdatedDocs is true, return all matched docs", function (done) {
      d.insert([{ a: 4 }, { a: 5 }, { a: 6 }], function (err, docs) {
         expect(docs.length).to.equal(3);

        d.update({ a: 7 }, { $set: { u: 1 } }, { multi: true, returnUpdatedDocs: true }, function (err, num, updatedDocs) {
           expect(num).to.equal(0);
           expect(updatedDocs.length).to.equal(0);

          d.update({ a: 5 }, { $set: { u: 2 } }, { multi: true, returnUpdatedDocs: true }, function (err, num, updatedDocs) {
             expect(num).to.equal(1);
             expect(updatedDocs.length).to.equal(1);
             expect(updatedDocs[0].a).to.equal(5);
             expect(updatedDocs[0].u).to.equal(2);

            d.update({ a: { $in: [4, 6] } }, { $set: { u: 3 } }, { multi: true, returnUpdatedDocs: true }, function (err, num, updatedDocs) {
               expect(num).to.equal(2);
               expect(updatedDocs.length).to.equal(2);
               expect(updatedDocs[0].u).to.equal(3);
               expect(updatedDocs[1].u).to.equal(3);
              if (updatedDocs[0].a === 4) {
                 expect(updatedDocs[0].a).to.equal(4);
                 expect(updatedDocs[1].a).to.equal(6);
              } else {
                 expect(updatedDocs[0].a).to.equal(6);
                 expect(updatedDocs[1].a).to.equal(4);
              }

              done();
            });
          });
        });
      });
    });

    it("createdAt property is unchanged and updatedAt correct after an update, even a complete document replacement", function (done) {
      var d2 = new Datastore({ inMemoryOnly: true, timestampData: true });
      d2.insert({ a: 1 });
      d2.findOne({ a: 1 }, function (err, doc) {
        var createdAt = doc.createdAt.getTime();

        // Modifying update
        setTimeout(function () {
          d2.update({ a: 1 }, { $set: { b: 2 } }, {});
          d2.findOne({ a: 1 }, function (err, doc) {
             expect(doc.createdAt.getTime()).to.equal(createdAt);
            assert.isBelow(Date.now() - doc.updatedAt.getTime(), 5);

            // Complete replacement
            setTimeout(function () {
              d2.update({ a: 1 }, { c: 3 }, {});
              d2.findOne({ c: 3 }, function (err, doc) {
                 expect(doc.createdAt.getTime()).to.equal(createdAt);
                assert.isBelow(Date.now() - doc.updatedAt.getTime(), 5);

                done();
              });
            }, 20);
          });
        }, 20);
      });
    });


    describe("Callback signature", function () {

      it("Regular update, multi false", function (done) {
        d.insert({ a: 1 });
        d.insert({ a: 2 });

        // returnUpdatedDocs set to false
        d.update({ a: 1 }, { $set: { b: 20 } }, {}, function (err, numAffected, affectedDocuments, upsert) {
          assert.isNull(err);
           expect(numAffected).to.equal(1);
          assert.isUndefined(affectedDocuments);
          assert.isUndefined(upsert);

          // returnUpdatedDocs set to true
          d.update({ a: 1 }, { $set: { b: 21 } }, { returnUpdatedDocs: true }, function (err, numAffected, affectedDocuments, upsert) {
            assert.isNull(err);
             expect(numAffected).to.equal(1);
             expect(affectedDocuments.a).to.equal(1);
             expect(affectedDocuments.b).to.equal(21);
            assert.isUndefined(upsert);

            done();
          });
        });
      });

      it("Regular update, multi true", function (done) {
        d.insert({ a: 1 });
        d.insert({ a: 2 });

        // returnUpdatedDocs set to false
        d.update({}, { $set: { b: 20 } }, { multi: true }, function (err, numAffected, affectedDocuments, upsert) {
          assert.isNull(err);
           expect(numAffected).to.equal(2);
          assert.isUndefined(affectedDocuments);
          assert.isUndefined(upsert);

          // returnUpdatedDocs set to true
          d.update({}, { $set: { b: 21 } }, { multi: true, returnUpdatedDocs: true }, function (err, numAffected, affectedDocuments, upsert) {
            assert.isNull(err);
             expect(numAffected).to.equal(2);
             expect(affectedDocuments.length).to.equal(2);
            assert.isUndefined(upsert);

            done();
          });
        });
      });

      it("Upsert", function (done) {
        d.insert({ a: 1 });
        d.insert({ a: 2 });

        // Upsert flag not set
        d.update({ a: 3 }, { $set: { b: 20 } }, {}, function (err, numAffected, affectedDocuments, upsert) {
          assert.isNull(err);
           expect(numAffected).to.equal(0);
          assert.isUndefined(affectedDocuments);
          assert.isUndefined(upsert);

          // Upsert flag set
          d.update({ a: 3 }, { $set: { b: 21 } }, { upsert: true }, function (err, numAffected, affectedDocuments, upsert) {
            assert.isNull(err);
             expect(numAffected).to.equal(1);
             expect(affectedDocuments.a).to.equal(3);
             expect(affectedDocuments.b).to.equal(21);
             expect(upsert).to.equal(true);

            d.find({}, function (err, docs) {
               expect(docs.length).to.equal(3);
              done();
            });
          });
        });
      });


    });   // ==== End of 'Update - Callback signature' ==== //

  });   // ==== End of 'Update' ==== //


  describe('Remove', function () {

    it('Can remove multiple documents', function (done) {
      var id1, id2, id3;

      // Test DB status
      function testPostUpdateState (cb) {
        d.find({}, function (err, docs) {
           expect(docs.length).to.equal(1);
 
          expect(Object.keys(docs[0]).length).to.equal(2);
           expect(docs[0]._id).to.equal(id1);
           expect(docs[0].somedata).to.equal('ok');

          return cb();
        });
      }

      // Actually launch the test
      async.waterfall([
      function (cb) {
        d.insert({ somedata: 'ok' }, function (err, doc1) {
          id1 = doc1._id;
          d.insert({ somedata: 'again', plus: 'additional data' }, function (err, doc2) {
            id2 = doc2._id;
            d.insert({ somedata: 'again' }, function (err, doc3) {
              id3 = doc3._id;
              return cb(err);
            });
          });
        });
      }
      , function (cb) {   // Test with query that doesn't match anything
        d.remove({ somedata: 'again' }, { multi: true }, function (err, n) {
          assert.isNull(err);
           expect(n).to.equal(2);
          return cb();
        });
      }
      , async.apply(testPostUpdateState)
      , function (cb) {
        d.loadDatabase(function (err) { return cb(err); });
      }
      , async.apply(testPostUpdateState)
      ], done);
    });

    // This tests concurrency issues
    it('Remove can be called multiple times in parallel and everything that needs to be removed will be', function (done) {
      d.insert({ planet: 'Earth' }, function () {
        d.insert({ planet: 'Mars' }, function () {
          d.insert({ planet: 'Saturn' }, function () {
            d.find({}, function (err, docs) {
               expect(docs.length).to.equal(3);

              // Remove two docs simultaneously
              var toRemove = ['Mars', 'Saturn'];
              async.each(toRemove, function(planet, cb) {
                d.remove({ planet: planet }, function (err) { return cb(err); });
              }, function (err) {
                d.find({}, function (err, docs) {
                   expect(docs.length).to.equal(1);

                  done();
                });
              });
            });
          });
        });
      });
    });

    it('Returns an error if the query is not well formed', function (done) {
      d.insert({ hello: 'world' }, function () {
        d.remove({ $or: { hello: 'world' } }, {}, function (err, nr, upsert) {
          assert.isDefined(err);
          assert.isUndefined(nr);
          assert.isUndefined(upsert);

          done();
        });
      });
    });

    it('Non-multi removes are persistent', function (done) {
      d.insert({ a:1, hello: 'world' }, function (err, doc1) {
        d.insert({ a:2, hello: 'earth' }, function (err, doc2) {
          d.insert({ a:3, hello: 'moto' }, function (err, doc3) {
            d.remove({ a: 2 }, {}, function (err) {
              assert.isNull(err);

              d.find({}, function (err, docs) {
                docs.sort(function (a, b) { return a.a - b.a; });
                 expect(docs.length).to.equal(2);
                 expect(docs[0]).to.deep.equal({ _id: doc1._id, a:1, hello: 'world' });
                 expect(docs[1]).to.deep.equal({ _id: doc3._id, a:3, hello: 'moto' });

                // Even after a reload the database state hasn't changed
                d.loadDatabase(function (err) {
                  assert.isNull(err);

                  d.find({}, function (err, docs) {
                    docs.sort(function (a, b) { return a.a - b.a; });
                     expect(docs.length).to.equal(2);
                     expect(docs[0]).to.deep.equal({ _id: doc1._id, a:1, hello: 'world' });
                     expect(docs[1]).to.deep.equal({ _id: doc3._id, a:3, hello: 'moto' });

                    done();
                  });
                });
              });
            });
          });
        });
      });
    });

    it('Multi removes are persistent', function (done) {
      d.insert({ a:1, hello: 'world' }, function (err, doc1) {
        d.insert({ a:2, hello: 'earth' }, function (err, doc2) {
          d.insert({ a:3, hello: 'moto' }, function (err, doc3) {
            d.remove({ a: { $in: [1, 3] } }, { multi: true }, function (err) {
              assert.isNull(err);

              d.find({}, function (err, docs) {
                 expect(docs.length).to.equal(1);
                 expect(docs[0]).to.deep.equal({ _id: doc2._id, a:2, hello: 'earth' });

                // Even after a reload the database state hasn't changed
                d.loadDatabase(function (err) {
                  assert.isNull(err);

                  d.find({}, function (err, docs) {
                     expect(docs.length).to.equal(1);
                     expect(docs[0]).to.deep.equal({ _id: doc2._id, a:2, hello: 'earth' });

                    done();
                  });
                });
              });
            });
          });
        });
      });
    });
    
    it('Can remove without the options arg (will use defaults then)', function (done) {
      d.insert({ a:1, hello: 'world' }, function (err, doc1) {
        d.insert({ a:2, hello: 'earth' }, function (err, doc2) {
          d.insert({ a:5, hello: 'pluton' }, function (err, doc3) {
            d.remove({ a: 2 }, function (err, nr) {
              assert.isNull(err);
               expect(nr).to.equal(1);
              d.find({}, function (err, docs) {
                var d1 = docs.find(doc => doc._id === doc1._id)
                  , d2 = docs.find(doc => doc._id === doc2._id)
                  , d3 = docs.find(doc => doc._id === doc3._id)
                  ;
                   
                expect(d1.a).to.equal(1);
                assert.isUndefined(d2);
                 expect(d3.a).to.equal(5);
                
                done();
              });
            });
          });
        });
      });
    });

  });   // ==== End of 'Remove' ==== //


  describe('Using indexes', function () {

    describe('ensureIndex and index initialization in database loading', function () {

      it('ensureIndex can be called right after a loadDatabase and be initialized and filled correctly', function (done) {
        var now = new Date()
          , rawData = serialize({ _id: "aaa", z: "1", a: 2, ages: [1, 5, 12] }) + '\n' +
                      serialize({ _id: "bbb", z: "2", hello: 'world' }) + '\n' +
                      serialize({ _id: "ccc", z: "3", nested: { today: now } })
          ;
 
        expect(d.getAllData().length).to.equal(0);

        fs.writeFile(testDb, rawData, 'utf8', function () {
          d.loadDatabase(function () {
             expect(d.getAllData().length).to.equal(3);

            assert.deepEqual(Object.keys(d.indexes), ['_id']);

            d.ensureIndex({ fieldName: 'z' });
             expect(d.indexes.z.fieldName).to.equal('z');
             expect(d.indexes.z.unique).to.equal(false);
             expect(d.indexes.z.sparse).to.equal(false);
             expect(d.indexes.z._getNumberOfKeys()).to.equal(3);
             expect(d.indexes.z.tree.search('1')[0]).to.equal(d.getAllData()[0]);
             expect(d.indexes.z.tree.search('2')[0]).to.equal(d.getAllData()[1]);
             expect(d.indexes.z.tree.search('3')[0]).to.equal(d.getAllData()[2]);

            done();
          });
        });
      });
      
      it('ensureIndex can be called twice on the same field, the second call will ahve no effect', function (done) {
         expect(Object.keys(d.indexes).length).to.equal(1);
         expect(Object.keys(d.indexes)[0]).to.equal("_id");
      
        d.insert({ planet: "Earth" }, function () {
          d.insert({ planet: "Mars" }, function () {
            d.find({}, function (err, docs) {
               expect(docs.length).to.equal(2);
              
              d.ensureIndex({ fieldName: "planet" }, function (err) {
                assert.isNull(err);
                 expect(Object.keys(d.indexes).length).to.equal(2);
                 expect(Object.keys(d.indexes)[0]).to.equal("_id");   
                 expect(Object.keys(d.indexes)[1]).to.equal("planet");   
 
                expect(d.indexes.planet.getAll().length).to.equal(2);
                
                // This second call has no effect, documents don't get inserted twice in the index
                d.ensureIndex({ fieldName: "planet" }, function (err) {
                  assert.isNull(err);
                   expect(Object.keys(d.indexes).length).to.equal(2);
                   expect(Object.keys(d.indexes)[0]).to.equal("_id");   
                   expect(Object.keys(d.indexes)[1]).to.equal("planet");   
 
                  expect(d.indexes.planet.getAll().length).to.equal(2);                
                  
                  done();
                });
              });
            });
          });
        });
      });

      it('ensureIndex can be called after the data set was modified and the index still be correct', function (done) {
        var rawData = serialize({ _id: "aaa", z: "1", a: 2, ages: [1, 5, 12] }) + '\n' +
                      serialize({ _id: "bbb", z: "2", hello: 'world' })
          ;
 
        expect(d.getAllData().length).to.equal(0);

        fs.writeFile(testDb, rawData, 'utf8', function () {
          d.loadDatabase(function () {
             expect(d.getAllData().length).to.equal(2);

            assert.deepEqual(Object.keys(d.indexes), ['_id']);

            d.insert({ z: "12", yes: 'yes' }, function (err, newDoc1) {
              d.insert({ z: "14", nope: 'nope' }, function (err, newDoc2) {
                d.remove({ z: "2" }, {}, function () {
                  d.update({ z: "1" }, { $set: { 'yes': 'yep' } }, {}, function () {
                    assert.deepEqual(Object.keys(d.indexes), ['_id']);

                    d.ensureIndex({ fieldName: 'z' });
                     expect(d.indexes.z.fieldName).to.equal('z');
                     expect(d.indexes.z.unique).to.equal(false);
                     expect(d.indexes.z.sparse).to.equal(false);
                     expect(d.indexes.z._getNumberOfKeys()).to.equal(3);

                    // The pointers in the _id and z indexes are the same
                     expect(d.indexes.z.tree.search('1')[0]).to.equal(d.indexes._id.getMatching('aaa')[0]);
                     expect(d.indexes.z.tree.search('12')[0]).to.equal(d.indexes._id.getMatching(newDoc1._id)[0]);
                     expect(d.indexes.z.tree.search('14')[0]).to.equal(d.indexes._id.getMatching(newDoc2._id)[0]);

                    // The data in the z index is correct
                    d.find({}, function (err, docs) {
                      var doc0 = docs.find(doc => doc._id === 'aaa')
                        , doc1 = docs.find(doc => doc._id === newDoc1._id)
                        , doc2 = docs.find(doc => doc._id === newDoc2._id)
                        ;
 
                      expect(docs.length).to.equal(3);

                      assert.deepEqual(doc0, { _id: "aaa", z: "1", a: 2, ages: [1, 5, 12], yes: 'yep' });
                      assert.deepEqual(doc1, { _id: newDoc1._id, z: "12", yes: 'yes' });
                      assert.deepEqual(doc2, { _id: newDoc2._id, z: "14", nope: 'nope' });

                      done();
                    });
                  });
                });
              });
            });
          });
        });
      });

      it('ensureIndex can be called before a loadDatabase and still be initialized and filled correctly', function (done) {
        var now = new Date()
          , rawData = serialize({ _id: "aaa", z: "1", a: 2, ages: [1, 5, 12] }) + '\n' +
                      serialize({ _id: "bbb", z: "2", hello: 'world' }) + '\n' +
                      serialize({ _id: "ccc", z: "3", nested: { today: now } })
          ;
 
        expect(d.getAllData().length).to.equal(0);

        d.ensureIndex({ fieldName: 'z' });
         expect(d.indexes.z.fieldName).to.equal('z');
         expect(d.indexes.z.unique).to.equal(false);
         expect(d.indexes.z.sparse).to.equal(false);
         expect(d.indexes.z._getNumberOfKeys()).to.equal(0);

        fs.writeFile(testDb, rawData, 'utf8', function () {
          d.loadDatabase(function () {
            var doc1 = d.getAllData().find(doc => doc.z === "1")
              , doc2 = d.getAllData().find(doc => doc.z === "2")
              , doc3 = d.getAllData().find(doc => doc.z === "3")
              ;
 
            expect(d.getAllData().length).to.equal(3);
 
            expect(d.indexes.z._getNumberOfKeys()).to.equal(3);
             expect(d.indexes.z.tree.search('1')[0]).to.equal(doc1);
             expect(d.indexes.z.tree.search('2')[0]).to.equal(doc2);
             expect(d.indexes.z.tree.search('3')[0]).to.equal(doc3);

            done();
          });
        });
      });

      it('Can initialize multiple indexes on a database load', function (done) {
        var now = new Date()
          , rawData = serialize({ _id: "aaa", z: "1", a: 2, ages: [1, 5, 12] }) + '\n' +
                      serialize({ _id: "bbb", z: "2", a: 'world' }) + '\n' +
                      serialize({ _id: "ccc", z: "3", a: { today: now } })
          ;
 
        expect(d.getAllData().length).to.equal(0);
        d.ensureIndex({ fieldName: 'z' }, function () {
          d.ensureIndex({ fieldName: 'a' }, function () {
             expect(d.indexes.a._getNumberOfKeys()).to.equal(0);
             expect(d.indexes.z._getNumberOfKeys()).to.equal(0);

            fs.writeFile(testDb, rawData, 'utf8', function () {
              d.loadDatabase(function (err) {
                var doc1 = d.getAllData().find(doc => doc.z === "1")
                  , doc2 = d.getAllData().find(doc => doc.z === "2")
                  , doc3 = d.getAllData().find(doc => doc.z === "3")
                  ;

                assert.isNull(err);
                 expect(d.getAllData().length).to.equal(3);
 
                expect(d.indexes.z._getNumberOfKeys()).to.equal(3);
                 expect(d.indexes.z.tree.search('1')[0]).to.equal(doc1);
                 expect(d.indexes.z.tree.search('2')[0]).to.equal(doc2);
                 expect(d.indexes.z.tree.search('3')[0]).to.equal(doc3);
 
                expect(d.indexes.a._getNumberOfKeys()).to.equal(3);
                 expect(d.indexes.a.tree.search(2)[0]).to.equal(doc1);
                 expect(d.indexes.a.tree.search('world')[0]).to.equal(doc2);
                 expect(d.indexes.a.tree.search({ today: now })[0]).to.equal(doc3);

                done();
              });
            });
          });

        });
      });

      it('If a unique constraint is not respected, database loading will not work and no data will be inserted', function (done) {
        var now = new Date()
          , rawData = serialize({ _id: "aaa", z: "1", a: 2, ages: [1, 5, 12] }) + '\n' +
                      serialize({ _id: "bbb", z: "2", a: 'world' }) + '\n' +
                      serialize({ _id: "ccc", z: "1", a: { today: now } })
          ;
 
        expect(d.getAllData().length).to.equal(0);

        d.ensureIndex({ fieldName: 'z', unique: true });
         expect(d.indexes.z._getNumberOfKeys()).to.equal(0);

        fs.writeFile(testDb, rawData, 'utf8', function () {
          d.loadDatabase(function (err) {
             expect(err.errorType).to.equal('uniqueViolated');
             expect(err.key).to.equal("1");
             expect(d.getAllData().length).to.equal(0);
             expect(d.indexes.z._getNumberOfKeys()).to.equal(0);

            done();
          });
        });
      });

      it('If a unique constraint is not respected, ensureIndex will return an error and not create an index', function (done) {
        d.insert({ a: 1, b: 4 }, function () {
          d.insert({ a: 2, b: 45 }, function () {
            d.insert({ a: 1, b: 3 }, function () {
              d.ensureIndex({ fieldName: 'b' }, function (err) {
                assert.isNull(err);

                d.ensureIndex({ fieldName: 'a', unique: true }, function (err) {
                   expect(err.errorType).to.equal('uniqueViolated');
                  assert.deepEqual(Object.keys(d.indexes), ['_id', 'b']);

                  done();
                });
              });
            });
          });
        });
      });
      
      it('Can remove an index', function (done) {
        d.ensureIndex({ fieldName: 'e' }, function (err) {
          assert.isNull(err);
           
          expect(Object.keys(d.indexes).length).to.equal(2);
          assert.isNotNull(d.indexes.e);
          
          d.removeIndex("e", function (err) {
            assert.isNull(err);
             expect(Object.keys(d.indexes).length).to.equal(1);
            assert.isUndefined(d.indexes.e); 
 
            done();
          });
        });
      });

    });   // ==== End of 'ensureIndex and index initialization in database loading' ==== //

    
    describe('Indexing newly inserted documents', function () {

      it('Newly inserted documents are indexed', function (done) {
        d.ensureIndex({ fieldName: 'z' });
         expect(d.indexes.z._getNumberOfKeys()).to.equal(0);

        d.insert({ a: 2, z: 'yes' }, function (err, newDoc) {
           expect(d.indexes.z._getNumberOfKeys()).to.equal(1);
          assert.deepEqual(d.indexes.z.getMatching('yes'), [newDoc]);

          d.insert({ a: 5, z: 'nope' }, function (err, newDoc) {
             expect(d.indexes.z._getNumberOfKeys()).to.equal(2);
            assert.deepEqual(d.indexes.z.getMatching('nope'), [newDoc]);

            done();
          });
        });
      });

      it('If multiple indexes are defined, the document is inserted in all of them', function (done) {
        d.ensureIndex({ fieldName: 'z' });
        d.ensureIndex({ fieldName: 'ya' });
         expect(d.indexes.z._getNumberOfKeys()).to.equal(0);

        d.insert({ a: 2, z: 'yes', ya: 'indeed' }, function (err, newDoc) {
           expect(d.indexes.z._getNumberOfKeys()).to.equal(1);
           expect(d.indexes.ya._getNumberOfKeys()).to.equal(1);
          assert.deepEqual(d.indexes.z.getMatching('yes'), [newDoc]);
          assert.deepEqual(d.indexes.ya.getMatching('indeed'), [newDoc]);

          d.insert({ a: 5, z: 'nope', ya: 'sure' }, function (err, newDoc2) {
             expect(d.indexes.z._getNumberOfKeys()).to.equal(2);
             expect(d.indexes.ya._getNumberOfKeys()).to.equal(2);
            assert.deepEqual(d.indexes.z.getMatching('nope'), [newDoc2]);
            assert.deepEqual(d.indexes.ya.getMatching('sure'), [newDoc2]);

            done();
          });
        });
      });

      it('Can insert two docs at the same key for a non unique index', function (done) {
        d.ensureIndex({ fieldName: 'z' });
         expect(d.indexes.z._getNumberOfKeys()).to.equal(0);

        d.insert({ a: 2, z: 'yes' }, function (err, newDoc) {
           expect(d.indexes.z._getNumberOfKeys()).to.equal(1);
          assert.deepEqual(d.indexes.z.getMatching('yes'), [newDoc]);

          d.insert({ a: 5, z: 'yes' }, function (err, newDoc2) {
             expect(d.indexes.z._getNumberOfKeys()).to.equal(1);
            assert.deepEqual(d.indexes.z.getMatching('yes'), [newDoc, newDoc2]);

            done();
          });
        });
      });

      it('If the index has a unique constraint, an error is thrown if it is violated and the data is not modified', function (done) {
        d.ensureIndex({ fieldName: 'z', unique: true });
         expect(d.indexes.z._getNumberOfKeys()).to.equal(0);

        d.insert({ a: 2, z: 'yes' }, function (err, newDoc) {
           expect(d.indexes.z._getNumberOfKeys()).to.equal(1);
          assert.deepEqual(d.indexes.z.getMatching('yes'), [newDoc]);

          d.insert({ a: 5, z: 'yes' }, function (err) {
             expect(err.errorType).to.equal('uniqueViolated');
             expect(err.key).to.equal('yes');

            // Index didn't change
             expect(d.indexes.z._getNumberOfKeys()).to.equal(1);
            assert.deepEqual(d.indexes.z.getMatching('yes'), [newDoc]);

            // Data didn't change
            assert.deepEqual(d.getAllData(), [newDoc]);
            d.loadDatabase(function () {
               expect(d.getAllData().length).to.equal(1);
              assert.deepEqual(d.getAllData()[0], newDoc);

              done();
            });
          });
        });
      });

      it('If an index has a unique constraint, other indexes cannot be modified when it raises an error', function (done) {
        d.ensureIndex({ fieldName: 'nonu1' });
        d.ensureIndex({ fieldName: 'uni', unique: true });
        d.ensureIndex({ fieldName: 'nonu2' });

        d.insert({ nonu1: 'yes', nonu2: 'yes2', uni: 'willfail' }, function (err, newDoc) {
          assert.isNull(err);
           expect(d.indexes.nonu1._getNumberOfKeys()).to.equal(1);
           expect(d.indexes.uni._getNumberOfKeys()).to.equal(1);
           expect(d.indexes.nonu2._getNumberOfKeys()).to.equal(1);

          d.insert({ nonu1: 'no', nonu2: 'no2', uni: 'willfail' }, function (err) {
             expect(err.errorType).to.equal('uniqueViolated');

            // No index was modified
             expect(d.indexes.nonu1._getNumberOfKeys()).to.equal(1);
             expect(d.indexes.uni._getNumberOfKeys()).to.equal(1);
             expect(d.indexes.nonu2._getNumberOfKeys()).to.equal(1);

            assert.deepEqual(d.indexes.nonu1.getMatching('yes'), [newDoc]);
            assert.deepEqual(d.indexes.uni.getMatching('willfail'), [newDoc]);
            assert.deepEqual(d.indexes.nonu2.getMatching('yes2'), [newDoc]);

            done();
          });
        });
      });

      it('Unique indexes prevent you from inserting two docs where the field is undefined except if theyre sparse', function (done) {
        d.ensureIndex({ fieldName: 'zzz', unique: true });
         expect(d.indexes.zzz._getNumberOfKeys()).to.equal(0);

        d.insert({ a: 2, z: 'yes' }, function (err, newDoc) {
           expect(d.indexes.zzz._getNumberOfKeys()).to.equal(1);
          assert.deepEqual(d.indexes.zzz.getMatching(undefined), [newDoc]);

          d.insert({ a: 5, z: 'other' }, function (err) {
             expect(err.errorType).to.equal('uniqueViolated');
            assert.isUndefined(err.key);

            d.ensureIndex({ fieldName: 'yyy', unique: true, sparse: true });

            d.insert({ a: 5, z: 'other', zzz: 'set' }, function (err) {
              assert.isNull(err);
               expect(d.indexes.yyy.getAll().length).to.equal(0);   // Nothing indexed
               expect(d.indexes.zzz.getAll().length).to.equal(2);

              done();
            });
          });
        });
      });

      it('Insertion still works as before with indexing', function (done) {
        d.ensureIndex({ fieldName: 'a' });
        d.ensureIndex({ fieldName: 'b' });

        d.insert({ a: 1, b: 'hello' }, function (err, doc1) {
          d.insert({ a: 2, b: 'si' }, function (err, doc2) {
            d.find({}, function (err, docs) {
              assert.deepEqual(doc1, docs.find(d => d._id === doc1._id));
              assert.deepEqual(doc2, docs.find(d => d._id === doc2._id));

              done();
            });
          });
        });
      });

      it('All indexes point to the same data as the main index on _id', function (done) {
        d.ensureIndex({ fieldName: 'a' });

        d.insert({ a: 1, b: 'hello' }, function (err, doc1) {
          d.insert({ a: 2, b: 'si' }, function (err, doc2) {
            d.find({}, function (err, docs) {
               expect(docs.length).to.equal(2);
               expect(d.getAllData().length).to.equal(2);
 
              expect(d.indexes._id.getMatching(doc1._id).length).to.equal(1);
               expect(d.indexes.a.getMatching(1).length).to.equal(1);
               expect(d.indexes._id.getMatching(doc1._id)[0]).to.equal(d.indexes.a.getMatching(1)[0]);
 
              expect(d.indexes._id.getMatching(doc2._id).length).to.equal(1);
               expect(d.indexes.a.getMatching(2).length).to.equal(1);
               expect(d.indexes._id.getMatching(doc2._id)[0]).to.equal(d.indexes.a.getMatching(2)[0]);

              done();
            });
          });
        });
      });

      it('If a unique constraint is violated, no index is changed, including the main one', function (done) {
        d.ensureIndex({ fieldName: 'a', unique: true });

        d.insert({ a: 1, b: 'hello' }, function (err, doc1) {
          d.insert({ a: 1, b: 'si' }, function (err) {
            assert.isDefined(err);

            d.find({}, function (err, docs) {
               expect(docs.length).to.equal(1);
               expect(d.getAllData().length).to.equal(1);
 
              expect(d.indexes._id.getMatching(doc1._id).length).to.equal(1);
               expect(d.indexes.a.getMatching(1).length).to.equal(1);
               expect(d.indexes._id.getMatching(doc1._id)[0]).to.equal(d.indexes.a.getMatching(1)[0]);
 
              expect(d.indexes.a.getMatching(2).length).to.equal(0);

              done();
            });
          });
        });
      });

    });   // ==== End of 'Indexing newly inserted documents' ==== //

    describe('Updating indexes upon document update', function () {

      it('Updating docs still works as before with indexing', function (done) {
        d.ensureIndex({ fieldName: 'a' });

        d.insert({ a: 1, b: 'hello' }, function (err, _doc1) {
          d.insert({ a: 2, b: 'si' }, function (err, _doc2) {
            d.update({ a: 1 }, { $set: { a: 456, b: 'no' } }, {}, function (err, nr) {
              var data = d.getAllData()
                , doc1 = data.find(doc => doc._id === _doc1._id)
                , doc2 = data.find(doc => doc._id === _doc2._id)
                ;

              assert.isNull(err);
               expect(nr).to.equal(1);
 
              expect(data.length).to.equal(2);
              assert.deepEqual(doc1, { a: 456, b: 'no', _id: _doc1._id });
              assert.deepEqual(doc2, { a: 2, b: 'si', _id: _doc2._id });

              d.update({}, { $inc: { a: 10 }, $set: { b: 'same' } }, { multi: true }, function (err, nr) {
                var data = d.getAllData()
                  , doc1 = data.find(doc => doc._id === _doc1._id)
                  , doc2 = data.find(doc => doc._id === _doc2._id)
                  ;

                assert.isNull(err);
                 expect(nr).to.equal(2);
 
                expect(data.length).to.equal(2);
                assert.deepEqual(doc1, { a: 466, b: 'same', _id: _doc1._id });
                assert.deepEqual(doc2, { a: 12, b: 'same', _id: _doc2._id });

                done();
              });
            });
          });
        });
      });

      it('Indexes get updated when a document (or multiple documents) is updated', function (done) {
        d.ensureIndex({ fieldName: 'a' });
        d.ensureIndex({ fieldName: 'b' });

        d.insert({ a: 1, b: 'hello' }, function (err, doc1) {
          d.insert({ a: 2, b: 'si' }, function (err, doc2) {
            // Simple update
            d.update({ a: 1 }, { $set: { a: 456, b: 'no' } }, {}, function (err, nr) {
              assert.isNull(err);
               expect(nr).to.equal(1);
 
              expect(d.indexes.a._getNumberOfKeys()).to.equal(2);
               expect(d.indexes.a.getMatching(456)[0]._id).to.equal(doc1._id);
               expect(d.indexes.a.getMatching(2)[0]._id).to.equal(doc2._id);
 
              expect(d.indexes.b._getNumberOfKeys()).to.equal(2);
               expect(d.indexes.b.getMatching('no')[0]._id).to.equal(doc1._id);
               expect(d.indexes.b.getMatching('si')[0]._id).to.equal(doc2._id);

              // The same pointers are shared between all indexes
               expect(d.indexes.a._getNumberOfKeys()).to.equal(2);
               expect(d.indexes.b._getNumberOfKeys()).to.equal(2);
               expect(d.indexes._id._getNumberOfKeys()).to.equal(2);
               expect(d.indexes.a.getMatching(456)[0]).to.equal(d.indexes._id.getMatching(doc1._id)[0]);
               expect(d.indexes.b.getMatching('no')[0]).to.equal(d.indexes._id.getMatching(doc1._id)[0]);
               expect(d.indexes.a.getMatching(2)[0]).to.equal(d.indexes._id.getMatching(doc2._id)[0]);
               expect(d.indexes.b.getMatching('si')[0]).to.equal(d.indexes._id.getMatching(doc2._id)[0]);

              // Multi update
              d.update({}, { $inc: { a: 10 }, $set: { b: 'same' } }, { multi: true }, function (err, nr) {
                assert.isNull(err);
                 expect(nr).to.equal(2);
 
                expect(d.indexes.a._getNumberOfKeys()).to.equal(2);
                 expect(d.indexes.a.getMatching(466)[0]._id).to.equal(doc1._id);
                 expect(d.indexes.a.getMatching(12)[0]._id).to.equal(doc2._id);
 
                expect(d.indexes.b._getNumberOfKeys()).to.equal(1);
                 expect(d.indexes.b.getMatching('same').length).to.equal(2);
                expect(d.indexes.b.getMatching('same').map(o => o._id)).to.contain(doc1._id);
                expect(d.indexes.b.getMatching('same').map(o => o._id)).to.contain(doc2._id);

                // The same pointers are shared between all indexes
                 expect(d.indexes.a._getNumberOfKeys()).to.equal(2);
                 expect(d.indexes.b._getNumberOfKeys()).to.equal(1);
                 expect(d.indexes.b.getAll().length).to.equal(2);
                 expect(d.indexes._id._getNumberOfKeys()).to.equal(2);
                 expect(d.indexes.a.getMatching(466)[0]).to.equal(d.indexes._id.getMatching(doc1._id)[0]);
                 expect(d.indexes.a.getMatching(12)[0]).to.equal(d.indexes._id.getMatching(doc2._id)[0]);
                // Can't test the pointers in b as their order is randomized, but it is the same as with a

                done();
              });
            });
          });
        });
      });

      it('If a simple update violates a contraint, all changes are rolled back and an error is thrown', function (done) {
        d.ensureIndex({ fieldName: 'a', unique: true });
        d.ensureIndex({ fieldName: 'b', unique: true });
        d.ensureIndex({ fieldName: 'c', unique: true });

        d.insert({ a: 1, b: 10, c: 100 }, function (err, _doc1) {
          d.insert({ a: 2, b: 20, c: 200 }, function (err, _doc2) {
            d.insert({ a: 3, b: 30, c: 300 }, function (err, _doc3) {
              // Will conflict with doc3
              d.update({ a: 2 }, { $inc: { a: 10, c: 1000 }, $set: { b: 30 } }, {}, function (err) {
                var data = d.getAllData()
                  , doc1 = data.find(doc => doc._id === _doc1._id)
                  , doc2 = data.find(doc => doc._id === _doc2._id)
                  , doc3 = data.find(doc => doc._id === _doc3._id)
                  ;
 
                expect(err.errorType).to.equal('uniqueViolated');

                // Data left unchanged
                 expect(data.length).to.equal(3);
                assert.deepEqual(doc1, { a: 1, b: 10, c: 100, _id: _doc1._id });
                assert.deepEqual(doc2, { a: 2, b: 20, c: 200, _id: _doc2._id });
                assert.deepEqual(doc3, { a: 3, b: 30, c: 300, _id: _doc3._id });

                // All indexes left unchanged and pointing to the same docs
                 expect(d.indexes.a._getNumberOfKeys()).to.equal(3);
                 expect(d.indexes.a.getMatching(1)[0]).to.equal(doc1);
                 expect(d.indexes.a.getMatching(2)[0]).to.equal(doc2);
                 expect(d.indexes.a.getMatching(3)[0]).to.equal(doc3);
 
                expect(d.indexes.b._getNumberOfKeys()).to.equal(3);
                 expect(d.indexes.b.getMatching(10)[0]).to.equal(doc1);
                 expect(d.indexes.b.getMatching(20)[0]).to.equal(doc2);
                 expect(d.indexes.b.getMatching(30)[0]).to.equal(doc3);
 
                expect(d.indexes.c._getNumberOfKeys()).to.equal(3);
                 expect(d.indexes.c.getMatching(100)[0]).to.equal(doc1);
                 expect(d.indexes.c.getMatching(200)[0]).to.equal(doc2);
                 expect(d.indexes.c.getMatching(300)[0]).to.equal(doc3);

                done();
              });
            });
          });
        });
      });

      it('If a multi update violates a contraint, all changes are rolled back and an error is thrown', function (done) {
        d.ensureIndex({ fieldName: 'a', unique: true });
        d.ensureIndex({ fieldName: 'b', unique: true });
        d.ensureIndex({ fieldName: 'c', unique: true });

        d.insert({ a: 1, b: 10, c: 100 }, function (err, _doc1) {
          d.insert({ a: 2, b: 20, c: 200 }, function (err, _doc2) {
            d.insert({ a: 3, b: 30, c: 300 }, function (err, _doc3) {
              // Will conflict with doc3
              d.update({ a: { $in: [1, 2] } }, { $inc: { a: 10, c: 1000 }, $set: { b: 30 } }, { multi: true }, function (err) {
                var data = d.getAllData()
                  , doc1 = data.find(doc => doc._id === _doc1._id)
                  , doc2 = data.find(doc => doc._id === _doc2._id)
                  , doc3 = data.find(doc => doc._id === _doc3._id)
                  ;
 
                expect(err.errorType).to.equal('uniqueViolated');

                // Data left unchanged
                 expect(data.length).to.equal(3);
                assert.deepEqual(doc1, { a: 1, b: 10, c: 100, _id: _doc1._id });
                assert.deepEqual(doc2, { a: 2, b: 20, c: 200, _id: _doc2._id });
                assert.deepEqual(doc3, { a: 3, b: 30, c: 300, _id: _doc3._id });

                // All indexes left unchanged and pointing to the same docs
                 expect(d.indexes.a._getNumberOfKeys()).to.equal(3);
                 expect(d.indexes.a.getMatching(1)[0]).to.equal(doc1);
                 expect(d.indexes.a.getMatching(2)[0]).to.equal(doc2);
                 expect(d.indexes.a.getMatching(3)[0]).to.equal(doc3);
 
                expect(d.indexes.b._getNumberOfKeys()).to.equal(3);
                 expect(d.indexes.b.getMatching(10)[0]).to.equal(doc1);
                 expect(d.indexes.b.getMatching(20)[0]).to.equal(doc2);
                 expect(d.indexes.b.getMatching(30)[0]).to.equal(doc3);
 
                expect(d.indexes.c._getNumberOfKeys()).to.equal(3);
                 expect(d.indexes.c.getMatching(100)[0]).to.equal(doc1);
                 expect(d.indexes.c.getMatching(200)[0]).to.equal(doc2);
                 expect(d.indexes.c.getMatching(300)[0]).to.equal(doc3);

                done();
              });
            });
          });
        });
      });

    });   // ==== End of 'Updating indexes upon document update' ==== //

    describe('Updating indexes upon document remove', function () {

      it('Removing docs still works as before with indexing', function (done) {
        d.ensureIndex({ fieldName: 'a' });

        d.insert({ a: 1, b: 'hello' }, function (err, _doc1) {
          d.insert({ a: 2, b: 'si' }, function (err, _doc2) {
            d.insert({ a: 3, b: 'coin' }, function (err, _doc3) {
              d.remove({ a: 1 }, {}, function (err, nr) {
                var data = d.getAllData()
                , doc2 = data.find(doc => doc._id === _doc2._id)
                , doc3 = data.find(doc => doc._id === _doc3._id)
                ;

                assert.isNull(err);
                 expect(nr).to.equal(1);
 
                expect(data.length).to.equal(2);
                assert.deepEqual(doc2, { a: 2, b: 'si', _id: _doc2._id });
                assert.deepEqual(doc3, { a: 3, b: 'coin', _id: _doc3._id });

                d.remove({ a: { $in: [2, 3] } }, { multi: true }, function (err, nr) {
                  var data = d.getAllData()
                  ;

                  assert.isNull(err);
                   expect(nr).to.equal(2);
                   expect(data.length).to.equal(0);

                  done();
                });
              });
            });
          });
        });
      });

      it('Indexes get updated when a document (or multiple documents) is removed', function (done) {
        d.ensureIndex({ fieldName: 'a' });
        d.ensureIndex({ fieldName: 'b' });

        d.insert({ a: 1, b: 'hello' }, function (err, doc1) {
          d.insert({ a: 2, b: 'si' }, function (err, doc2) {
            d.insert({ a: 3, b: 'coin' }, function (err, doc3) {
              // Simple remove
              d.remove({ a: 1 }, {}, function (err, nr) {
                assert.isNull(err);
                 expect(nr).to.equal(1);
 
                expect(d.indexes.a._getNumberOfKeys()).to.equal(2);
                 expect(d.indexes.a.getMatching(2)[0]._id).to.equal(doc2._id);
                 expect(d.indexes.a.getMatching(3)[0]._id).to.equal(doc3._id);
 
                expect(d.indexes.b._getNumberOfKeys()).to.equal(2);
                 expect(d.indexes.b.getMatching('si')[0]._id).to.equal(doc2._id);
                 expect(d.indexes.b.getMatching('coin')[0]._id).to.equal(doc3._id);

                // The same pointers are shared between all indexes
                 expect(d.indexes.a._getNumberOfKeys()).to.equal(2);
                 expect(d.indexes.b._getNumberOfKeys()).to.equal(2);
                 expect(d.indexes._id._getNumberOfKeys()).to.equal(2);
                 expect(d.indexes.a.getMatching(2)[0]).to.equal(d.indexes._id.getMatching(doc2._id)[0]);
                 expect(d.indexes.b.getMatching('si')[0]).to.equal(d.indexes._id.getMatching(doc2._id)[0]);
                 expect(d.indexes.a.getMatching(3)[0]).to.equal(d.indexes._id.getMatching(doc3._id)[0]);
                 expect(d.indexes.b.getMatching('coin')[0]).to.equal(d.indexes._id.getMatching(doc3._id)[0]);

                // Multi remove
                d.remove({}, { multi: true }, function (err, nr) {
                  assert.isNull(err);
                   expect(nr).to.equal(2);
 
                  expect(d.indexes.a._getNumberOfKeys()).to.equal(0);
                   expect(d.indexes.b._getNumberOfKeys()).to.equal(0);
                   expect(d.indexes._id._getNumberOfKeys()).to.equal(0);

                  done();
                });
              });
            });
          });
        });
      });

    });   // ==== End of 'Updating indexes upon document remove' ==== //


    describe('Persisting indexes', function () {

      it('Indexes are persisted to a separate file and recreated upon reload', function (done) {
        var persDb = pp('persistIndexes.db')
          , db
          ;

        if (fs.existsSync(persDb)) { fs.writeFileSync(persDb, '', 'utf8'); }
        db = new Datastore({ filename: persDb, autoload: true });
 
        expect(Object.keys(db.indexes).length).to.equal(1);
         expect(Object.keys(db.indexes)[0]).to.equal("_id");

        db.insert({ planet: "Earth" }, function (err) {
          assert.isNull(err);
          db.insert({ planet: "Mars" }, function (err) {
            assert.isNull(err);

            db.ensureIndex({ fieldName: "planet" }, function (err) {
               expect(Object.keys(db.indexes).length).to.equal(2);
               expect(Object.keys(db.indexes)[0]).to.equal("_id");
               expect(Object.keys(db.indexes)[1]).to.equal("planet");
               expect(db.indexes._id.getAll().length).to.equal(2);
               expect(db.indexes.planet.getAll().length).to.equal(2);
               expect(db.indexes.planet.fieldName).to.equal("planet");

              // After a reload the indexes are recreated
              db = new Datastore({ filename: persDb });
              db.loadDatabase(function (err) {
                assert.isNull(err);
                 expect(Object.keys(db.indexes).length).to.equal(2);
                 expect(Object.keys(db.indexes)[0]).to.equal("_id");
                 expect(Object.keys(db.indexes)[1]).to.equal("planet");
                 expect(db.indexes._id.getAll().length).to.equal(2);
                 expect(db.indexes.planet.getAll().length).to.equal(2);
                 expect(db.indexes.planet.fieldName).to.equal("planet");

                // After another reload the indexes are still there (i.e. they are preserved during autocompaction)
                db = new Datastore({ filename: persDb });
                db.loadDatabase(function (err) {
                  assert.isNull(err);
                   expect(Object.keys(db.indexes).length).to.equal(2);
                   expect(Object.keys(db.indexes)[0]).to.equal("_id");
                   expect(Object.keys(db.indexes)[1]).to.equal("planet");
                   expect(db.indexes._id.getAll().length).to.equal(2);
                   expect(db.indexes.planet.getAll().length).to.equal(2);
                   expect(db.indexes.planet.fieldName).to.equal("planet");

                  done();
                });
              });
            });
          });
        });
      });

      it('Indexes are persisted with their options and recreated even if some db operation happen between loads', function (done) {
        var persDb = pp('persistIndexes.db')
          , db
        ;

        if (fs.existsSync(persDb)) { fs.writeFileSync(persDb, '', 'utf8'); }
        db = new Datastore({ filename: persDb, autoload: true });
 
        expect(Object.keys(db.indexes).length).to.equal(1);
         expect(Object.keys(db.indexes)[0]).to.equal("_id");

        db.insert({ planet: "Earth" }, function (err) {
          assert.isNull(err);
          db.insert({ planet: "Mars" }, function (err) {
            assert.isNull(err);

            db.ensureIndex({ fieldName: "planet", unique: true, sparse: false }, function (err) {
               expect(Object.keys(db.indexes).length).to.equal(2);
               expect(Object.keys(db.indexes)[0]).to.equal("_id");
               expect(Object.keys(db.indexes)[1]).to.equal("planet");
               expect(db.indexes._id.getAll().length).to.equal(2);
               expect(db.indexes.planet.getAll().length).to.equal(2);
               expect(db.indexes.planet.unique).to.equal(true);
               expect(db.indexes.planet.sparse).to.equal(false);

              db.insert({ planet: "Jupiter" }, function (err) {
                assert.isNull(err);

                // After a reload the indexes are recreated
                db = new Datastore({ filename: persDb });
                db.loadDatabase(function (err) {
                  assert.isNull(err);
                   expect(Object.keys(db.indexes).length).to.equal(2);
                   expect(Object.keys(db.indexes)[0]).to.equal("_id");
                   expect(Object.keys(db.indexes)[1]).to.equal("planet");
                   expect(db.indexes._id.getAll().length).to.equal(3);
                   expect(db.indexes.planet.getAll().length).to.equal(3);
                   expect(db.indexes.planet.unique).to.equal(true);
                   expect(db.indexes.planet.sparse).to.equal(false);

                  db.ensureIndex({ fieldName: 'bloup', unique: false, sparse: true }, function (err) {
                    assert.isNull(err);
                     expect(Object.keys(db.indexes).length).to.equal(3);
                     expect(Object.keys(db.indexes)[0]).to.equal("_id");
                     expect(Object.keys(db.indexes)[1]).to.equal("planet");
                     expect(Object.keys(db.indexes)[2]).to.equal("bloup");
                     expect(db.indexes._id.getAll().length).to.equal(3);
                     expect(db.indexes.planet.getAll().length).to.equal(3);
                     expect(db.indexes.bloup.getAll().length).to.equal(0);
                     expect(db.indexes.planet.unique).to.equal(true);
                     expect(db.indexes.planet.sparse).to.equal(false);
                     expect(db.indexes.bloup.unique).to.equal(false);
                     expect(db.indexes.bloup.sparse).to.equal(true);

                    // After another reload the indexes are still there (i.e. they are preserved during autocompaction)
                    db = new Datastore({ filename: persDb });
                    db.loadDatabase(function (err) {
                      assert.isNull(err);
                       expect(Object.keys(db.indexes).length).to.equal(3);
                       expect(Object.keys(db.indexes)[0]).to.equal("_id");
                       expect(Object.keys(db.indexes)[1]).to.equal("planet");
                       expect(Object.keys(db.indexes)[2]).to.equal("bloup");
                       expect(db.indexes._id.getAll().length).to.equal(3);
                       expect(db.indexes.planet.getAll().length).to.equal(3);
                       expect(db.indexes.bloup.getAll().length).to.equal(0);
                       expect(db.indexes.planet.unique).to.equal(true);
                       expect(db.indexes.planet.sparse).to.equal(false);
                       expect(db.indexes.bloup.unique).to.equal(false);
                       expect(db.indexes.bloup.sparse).to.equal(true);

                      done();
                    });
                  });
                });
              });
            });
          });
        });
      });

      it('Indexes can also be removed and the remove persisted', function (done) {
        var persDb = pp('persistIndexes.db')
          , db
        ;

        if (fs.existsSync(persDb)) { fs.writeFileSync(persDb, '', 'utf8'); }
        db = new Datastore({ filename: persDb, autoload: true });
 
        expect(Object.keys(db.indexes).length).to.equal(1);
         expect(Object.keys(db.indexes)[0]).to.equal("_id");

        db.insert({ planet: "Earth" }, function (err) {
          assert.isNull(err);
          db.insert({ planet: "Mars" }, function (err) {
            assert.isNull(err);

            db.ensureIndex({ fieldName: "planet" }, function (err) {
              assert.isNull(err);
              db.ensureIndex({ fieldName: "another" }, function (err) {
                assert.isNull(err);
                 expect(Object.keys(db.indexes).length).to.equal(3);
                 expect(Object.keys(db.indexes)[0]).to.equal("_id");
                 expect(Object.keys(db.indexes)[1]).to.equal("planet");
                 expect(Object.keys(db.indexes)[2]).to.equal("another");
                 expect(db.indexes._id.getAll().length).to.equal(2);
                 expect(db.indexes.planet.getAll().length).to.equal(2);
                 expect(db.indexes.planet.fieldName).to.equal("planet");

                // After a reload the indexes are recreated
                db = new Datastore({ filename: persDb });
                db.loadDatabase(function (err) {
                  assert.isNull(err);
                   expect(Object.keys(db.indexes).length).to.equal(3);
                   expect(Object.keys(db.indexes)[0]).to.equal("_id");
                   expect(Object.keys(db.indexes)[1]).to.equal("planet");  
                   expect(Object.keys(db.indexes)[2]).to.equal("another");
                   expect(db.indexes._id.getAll().length).to.equal(2);
                   expect(db.indexes.planet.getAll().length).to.equal(2);
                   expect(db.indexes.planet.fieldName).to.equal("planet");

                  // Index is removed
                  db.removeIndex("planet", function (err) {
                    assert.isNull(err);
                     expect(Object.keys(db.indexes).length).to.equal(2);
                     expect(Object.keys(db.indexes)[0]).to.equal("_id");
                     expect(Object.keys(db.indexes)[1]).to.equal("another");
                     expect(db.indexes._id.getAll().length).to.equal(2);

                    // After a reload indexes are preserved
                    db = new Datastore({ filename: persDb });
                    db.loadDatabase(function (err) {
                      assert.isNull(err);
                       expect(Object.keys(db.indexes).length).to.equal(2);
                       expect(Object.keys(db.indexes)[0]).to.equal("_id");
                       expect(Object.keys(db.indexes)[1]).to.equal("another");
                       expect(db.indexes._id.getAll().length).to.equal(2);

                      // After another reload the indexes are still there (i.e. they are preserved during autocompaction)
                      db = new Datastore({ filename: persDb });
                      db.loadDatabase(function (err) {
                        assert.isNull(err);
                         expect(Object.keys(db.indexes).length).to.equal(2);
                         expect(Object.keys(db.indexes)[0]).to.equal("_id");
                         expect(Object.keys(db.indexes)[1]).to.equal("another");
                         expect(db.indexes._id.getAll().length).to.equal(2);

                        done();
                      });
                    });
                  });
                });
              });
            });
          });
        });
      });

    });   // ==== End of 'Persisting indexes' ====

    it('Results of getMatching should never contain duplicates', function (done) {
      d.ensureIndex({ fieldName: 'bad' });
      d.insert({ bad: ['a', 'b'] }, function () {
        d.getCandidates({ bad: { $in: ['a', 'b'] } }, function (err, res) {
           expect(res.length).to.equal(1);
          done();
        });
      });
    });

  });   // ==== End of 'Using indexes' ==== //


});
