/*jshint expr:true */
'use strict';

var pouch = require('pouchdb');
var Mapreduce = require('../');
pouch.plugin('mapreduce', Mapreduce);
var chai = require('chai');
var should = chai.should();
require("mocha-as-promised")();
chai.use(require("chai-as-promised"));
var denodify = require('lie-denodify');
var all = require("lie-all");
describe('local', function () {
  if (process.argv.length) {
    process.argv.slice(4).forEach(tests);
  } else {
    tests('testDB');
  }
});
var pouchPromise = denodify(pouch);
function tests(dbName) {
  beforeEach(function (done) {
    pouch(dbName, function (err, d) {
      done();
    });
  });
  afterEach(function (done) {
    pouch.destroy(dbName, function () {
      done();
    });
  });
  describe('views', function () {

    it('Testing query with keys', function () {
      return pouchPromise(dbName).then(function (db) {
        var bulk = denodify(db.bulkDocs);
        var opts = {include_docs: true};
        return bulk({
          docs: [
            {_id: 'doc_0', field: 0},
            {_id: 'doc_1', field: 1},
            {_id: 'doc_2', field: 2},
            {_id: 'doc_empty', field: ''},
            {_id: 'doc_null', field: null},
            {_id: 'doc_undefined' /* field undefined */},
            {_id: 'doc_foo', field: 'foo'},
            {
              _id: "_design/test",
              views: {
                mapFunc: {
                  map: "function (doc) {emit(doc.field);}"
                }
              }
            }
          ]
        }).then(function () {
          return db.query("test/mapFunc", opts);
        }).then(function (data) {
          data.rows.should.have.length(7, 'returns all docs');
          opts.keys = [];
          return db.query("test/mapFunc", opts);
        }).then(function (data) {
          data.rows.should.have.length(0, 'returns 0 docs');

          opts.keys = [0];
          return db.query("test/mapFunc", opts);
        }).then(function (data) {
          data.rows.should.have.length(1, 'returns one doc');
          data.rows[0].doc._id.should.equal('doc_0');

          opts.keys = [2, 'foo', 1, 0, null, ''];
          return db.query("test/mapFunc", opts);
        }).then(function (data) {
          // check that the returned ordering fits opts.keys
          data.rows.should.have.length(7, 'returns 7 docs in correct order');
          data.rows[0].doc._id.should.equal('doc_2');
          data.rows[1].doc._id.should.equal('doc_foo');
          data.rows[2].doc._id.should.equal('doc_1');
          data.rows[3].doc._id.should.equal('doc_0');
          data.rows[4].doc._id.should.equal('doc_null');
          data.rows[5].doc._id.should.equal('doc_undefined');
          data.rows[6].doc._id.should.equal('doc_empty');

          opts.keys = [3, 1, 4, 2];
          return db.query("test/mapFunc", opts);
        }).then(function (data) {
          // nonexistent keys just give us holes in the list
          data.rows.should.have.length(2, 'returns 2 non-empty docs');
          data.rows[0].key.should.equal(1);
          data.rows[0].doc._id.should.equal('doc_1');
          data.rows[1].key.should.equal(2);
          data.rows[1].doc._id.should.equal('doc_2');

          opts.keys = [2, 1, 2, 0, 2, 1];
          return db.query("test/mapFunc", opts);
        }).then(function (data) {
          // with duplicates, we return multiple docs
          data.rows.should.have.length(6, 'returns 6 docs with duplicates');
          data.rows[0].doc._id.should.equal('doc_2');
          data.rows[1].doc._id.should.equal('doc_1');
          data.rows[2].doc._id.should.equal('doc_2');
          data.rows[3].doc._id.should.equal('doc_0');
          data.rows[4].doc._id.should.equal('doc_2');
          data.rows[5].doc._id.should.equal('doc_1');

          opts.keys = [2, 1, 2, 3, 2];
          return db.query("test/mapFunc", opts);
        }).then(function (data) {
          // duplicates and unknowns at the same time, for maximum crazy
          data.rows.should.have.length(4, 'returns 2 docs with duplicates/unknowns');
          data.rows[0].doc._id.should.equal('doc_2');
          data.rows[1].doc._id.should.equal('doc_1');
          data.rows[2].doc._id.should.equal('doc_2');
          data.rows[3].doc._id.should.equal('doc_2');

          opts.keys = [3];
          return db.query("test/mapFunc", opts);
        }).then(function (data) {
          data.rows.should.have.length(0, 'returns 0 doc due to unknown key');

          opts.include_docs = false;
          opts.keys = [3, 2];
          return db.query("test/mapFunc", opts);
        }).then(function (data) {
          data.rows.should.have.length(1, 'returns 1 doc due to unknown key');
          data.rows[0].id.should.equal('doc_2');
          should.not.exist(data.rows[0].doc, 'no doc, since include_docs=false');
        });
        // */
      });
    });
      return;

    it('Testing query with multiple keys, multiple docs', function () {
      var mapFunction = function (doc) {
        emit(doc.field1);
        emit(doc.field2);
      };
      function ids(row) {
        return row.id;
      }
      var opts = {keys: [0, 1, 2]};
      var spec;
      return pouchPromise(dbName).then(function (db) {
        var bulk = denodify(db.bulkDocs);
        return bulk({
          docs: [
            {_id: '0', field1: 0},
            {_id: '1a', field1: 1},
            {_id: '1b', field1: 1},
            {_id: '1c', field1: 1},
            {_id: '2+3', field1: 2, field2: 3},
            {_id: '4+5', field1: 4, field2: 5},
            {_id: '3+5', field1: 3, field2: 5},
            {_id: '3+4', field1: 3, field2: 4}
          ]
        }).then(function () {
          spec = ['0', '1a', '1b', '1c', '2+3'];
          return db.query(mapFunction, opts);
        }).then(function (data) {
          data.rows.map(ids).should.deep.equal(spec);

          opts.keys = [3, 5, 4, 3];
          spec = ['2+3', '3+4', '3+5', '3+5', '4+5', '3+4', '4+5', '2+3', '3+4', '3+5'];
          return db.query(mapFunction, opts);
        }).then(function (data) {
          data.rows.map(ids).should.deep.equal(spec);
        });
      });
    });
    it('Testing multiple emissions (issue #14)', function () {
      return pouchPromise(dbName).then(function (db) {
        var bulk = denodify(db.bulkDocs);
        return bulk({
          docs: [
            {_id: 'doc1', foo : 'foo', bar : 'bar'},
            {_id: 'doc2', foo : 'foo', bar : 'bar'}
          ]
        }).then(function () {
          var mapFunction = function (doc) {
            emit(doc.foo);
            emit(doc.foo);
            emit(doc.bar);
            emit(doc.bar, 'multiple values!');
            emit(doc.bar, 'crazy!');
          };
          var opts = {keys: ['foo', 'bar']};

          return db.query(mapFunction, opts);
        });
      }).then(function (data) {
        data.rows.should.have.length(10);

        data.rows[0].key.should.equal('foo');
        data.rows[0].id.should.equal('doc1');
        data.rows[1].key.should.equal('foo');
        data.rows[1].id.should.equal('doc1');

        data.rows[2].key.should.equal('foo');
        data.rows[2].id.should.equal('doc2');
        data.rows[3].key.should.equal('foo');
        data.rows[3].id.should.equal('doc2');

        data.rows[4].key.should.equal('bar');
        data.rows[4].id.should.equal('doc1');
        should.not.exist(data.rows[4].value);
        data.rows[5].key.should.equal('bar');
        data.rows[5].id.should.equal('doc1');
        data.rows[5].value.should.equal('crazy!');
        data.rows[6].key.should.equal('bar');
        data.rows[6].id.should.equal('doc1');
        data.rows[6].value.should.equal('multiple values!');

        data.rows[7].key.should.equal('bar');
        data.rows[7].id.should.equal('doc2');
        should.not.exist(data.rows[7].value);
        data.rows[8].key.should.equal('bar');
        data.rows[8].id.should.equal('doc2');
        data.rows[8].value.should.equal('crazy!');
        data.rows[9].key.should.equal('bar');
        data.rows[9].id.should.equal('doc2');
        data.rows[9].value.should.equal('multiple values!');
      });
    });
    it('Testing empty startkeys and endkeys', function (done) {
      var mapFunction = function (doc) {
        emit(doc.field);
      };
      var opts = {startkey: null, endkey: ''};
      function ids(row) {
        return row.id;
      }
      var spec;
      return pouchPromise(dbName).then(function (db) {
        var bulk = denodify(db.bulkDocs);
        return bulk({
          docs: [
            {_id: 'doc_empty', field: ''},
            {_id: 'doc_null', field: null},
            {_id: 'doc_undefined' /* field undefined */},
            {_id: 'doc_foo', field: 'foo'}
          ]
        }).then(function () {
          spec = ['doc_null', 'doc_undefined', 'doc_empty'];
          return db.query(mapFunction, opts);
        }).then(function (data) {
          data.rows.map(ids).should.deep.equal(spec);

          opts = {startkey: '', endkey: 'foo'};
          spec = ['doc_empty', 'doc_foo'];
          return db.query(mapFunction, opts);
        }).then(function (data) {
          data.rows.map(ids).should.deep.equal(spec);

          opts = {startkey: null, endkey: null};
          spec = ['doc_null', 'doc_undefined'];
          return db.query(mapFunction, opts);
        }).then(function (data) {
          data.rows.map(ids).should.deep.equal(spec);

          opts.descending = true;
          spec.reverse();
          return db.query(mapFunction, opts);
        }).then(function (data) {
          data.rows.map(ids).should.deep.equal(spec);
        });
      });
    });
    it('Testing ordering with startkey/endkey/key', function (done) {
      var mapFunction = function (doc) {
        emit(doc.field, null);
      };
      var opts = {startkey: '1', endkey: '4'};
      function ids(row) {
        return row.id;
      }
      var spec;
      return pouchPromise(dbName).then(function (db) {
        var bulk = denodify(db.bulkDocs);
        return bulk({
          docs: [
            {_id: 'h', field: '4'},
            {_id: 'a', field: '1'},
            {_id: 'e', field: '2'},
            {_id: 'c', field: '1'},
            {_id: 'f', field: '3'},
            {_id: 'g', field: '4'},
            {_id: 'd', field: '2'},
            {_id: 'b', field: '1'}
          ]
        }).then(function () {
          spec = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'];
          return db.query(mapFunction, opts);
        }).then(function (data) {
          data.rows.map(ids).should.deep.equal(spec);

          opts = {key: '1'};
          spec = ['a', 'b', 'c'];
          return db.query(mapFunction, opts);
        }).then(function (data) {
          data.rows.map(ids).should.deep.equal(spec);

          opts = {key: '2'};
          spec = ['d', 'e'];
          return db.query(mapFunction, opts);
        }).then(function (data) {
          data.rows.map(ids).should.deep.equal(spec);

          opts.descending = true;
          spec.reverse();
          return db.query(mapFunction, opts);
        }).then(function (data) {
          data.rows.map(ids).should.deep.equal(spec);
        });
      });
    });
    it('Testing ordering with dates', function (done) {
      function ids(row) {
        return row.id;
      }
      return pouchPromise(dbName).then(function (db) {
        var bulk = denodify(db.bulkDocs);
        return bulk({
          docs: [
            {_id: '1969', date: '1969 was when Space Oddity hit'},
            {_id: '1971', date : new Date('1971-12-17T00:00:00.000Z')}, // Hunky Dory was released
            {_id: '1972', date: '1972 was when Ziggy landed on Earth'},
            {_id: '1977', date: new Date('1977-01-14T00:00:00.000Z')}, // Low was released
            {_id: '1985', date: '1985+ is better left unmentioned'}
          ]
        }).then(function () {
          var mapFunction = function (doc) {
            emit(doc.date, null);
          };
          return db.query(mapFunction);
        }).then(function (data) {
          data.rows.map(ids).should.deep.equal(['1969', '1971', '1972', '1977', '1985']);
        });
      });
    });
    it('should error with a callback', function (done) {
      pouch(dbName, function (err, db) {
        db.query('fake/thing', function (err) {
          var a = err.should.exist;
          done();
        });
      });
    });
    it('should work with a joined doc', function () {
      function change(row) {
        return [row.key, row.doc._id, row.doc.val];
      }
      return pouchPromise(dbName).then(function (db) {
        var bulk = denodify(db.bulkDocs);
        return bulk({
          docs: [
            {_id: 'a', join: 'b', color: 'green'},
            {_id: 'b', val: 'c'},
            {_id: 'd', join: 'f', color: 'red'},
            {
              _id: "_design/test",
              views: {
                mapFunc: {
                  map: "function (doc) {if(doc.join){emit(doc.color, {_id:doc.join});}}"
                }
              }
            }
          ]
        }).then(function () {
          return db.query('test/mapFunc', {include_docs: true});
        }).then(function (resp) {
          return change(resp.rows[0]).should.deep.equal(['green', 'b', 'c']);
        });
      });
    });
  });
}
