'use strict';

var PouchDB = require('pouchdb');
var pouchCollate = require('pouchdb-collate');
var Promise = require('lie');
var collate = pouchCollate.collate;
var normalizeKey = pouchCollate.normalizeKey;
var httpQuery = require('./httpQuery.js');
var promise = require('lie');
var all = require('lie-all');
var extend = PouchDB.extend;

function MapReduceError(name, msg, code) {
  this.name = name;
  this.message = msg;
  this.status =  code;
}
MapReduceError.prototype = new Error();

function sum(values) {
  return values.reduce(function (a, b) {
    return a + b;
  }, 0);
}

var builtInReduce = {
  "_sum": function (keys, values) {
    return sum(values);
  },

  "_count": function (keys, values, rereduce) {
    return values.length;
  },

  "_stats": function (keys, values) {
    return {
      'sum': sum(values),
      'min': Math.min.apply(null, values),
      'max': Math.max.apply(null, values),
      'count': values.length,
      'sumsqr': (function () {
        var _sumsqr = 0;
        for (var idx in values) {
          if (typeof values[idx] === 'number') {
            _sumsqr += values[idx] * values[idx];
          } else {
            throw MapReduceError(
              'builtin _stats function requires map values to be numbers',
              'invalid_value',
              500
            );
          }
        }
        return _sumsqr;
      })()
    };
  }
};

function MapReduce(db) {
  if (!(this instanceof MapReduce)) {
    return new MapReduce(db);
  }

  function viewQuery(fun, options) {
    options = extend(true, {}, options);

    /*jshint evil: true */
    if (!options.skip) {
      options.skip = 0;
    }

    if (!fun.reduce) {
      options.reduce = false;
    }

    var results = [];
    var currentDoc;
    var num_started = 0;

    function emit(key, val) {
      var viewRow = {
        id: currentDoc._id,
        key: key,
        value: val,
        doc: extend(true, {}, currentDoc)
      };
      results.push(promise(function (resolve, reject) {
        //in this special case, join on _id (issue #106)
        if (val && typeof val === 'object' && val._id) {
          db.get(val._id, function (_, joined_doc) {
            if (joined_doc) {
              viewRow.doc = joined_doc;
            }
            resolve(viewRow);
          });
        } else {
          resolve(viewRow);
        }
      }));
    }
    // ugly way to make sure references to 'emit' in map/reduce bind to the
    // above emit

    eval('fun.map = ' + fun.map.toString() + ';');
    if (fun.reduce) {
      if (builtInReduce[fun.reduce]) {
        fun.reduce = builtInReduce[fun.reduce];
      } else {
        eval('fun.reduce = ' + fun.reduce.toString() + ';');
      }
    }

    function doReduce(options, res) {
      var groups = [];
      var results = res.rows;
      var error = null;
      results.forEach(function (e) {
        var last = groups[groups.length - 1];
        if (last && collate(last.key[0][0], e.key) === 0) {
          last.key.push([e.key, e.id]);
          last.value.push(e.value);
          return;
        }
        groups.push({key: [
          [e.key, e.id]
          ], value: [e.value]});
      });
      groups.forEach(function (e) {
        e.value = fun.reduce(e.key, e.value);
        e.key = e.key[0][0];
      });
      return {
        total_rows: groups.length,
        offset: options.skip,
        rows: ('limit' in options) ? groups.slice(options.skip, options.limit + options.skip) :
        (options.skip > 0) ? groups.slice(options.skip) : groups
      };
    }

    // DISCUSSION: MD5? whatever?
    options.name = options.name.replace(/\//g, '_');
    if (options.temp) {
      options.name += Math.random();
    }

    // DISCUSSION: keep version here (in name) for possible future use?
    // like updgrade.
    var VIEW_PREFIX = '_pouchdb_views_';
    var VIEW_META_PREFIX = VIEW_PREFIX + 'metadata_';

    var view = new PouchDB(VIEW_PREFIX + options.name);
    var viewMetadata = new PouchDB(VIEW_META_PREFIX + options.name);

    // returns promise which resolves to array of emited (key, value)s
    function doMap(doc) {
      //console.log('xxxxxxxxx', doc)

      // FIXME: clone. Can we get rid of it?
      currentDoc = extend(true, {}, doc);
      //console.log('doMap', currentDoc);
      results = [];
      fun.map.call(this, doc);
      return all(results);
    }

    function getSeq() {
      return viewMetadata.get('seq').then(function (doc) {
        return doc.seq;
      }, function (reason) {
        return -1;
      });
    }
    function setSeq(seq) {
      return viewMetadata.get('seq').then(null, function (err) {
        return {_id: 'seq'};
      }).then(function (doc) {
        doc.seq = seq;
        return viewMetadata.put(doc);
      });
    }

    function updateView (seq) {
      var queue = [];
      function processChange (change) {
        var doc = change.doc;
        var metaDocId = 'emitted_map_' + doc._id;

        var cleanupIndex = viewMetadata.get(metaDocId).then(function (doc){
          // remove those guys from view
          var removePromises = doc.keys.map(function (key) {
            return view.get(key).then(function (doc) {
              return view.remove(doc);
            }, function (reason) {
              console.error('!!!!!!!!!!!!!!!!!!!!!!!!!! no doc');
            });
          });
          var removeMetadata = viewMetadata.remove(doc);
          return all([all(removePromises), removeMetadata]);
        }, function (reason) {
          // console.log('* nothing to cleanup')
          return; // it's ok - nothing to cleanup
        });

        if ('deleted' in change || change.id[0] === "_") {
          queue.push(cleanupIndex);
          return;
        }

        var results = doMap(doc);
        var stuff = all([results, cleanupIndex]).then(function (ok) {
          var results = ok[0];

          var keys = [];
          var rows = results.map(function (row, i) {
            //console.log('emitted', row)

            var viewKey = [row.key, row.id, row.value, i];
            var strViewKey = pouchCollate.toIndexableString(viewKey);
            keys.push(strViewKey);
            return {
              _id: strViewKey,
              id: row.id,
              key: normalizeKey(row.key),
              value: row.value,
              doc: row.doc
            };
          });

          // save those keys
          var saveMetadata = viewMetadata.get(metaDocId).then(null, function () {
            return {_id: metaDocId};
          }).then(function (doc) {
            doc.keys = keys;
            return viewMetadata.put(doc);
          });

          // once metadata is saved we can change the view index
          var processIndex = saveMetadata.then(function () {
            return view.bulkDocs({docs: rows});
          });

          return processIndex;
        });
        queue.push(stuff);
      }


      return promise(function (fulfill, reject) {
        db.changes({
          // TODO: what would happen if we failed to save the seq
          // and so we retrieve those same changes once again???
          since: seq,
          conflicts: true,
          include_docs: true,
          onChange: processChange,
          complete: function (err, res) {
            setSeq(res.last_seq).then(function () {
              fulfill(all(queue));
            });
          }
        });
      });
    }

    function doQuery (options) {
      var opts = {include_docs: true};
      if (typeof options.keys !== 'undefined') {
        if (options.keys.length === 0) {
          return {rows: []};
        }

        var results = options.keys.map(function (key) {
          // can't do query here because of eval strage errors
          // db.query(fun, {
          //   key: key
          // });

          return doQuery({
            key: key,
            reduce: false
          });
        });


        return all(results).then(function (res) {
          var rows = res.reduce(function (prev, cur) {
            return prev.concat(cur.rows);
          }, []);

          return {
            total_rows: rows.length,
            rows: rows
          };
        });
      }

      if (typeof options.limit !== 'undefined') {
        // If reduce is on we can't optimize for this as we
        // need all these rows to calculate reduce
        if (options.reduce === false) {
          opts.limit = options.limit;
        }
      }
      if (typeof options.descending !== 'undefined') {
        opts.descending = options.descending;
      }
      if (typeof options.key !== 'undefined') {
        options.startkey = options.key;
        options.endkey = options.key;
      }
      if (typeof options.startkey !== 'undefined') {
        // If normal order (not descending) throw here null so that
        // this is smaller than any real key. For descending we have
        // to be bigger than any key of this kind...
        opts.startkey = pouchCollate.toIndexableString([normalizeKey(options.startkey), !options.descending ? null : {}]);
      }
      if (typeof options.endkey !== 'undefined') {
        opts.endkey = pouchCollate.toIndexableString([normalizeKey(options.endkey), !options.descending ? {} : null]);
      }

      return view.allDocs(opts).then(function (res) {
        //console.log('\n\nallDocs raw\n', res);

        res.rows = res.rows.map(function (row) {
          return {
            id: row.doc.id,
            key: row.doc.key,
            value: row.doc.value,
            doc: row.doc.doc
          };
        });

        //console.log('\n\nallDocs res', res);
        if (options.reduce === false) {
          res.rows = res.rows.slice(options.skip);
          return res;
        } else {
          return doReduce(options, res);
        }
      }).then(cleanup);

      // FIXME: don't like this
      function cleanup (data) {
        return promise(function(fulfill) {
          if (options.temp) {
            PouchDB.destroy(VIEW_PREFIX + options.name, function (err) {
              PouchDB.destroy(VIEW_META_PREFIX + options.name, function () {
                fulfill(data); // especially this!
              });
            });
          }
          fulfill(data); // and this
        })
      }
      return retrievePromise;
    }

    return getSeq().then(function (seq) {
      // console.log('seq is ', seq)

      return updateView(seq);
    }).then(function () {
      return doQuery(options);
    }).then(function (res) {
      // FIXME: maybe better place for this
      if (!options.include_docs) {
        res.rows.forEach(function (row) {
          delete row.doc;
        });
      }

      return res;
    }, function (reason) {
      console.log('big error');

      throw reason;
    });
  }

  var queryPromised = function (fun, opts) {
    if (db.type() === 'http') {
      return httpQuery(db, fun, opts);
    }

    if (typeof fun === 'object') {
      opts.name = '_temp_view';
      opts.temp = true;
      return viewQuery(fun, opts);
    }

    var parts = fun.split('/');
    return db.get('_design/' + parts[0]).then(function (doc) {
      opts.name = fun;
      if (!doc.views[parts[1]]) {
        throw { name: 'not_found', message: 'missing_named_view' };
      }

      return viewQuery({
        map: doc.views[parts[1]].map,
        reduce: doc.views[parts[1]].reduce
      }, opts);
    });
  };

  this.query = function (fun, opts, callback) {
    if (typeof opts === 'function') {
      callback = opts;
      opts = {};
    }
    if (typeof opts === 'undefined') {
      opts = {};
    }
    if (callback) {
      opts.complete = callback;
    }
    if (typeof fun === 'function') {
      fun = {map: fun};
    }
    var promise = queryPromised(fun, opts);
    promise.then(function (value) {
      if (opts.complete) {
        opts.complete(null, value);
      }
    }, opts.complete);
    return promise;
  };
}

// Deletion is a noop since we dont store the results of the view
MapReduce._delete = function () {
};
module.exports = MapReduce;
