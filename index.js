'use strict';

var PouchDB = require('pouchdb');
var pouchCollate = require('pouchdb-collate');
var Promise = require('lie');
var collate = pouchCollate.collate;
var normalizeKey = pouchCollate.normalizeKey;
var httpQuery = require('./httpQuery.js');
var promise = require('lie');
var all = require('lie-all');
// This is the first implementation of a basic plugin, we register the
// plugin object with pouch and it is mixin'd to each database created
// (regardless of adapter), adapters can override plugins by providing
// their own implementation. functions on the plugin object that start
// with _ are reserved function that are called by pouchdb for special
// notifications.

// If we wanted to store incremental views we can do it here by listening
// to the changes feed (keeping track of our last update_seq between page loads)
// and storing the result of the map function (possibly using the upcoming
// extracted adapter functions)

function MapReduceError(name, msg, code) {
  this.name = name;
  this.message = msg;
  this.status =  code;
}
MapReduceError.prototype = new Error();

function sortByIdAndValue(a, b) {
  // sort by id, then value
  var idCompare = collate(a.id, b.id);
  return idCompare !== 0 ? idCompare : collate(a.value, b.value);
}

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
            return new MapReduceError(
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

  function mapUsingKeys(inputResults, keys) {
    inputResults.sort(sortByIdAndValue);

    var results = [];
    keys.forEach(function (key) {
      inputResults.forEach(function (res) {
        if (collate(key, res.key) === 0) {
          results.push(res);
        }
      });
    });
    return results;
  }

  function viewQuery(fun, options) {
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
        // FIXME: clone
        doc: JSON.parse(JSON.stringify(currentDoc))
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

    //only proceed once all documents are mapped and joined
    function processResults(results) {
      //console.log('\n\nprocessResults\n', results)

      var error;

      if (typeof options.keys !== 'undefined' && results.length) {
        // user supplied a keys param, sort by keys
        results = mapUsingKeys(results, options.keys);
      } else { // normal sorting
        results.rows.sort(function (a, b) {
          // sort by key, then id
          var keyCollate = collate(a.key, b.key);
          return keyCollate !== 0 ? keyCollate : collate(a.id, b.id);
        });
      }
      if (options.descending) {
        results.reverse();
      }
      if (options.reduce === false) {
        return options.complete(null, results);

        return options.complete(null, {
          total_rows: results.length,
          offset: options.skip,
          rows: ('limit' in options) ? results.slice(options.skip, options.limit + options.skip) :
            (options.skip > 0) ? results.slice(options.skip) : results
        });
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
        if (e.value.sumsqr && e.value.sumsqr instanceof MapReduceError) {
          error = e.value;
          return;
        }
        e.key = e.key[0][0];
      });
      if (error) {
        options.complete(error);
        return;
      }
      options.complete(null, {
        total_rows: groups.length,
        offset: options.skip,
        rows: ('limit' in options) ? groups.slice(options.skip, options.limit + options.skip) :
        (options.skip > 0) ? groups.slice(options.skip) : groups
      });
    }

    // TODO: what about slashes in db_name?
    // TODO: where should we destroy it?
    options.name = options.name.replace(/\//g, '_') + Math.random();

    var view = new PouchDB('_pouchdb_views_' + options.name);

    var modifications = [];
    db.changes({
      conflicts: true,
      include_docs: true,
      onChange: function (change) {
        //console.log('\nonChange', change);

        results = [];
        if ('deleted' in change || change.id[0] === "_") {
          return;
        }
        // FIXME: clone
        // 
        currentDoc = JSON.parse(JSON.stringify(change.doc));
        fun.map.call(this, change.doc);

        // problems:
        // 1. we have to add map from _id to list of emitted values
        // 2. this should be processed one by one because otherwise
        // we could mess up. Can we? Remember that in changes feed
        // one 
        if (!results.length) {
          return;
        }
        var mods = promise(function (resolve, reject) {
          all(results).then(function (results) {
            //console.log('results', results)
            var rows = results.map(function (row) {
              //console.log('emitted', row)

              var view_key = [row.key, row.id, row.value];
              return {
                _id: pouchCollate.toIndexableString(view_key),
                id: row.id,
                key: row.key,
                value: row.value,
                doc: row.doc
              };
            });
            var b = view.bulkDocs({docs: rows});
            b.then(function () {
              //console.log('bulk finished')
            });
            resolve(b);
          });
        });
        modifications.push(mods);
      },
      complete: function () {
        all(modifications).then(function () {
          var opts = {include_docs: true};

          if (typeof options.keys !== 'undefined') {
            var results = options.keys.map(function (key) {
              return db.query({key: key});
            });
            all(results).then(function (res) {
              console.log(res);
            }, options.complete);
            return;
          }

          if (typeof options.limit !== 'undefined') {
            opts.limit = options.limit;
          }
          if (typeof options.descending !== 'undefined') {
            opts.descending = options.descending;
          }
          if (typeof options.key !== 'undefined') {
            options.startkey = options.key;
            options.endkey = options.key;
          }
          if (typeof options.startkey !== 'undefined') {
            opts.startkey = pouchCollate.toIndexableString([options.startkey, null]);
          }
          if (typeof options.endkey !== 'undefined') {
            opts.endkey = pouchCollate.toIndexableString([options.endkey, {}]);
          }

          view.allDocs(opts).then(function (res) {
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
              options.complete(null, res);
            } else {
              doReduce(options, res);
            }
          }, function (reason) {
            console.log('ERROR', reason);
          });
        });
      }
    });
  }

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
    var realCB = opts.complete;
    var promise = new Promise(function (resolve, reject) {
      opts.complete = function (err, data) {
        if (err) {
          reject(err);
        } else {
          resolve(data);
        }
      };

      if (typeof fun === 'function') {
        fun = {map: fun};
      }

      if (db.type() === 'http') {
        return httpQuery(db, fun, opts);
      }

      if (typeof fun === 'object') {
        opts.name = 'temp_view';
        return viewQuery(fun, opts);
      }

      var parts = fun.split('/');
      db.get('_design/' + parts[0], function (err, doc) {
        if (err) {
          opts.complete(err);
          return;
        }
        opts.name = fun;

        if (!doc.views[parts[1]]) {
          opts.complete({ name: 'not_found', message: 'missing_named_view' });
          return;
        }
        viewQuery({
          map: doc.views[parts[1]].map,
          reduce: doc.views[parts[1]].reduce
        }, opts);
      });
    });
    if (realCB) {
      promise.then(function (resp) {
        realCB(null, resp);
      }, realCB);
    }
    return promise;
  };
}

// Deletion is a noop since we dont store the results of the view
MapReduce._delete = function () {
};
module.exports = MapReduce;
