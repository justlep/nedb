import {getDotValue, compareThings, matchQuery, modifyDoc} from './model.js';   

/**
 * Manages access to data for a collection, be it to find, update or remove it.
 */
export class Cursor {

    /**
     * @param {Datastore} db - The datastore this cursor is bound to
     * @param {Object} query - The query this cursor will operate on
     * @param {NedbCursorExecFunction} [execFn] - Handler to be executed after cursor has found the results
     *                                            and before the callback passed to find/findOne/update/remove
     * @param {?Object} [projection] - see {@link projection}
     * @param {number} [limit] - default is no limit
     */
    constructor(db, query, execFn, projection = null, limit = 0) {
        this.db = db;
        this.query = query || Object.create(null);
        this.execFn = execFn;
        this._projection = projection;
        this._limit = limit;
        this._skip = 0;
    }

    /**
     * @callback NedbCursorExecFunction
     * @param {?Error} err
     * @param {Object[]} documents
     * @param {function} callback
     */
    
    /**
     * Set a limit to the number of results
     * @param {number} limit
     * @return {Cursor}
     */
    limit(limit) {
        this._limit = limit || 0;
        return this;
    }

    /**
     * Skip a the number of results
     * @param {number} skip
     * @return {Cursor}
     */
    skip(skip) {
        this._skip = skip || 0;
        return this;
    }

    /**
     * Sort results of the query
     * @param {SortQuery} sortQuery 
     * @return {Cursor}
     */
    sort(sortQuery) {
        this._sort = sortQuery;
        return this;
    }

    /**
     * Add the use of a projection
     * @param {Object} projection - MongoDB-style projection. {} means take all fields. Then it's { key1: 1, key2: 1 } to take only key1 and key2
     *                              { key1: 0, key2: 0 } to omit only key1 and key2. Except _id, you can't mix takes and omits
     */
    projection(projection) {
        this._projection = projection;
        return this;
    }

    /**
     * Apply the projection
     * @param {Object[]} candidates - documents to apply projection to
     * @return {Object[]} - documents
     */
    project(candidates) {
        if (!this._projection || !Object.keys(this._projection).length) {
            return candidates;
        }

        let docs = [],
            keepId = this._projection._id !== 0,
            action;

        // continue with an '_id'-free copy of projection
        this._projection = Object.assign(Object.create(null), this._projection);
        delete this._projection._id;

        // Check for consistency
        let keys = Object.keys(this._projection);
        for (const k of keys) {
            if (action !== undefined && this._projection[k] !== action) {
                throw new Error('Can\'t both keep and omit fields except for _id');
            }
            action = this._projection[k];
        }

        // Do the actual projection
        for (const candidate of candidates) {
            let toPush;
            if (action === 1) { // pick-type projection
                let $set = Object.create(null);
                for (let k of keys) {
                    let dotVal = getDotValue(candidate, k);
                    if (dotVal !== undefined) {
                        $set[k] = dotVal;
                    }
                }
                toPush = modifyDoc(Object.create(null), {$set});
            } else {   // omit-type projection
                let $unset = Object.create(null);
                for (let k of keys) {
                    $unset[k] = true;
                }
                toPush = modifyDoc(candidate, {$unset});
            }
            if (keepId) {
                toPush._id = candidate._id;
            } else {
                delete toPush._id;
            }
            docs.push(toPush);
        }

        return docs;
    }

    /**
     * @callback NedbCursorExecResultCallback
     * @param {?Error} err
     * @param {Object[]} docs 
     * 
     */

    /**
     * Get all matching elements
     * Will return pointers to matched elements (shallow copies), returning full copies is the role of find or findOne
     * This is an internal function, use exec which uses the executor
     *
     * @param {NedbCursorExecResultCallback} _callback - e.g. function(err, docs)
     */
    _exec(_callback) {
        const callback = (err, _docs) => this.execFn ? this.execFn(err, _docs, _callback) : _callback(err, _docs);

        let docs;
        
        this.db.getCandidates(this.query, (err, candidates) => {
            if (err) {
                return callback(err);
            }

            if (this._sort) { 
                
                // match-all first, then sort, then skip/limit
                
                try {
                    docs = candidates.filter(doc => matchQuery(doc, this.query));
                } catch (err) {
                    return callback(err);
                }

                const criteria = Object.keys(this._sort).map(key => ({key, direction: this._sort[key]}));
                     
                docs.sort((a, b) => {
                    for (const criterion of criteria) {
                        let comp = criterion.direction * compareThings(getDotValue(a, criterion.key), 
                                                                       getDotValue(b, criterion.key), this.db.compareStrings);
                        if (comp !== 0) {
                            return comp;
                        }
                    }
                    return 0;
                });

                if (this._limit || this._skip) {
                    docs = docs.slice(this._skip, this._skip + (this._limit || docs.length));
                }
                
            } else {
                
                // match-all + skip/limit, both in one go 
                
                try {
                    docs = [];

                    let stillToSkip = this._skip,
                        stillToAdd = this._limit;

                    for (const candidate of candidates) {
                        if (!matchQuery(candidate, this.query)) {
                            continue;
                        }
                        if (stillToSkip) {
                            stillToSkip--;
                        } else {
                            docs.push(candidate);
                            if (!--stillToAdd) {
                                break;
                            }
                        }
                    }
                } catch (err) {
                    return callback(err);
                }
            }
            
            // Apply projection
            try {
                docs = this.project(docs); 
            } catch (err) {
                return callback(err);
            }

            callback(null, docs);
        });
    }

    exec() {
        this.db.executor.push({this: this, fn: this._exec, arguments});
    }
    
}

/**
 * A SortQuery is {'fieldName': order}, where fieldName can use the dot-notation, order 1 means ascending / -1 descending
 * @typedef {Object.<string, number>} SortQuery
 */
