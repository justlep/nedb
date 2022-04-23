import {getDotValue, compareThings, matchQuery, modifyDoc} from './model.js';   

/**
 * Manages access to data for a collection, be it to find, update or remove it.
 *
 * @param {Datastore} db - The datastore this cursor is bound to
 * @param {Query} query - The query this cursor will operate on
 * @param {Function} [execFn] - Handler to be executed after cursor has found the results 
 *                              and before the callback passed to find/findOne/update/remove
 * @param {?Object} [projection] - see {@link projection}
 * @param {number} [limit] - default is no limit
 */
export class Cursor {
    
    constructor(db, query, execFn, projection = null, limit = null) {
        this.db = db;
        this.query = query || Object.create(null);
        this.execFn = execFn || null;
        this._projection = projection;
        this._limit = limit;
    }

    /**
     * Set a limit to the number of results
     * @return {Cursor}
     */
    limit(limit) {
        this._limit = limit;
        return this;
    }

    /**
     * Skip a the number of results
     * @return {Cursor}
     */
    skip(skip) {
        this._skip = skip;
        return this;
    }

    /**
     * Sort results of the query
     * @param {SortQuery} sortQuery - SortQuery is { field: order }, field can use the dot-notation, order is 1 for ascending and -1 for descending
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
     */
    project(candidates) {
        let res = [],
            keepId, 
            action;

        if (!this._projection || !Object.keys(this._projection).length) {
            return candidates;
        }

        keepId = this._projection._id !== 0;
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
            res.push(toPush);
        }

        return res;
    }


    /**
     * Get all matching elements
     * Will return pointers to matched elements (shallow copies), returning full copies is the role of find or findOne
     * This is an internal function, use exec which uses the executor
     *
     * @param {Function} _callback - Signature: err, results
     */
    _exec(_callback) {
        let res = [],
            added = 0,
            skipped = 0,
            error = null;

        const callback = (err, res) => this.execFn ? this.execFn(err, res, _callback) : _callback(err, res);

        this.db.getCandidates(this.query, (err, candidates) => {
            if (err) {
                return callback(err);
            }

            try {
                for (const candidate of candidates) {
                    if (!matchQuery(candidate, this.query)) {
                        continue;
                    }
                    // If a sort is defined, wait for the results to be sorted before applying limit and skip
                    if (this._sort) {
                        res.push(candidate);
                    } else if (this._skip && this._skip > skipped) {
                        skipped++;
                    } else {
                        res.push(candidate);
                        added++;
                        if (this._limit && this._limit <= added) {
                            break;
                        }
                    }
                }
            } catch (err) {
                return callback(err);
            }

            // Apply all sorts
            if (this._sort) {
                const criteria = Object.keys(this._sort).map(key => ({key, direction: this._sort[key]}));
                res.sort((a, b) => {
                    for (const criterion of criteria) {
                        let compare = criterion.direction * compareThings(getDotValue(a, criterion.key), 
                                                                          getDotValue(b, criterion.key), 
                                                                          this.db.compareStrings);
                        if (compare !== 0) {
                            return compare;
                        }
                    }
                    return 0;
                });

                // Applying limit and skip
                let limit = this._limit || res.length,
                    skip = this._skip || 0;

                res = res.slice(skip, skip + limit);
            }

            // Apply projection
            try {
                res = this.project(res);
            } catch (e) {
                error = e;
                res = undefined;
            }

            return callback(error, res);
        });
    }

    exec() {
        this.db.executor.push({this: this, fn: this._exec, arguments});
    }
    
}
