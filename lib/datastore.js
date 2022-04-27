import {hasOwnProp, hasAnyOwnPropOf, generateUid, isDate, defaultErrorCallback} from './customUtils.js';
import {Index} from './index-basic.js';
import {Executor} from './executor.js';
import {Persistence} from './persistence.js';
import {Cursor} from './cursor.js';
import {EventEmitter} from 'events';
import {checkObject, cloneDeep, matchQuery, modifyDoc} from './model.js';
import {legacyEachSeries, legacyWaterfall} from './async-legacy.js';
import {UniqueStringIndex} from './index-uniqueString.js';

const NOP = () => null;

const EXEC_FN_COUNT_DOCS = (err, docs, cb) => err ? cb(err) : cb(null, docs.length);
const EXEC_FN_CLONE_DOCS = (err, docs, cb) => err ? cb(err) : cb(null, docs.map(cloneDeep));
const EXEC_FN_CLONE_SINGLE_DOC = (err, docs, cb) => err ? cb(err) : cb(null, docs.length === 1 ? cloneDeep(docs[0]) : null);

/**
 * Represents a new collection
 *
 * @emits {String} "compaction.done" - Fired whenever a compaction operation was finished
 */
export class Datastore extends EventEmitter {

    /**
     * @param {String} [filename] Optional, datastore will be in-memory only if not provided
     * @param {Boolean} [timestampData] Optional, defaults to false. If set to true, createdAt and updatedAt will be created and populated automatically (if not specified by user)
     * @param {Boolean} [inMemoryOnly] Optional, defaults to false
     * @param {Boolean} [autoload] Optional, defaults to false
     * @param {Function} [onload] Optional, if autoload is used this will be called after the load database with the error object as parameter. If you don't pass it the error will be thrown
     * @param {Function} [beforeDeserialization] Optional, serialization hooks
     * @param {Function} [afterSerialization] Optional, serialization hooks
     * @param {Number} [corruptAlertThreshold] Optional, threshold after which an alert is thrown if too much data is corrupt
     * @param {Function} [compareStrings] Optional, string comparison function that overrides default for sorting
     */
    constructor({filename, timestampData, inMemoryOnly, autoload, onload, beforeDeserialization, afterSerialization, 
                    corruptAlertThreshold, compareStrings} = {}) {
        super();
        
        if (arguments.length && (!arguments[0] || typeof arguments[0] !== 'object')) {
            // no more retro-compatibility with v0.6 and before
            throw new Error('Invalid Datastore options');
        }
        
        if (filename && typeof filename !== 'string') {
            throw new Error('Invalid datastore filename');
        }
        
        /** @type {?string} */
        this.filename = filename || null;
        this.inMemoryOnly = !filename || !!inMemoryOnly;
        this.autoload = !!autoload;
        this.timestampData = !!timestampData;
        this.compareStrings = compareStrings;

        this.persistence = new Persistence({
            db: this,
            afterSerialization,
            beforeDeserialization,
            corruptAlertThreshold
        });

        // This new executor is ready if we don't use persistence, otherwise once loadDatabase is called
        this.executor = new Executor(this.inMemoryOnly);

        // Indexed by field name, dot notation can be used
        // _id is always indexed and since _ids are generated randomly the underlying
        // binary is always well-balanced
        
        /** @type {Object.<string, NedbIndex>} - mapping fieldName -> NedbIndex implementation */
        this.indexes = Object.create(null);

        this.indexes._id = new UniqueStringIndex('_id'); // was: new Index({fieldName: '_id', unique: true});

        /** @type {Object.<string, NedbIndex>} - mapping fieldName -> NedbIndex implementation */
        this.ttlIndexes = Object.create(null);

        // Queue a load of the database right away and call the onload handler
        // By default (no onload handler), if there is an error there, no operation will be possible so warn the user by throwing an exception
        if (this.autoload) {
            this.loadDatabase(onload);
        }
    }

    /**
     * Load the database from the datafile, and trigger the execution of buffered commands if any
     * @param {NedbErrorCallback} [cb] Optional callback
     */
    loadDatabase(cb) {
        this.executor.push({this: this.persistence, fn: this.persistence.loadDatabase, arguments: [cb || defaultErrorCallback]}, true);
    }

    /**
     * Get an array of all the data in the database
     * @return {Object[]} - array of documents
     */
    getAllData() {
        return this.indexes._id.getAll();
    }

    /**
     * Reset all currently defined indexes
     * @param {Object|Object[]} [newDocOrDocs] - new single document or array of documents to add to the index after reset
     */
    resetIndexes(newDocOrDocs) {
        for (const index of Object.values(this.indexes)) {
            index.reset(newDocOrDocs);
        }
    }

    /**
     * Ensure an index is kept for this field. Same parameters as lib/indexes
     * For now this function is synchronous, we need to test how much time it takes
     * We use an async API for consistency with the rest of the code
     * @param {Object} options
     * @param {String} options.fieldName
     * @param {Boolean} [options.unique]
     * @param {Boolean} [options.sparse]
     * @param {Number} [options.expireAfterSeconds] - Optional, if set this index becomes a TTL index (only works on Date fields, not arrays of Date)
     * @param {NedbErrorCallback} [cb] Optional callback
     */
    ensureIndex(options, cb) {
        let err;

        if (!options.fieldName) {
            err = new Error('Cannot create an index without a fieldName');
            err.missingFieldName = true;
            return cb?.(err);
        }
        if (this.indexes[options.fieldName]) {
            return cb?.(null);
        }

        this.indexes[options.fieldName] = new Index(options);
        if (options.expireAfterSeconds !== undefined) {
            this.ttlIndexes[options.fieldName] = options.expireAfterSeconds;
        }   // With this implementation index creation is not necessary to ensure TTL but we stick with MongoDB's API here

        try {
            this.indexes[options.fieldName].insert(this.getAllData());
        } catch (err) {
            delete this.indexes[options.fieldName];
            return cb?.(err);
        }

        // We may want to force all options to be persisted including defaults, not just the ones passed the index creation function
        this.persistence.persistNewState([{$$indexCreated: options}], cb);
    }

    /**
     * Remove an index
     * @param {String} fieldName
     * @param {NedbErrorCallback} [cb] Optional callback
     */
    removeIndex(fieldName, cb) {
        delete this.indexes[fieldName];
        this.persistence.persistNewState([{$$indexRemoved: fieldName}], cb);
    }

    /**
     * Add one or several document(s) to all indexes
     * @param {Object} doc
     */
    addToIndexes(doc) {
        let allIndexes = Object.values(this.indexes),
            totalRevertible = 0;

        try {
            for (const index of allIndexes) {
                index.insert(doc);
                totalRevertible++;
            }
        } catch (err) {
            // If an error happened, we need to rollback the insert on all other indexes
            while (totalRevertible > 0) {
                allIndexes[--totalRevertible].remove(doc);
            }
            throw err;
        }
    }

    /**
     * If one insertion fails (e.g. because of a unique constraint), roll back all previous
     * inserts and throws the error
     * @param {Object[]} preparedDocs
     */
    _addMultipleDocsToIndexes(preparedDocs) {
        let totalRevertible = 0;
        try {
            for (const doc of preparedDocs) {
                this.addToIndexes(doc);
                totalRevertible++;
            }
        } catch (err) {
            while (totalRevertible) {
                this._removeFromIndexes(preparedDocs[--totalRevertible]);
            }
            throw err;
        }
    }
    
    /**
     * Remove one or several document(s) from all indexes
     * @param {Object|Object[]} docOrDocs
     */
    _removeFromIndexes(docOrDocs) {
        for (const index of Object.values(this.indexes)) {
            index.remove(docOrDocs);
        }
    }

    /**
     * Update one or several documents in all indexes
     * To update multiple documents, oldDoc must be an array of { oldDoc, newDoc } pairs
     * If one update violates a constraint, all changes are rolled back
     * @param {Object|NedbOldNewDocPair[]} oldDoc
     * @param {Object} [newDoc]
     */
    _updateIndexes(oldDoc, newDoc) {
        let allIndexes = Object.values(this.indexes),
            totalRevertible = 0;

        try {
            for (const index of allIndexes) {
                index.update(oldDoc, newDoc);
                totalRevertible++;
            }
        } catch (err) {
            // If an error happened, we need to rollback the update on all other indexes
            while (totalRevertible > 0) {
                allIndexes[--totalRevertible].revertUpdate(oldDoc, newDoc);
            }
            throw err;
        }
    }

    /**
     * Return the list of candidates for a given query
     * Crude implementation for now, we return the candidates given by the first usable index if any
     * We try the following query types, in this order: basic match, $in match, comparison match
     * One way to make it better would be to enable the use of multiple indexes if the first usable index
     * returns too much data. I may do it in the future.
     *
     * Returned candidates will be scanned to find and remove all expired documents
     *
     * @param {Object} query
     * @param {function(?Error, Object[])} callback 
     * @param {Boolean} [dontExpireStaleDocs] - If true, don't remove stale docs. 
     *                                          Useful for the remove function which shouldn't be impacted by expirations
     * @internal
     */
    getCandidates(query, callback, dontExpireStaleDocs = false) {
        let indexNames = Object.keys(this.indexes); // for few # of indexes, indexOf() is faster than object lookups or Set.has

        legacyWaterfall([
            // STEP 1: get candidates list by checking indexes from most to least frequent usecase
            (cb) => {
                const allQueryKeys = Object.keys(query);
                
                // frequent use-case: getById()
                if (allQueryKeys.length === 1 && query._id) {
                    return cb(null, this.indexes._id.getMatching(query._id));
                }
                
                // For a basic match
                let usableQueryKeys = allQueryKeys.filter(key => {
                    if (~indexNames.indexOf(key)) {
                        let valType = typeof query[key];
                        return (valType === 'string' || valType === 'number' || valType === 'boolean' || isDate(query[key]) || query[key] === null);
                    }
                    return false;
                });
                if (usableQueryKeys.length) {
                    return cb(null, this.indexes[usableQueryKeys[0]].getMatching(query[usableQueryKeys[0]]));
                }

                // For a $in match
                usableQueryKeys = allQueryKeys.filter(key => hasOwnProp(query[key], '$in') && ~indexNames.indexOf(key));
                if (usableQueryKeys.length) {
                    return cb(null, this.indexes[usableQueryKeys[0]].getMatching(query[usableQueryKeys[0]].$in));
                }

                // For a comparison match
                usableQueryKeys = allQueryKeys.filter(key => ~indexNames.indexOf(key) && hasAnyOwnPropOf(query[key], '$lt', '$lte', '$gt', '$gte'));
                if (usableQueryKeys.length) {
                    return cb(null, this.indexes[usableQueryKeys[0]].getBetweenBounds(query[usableQueryKeys[0]]));
                }

                // By default, return all the DB data
                return cb(null, this.getAllData());
            },
            // STEP 2: remove all expired documents
            (docs, cb) => {
                if (dontExpireStaleDocs) {
                    return cb(null, docs);
                }

                let ttlIndexesFieldNames = Object.keys(this.ttlIndexes);
                if (!ttlIndexesFieldNames.length) {
                    return cb(null, docs);
                }

                let expiredDocsIds = [],
                    validDocs = [],
                    now = Date.now();
                
                for (const doc of docs) {
                    if (ttlIndexesFieldNames.find(f => isDate(doc[f]) && now > doc[f].getTime() + this.ttlIndexes[f] * 1000)) {
                        expiredDocsIds.push(doc._id);
                    } else {
                        validDocs.push(doc);
                    }
                }

                if (!expiredDocsIds.length) {
                    return cb(null, validDocs);
                }
                
                legacyEachSeries(expiredDocsIds, (_id, _cb) => this._remove({_id}, {}, _cb),
                                                 (/* err ignored */) => cb(null, validDocs));
            }
            
        ], (err, docs) => callback(err || null, docs));
    }

    /**
     * Insert a new document
     * @param {Object|Object[]} docOrDocs
     * @param {function(?Error, Object|Object[])} [cb] - optional callback, e.g. function(err, insertedDocOrDocs)
     */
    _insert(docOrDocs, cb) {
        let docs = Array.isArray(docOrDocs) ? docOrDocs : [docOrDocs],
            preparedDocs,
            singlePreparedDoc;
       
        try {
            // prepare doc or docs for insertion
            // i.e. adds _id and timestamps if necessary on a copy of newDoc to avoid any side effect on user input
            
            let useTimestamp = this.timestampData ? new Date() : false; 
            
            preparedDocs = docs.map(doc => {
                let preparedDoc = cloneDeep(doc);
                if (preparedDoc._id === undefined) {
                    preparedDoc._id = this.createNewId();
                }
                if (useTimestamp) {
                    if (preparedDoc.createdAt === undefined) {
                        preparedDoc.createdAt = useTimestamp;
                    }
                    if (preparedDoc.updatedAt === undefined) {
                        preparedDoc.updatedAt = useTimestamp;
                    }
                }
                return preparedDoc;
            });

            checkObject(...preparedDocs);
            
            if (preparedDocs.length === 1) {
                singlePreparedDoc = preparedDocs[0];
                this.addToIndexes(singlePreparedDoc);
            } else {
                this._addMultipleDocsToIndexes(preparedDocs);
            }
        } catch (err) {
            return cb?.(err);
        }

        this.persistence.persistNewState(preparedDocs, err => cb?.(err || null, err ? null : cloneDeep(singlePreparedDoc || preparedDocs)));
    }

    /**
     * Create a new _id that's not already in use
     * @return {string}
     */
    createNewId() {
        let id;
        do {
            id = generateUid(16);
        } while (this.indexes._id.getMatching(id).length);
        return id;
    }

    /**
     * Insert a new document
     * @param {Object|Object[]} docOrDocs
     * @param {function(?Error, Object|Object[])} [cb] - optional callback, e.g. function(err, insertedDocOrDocs)
     */
    insert(docOrDocs, cb) {
        this.executor.push({this: this, fn: this._insert, arguments: [docOrDocs, cb]});
    }

    /**
     * Count all documents matching the query
     * @param {Object} query MongoDB-style query
     * @param {function(?Error, number)} [callback] - optional callback, e.g. function(error, numMatches) 
     * @return {Cursor|void} - the Cursor if no callback was given
     */
    count(query, callback) {
        let cursor = new Cursor(this, query, EXEC_FN_COUNT_DOCS);
        
        if (typeof callback !== 'function') {
            return cursor;
        }
        cursor.exec(callback);
    }

    /**
     * Find all documents matching the query
     * If no callback is passed, we return the cursor so that user can limit, skip and finally exec
     * @param {Object} query MongoDB-style query
     * @param {Object} [projection] MongoDB-style projection
     * @param {function(?Error, Object[])} [callback] - result callback, e.g. function(err, matchedDocumentClones}
     * @return {Cursor|void} - the Cursor if no callback was given
     */
    find(query, projection, callback) {
        if (arguments.length === 2 && typeof projection === 'function') {
            callback = projection;
            projection = null;
        }

        let cursor = new Cursor(this, query, EXEC_FN_CLONE_DOCS, projection);

        if (typeof callback !== 'function') {
            return cursor;
        }
        cursor.exec(callback);
    }

    /**
     * Find one document matching the query
     * @param {Object} query MongoDB-style query
     * @param {Object} [projection] MongoDB-style projection
     * @param {function(?Error, Object)} [callback] - result callback, e.g. function(err, matchedDocumentClone}
     * @return {Cursor|void} - the Cursor if no callback was given
     */
    findOne(query, projection, callback) {
        if (arguments.length === 2 && typeof projection === 'function') {
            callback = projection;
            projection = null;
        }

        let cursor = new Cursor(this, query, EXEC_FN_CLONE_SINGLE_DOC, projection, 1);
        
        if (typeof callback !== 'function') {
            return cursor;
        }
        cursor.exec(callback);
    }

    /**
     * Update all docs matching query
     * @param {Object} query
     * @param {Object} updateQuery
     * @param {NedbUpdateOptions} [options]
     * @param {NedbUpdateCallback} [callback]
     *
     * WARNING: The API was changed between v1.7.4 and v1.8, for consistency and readability reasons. Prior and including to v1.7.4,
     *          the callback signature was (err, numAffected, updated) where updated was the updated document in case of an upsert
     *          or the array of updated documents for an update if the returnUpdatedDocs option was true. That meant that the type of
     *          affectedDocuments in a non multi update depended on whether there was an upsert or not, leaving only two ways for the
     *          user to check whether an upsert had occured: checking the type of affectedDocuments or running another find query on
     *          the whole dataset to check its size. Both options being ugly, the breaking change was necessary.
     * @private
     */
    _updateOrInsert(query, updateQuery, options, callback = NOP) {
        if (typeof options === 'function') {
            callback = options;
            options = {};
        }
        
        if (!options.upsert) {
            return this._update(query, updateQuery, options, callback);
        } else if (options.multi) {
            throw new Error('');
        }
        
        // Need to use an internal function not tied to the executor to avoid deadlock
        new Cursor(this, query).limit(1)._exec((err, docs) => {
            if (err) {
                return callback(err);
            }
            if (docs.length === 1) {
                return this._update(query, updateQuery, options, callback);
            }

            // from here it's a pure insert
            
            let toBeInserted;

            try {
                checkObject(updateQuery);
                // updateQuery is a simple object with no modifier, use it as the document to insert
                toBeInserted = updateQuery;
            } catch (e) {
                // updateQuery contains modifiers, use the find query as the base,
                // strip it from all operators and update it according to updateQuery
                try {
                    toBeInserted = modifyDoc(cloneDeep(query, true), updateQuery);
                } catch (err) {
                    return callback(err);
                }
            }

            this._insert(toBeInserted, (err, newDoc) => err ? callback(err) : callback(null, 1, newDoc, true));
        });
    }

    /**
     * @param {Object} query
     * @param {Object} updateQuery
     * @param {NedbUpdateOptions} [options]
     * @param {NedbUpdateCallback} [callback] 
     * @private
     */
    _update(query, updateQuery, {multi, returnUpdatedDocs}, callback) {
        let modifications = [],
            totalReplaced = 0;

        this.getCandidates(query, (err, candidates) => {
            if (err) {
                return callback(err);
            }

            // Preparing update (if an error is thrown here neither the datafile nor
            // the in-memory indexes are affected)
            try {
                let newUpdatedAt = this.timestampData ? new Date() : false;
                for (const oldDoc of candidates) {
                    if (matchQuery(oldDoc, query) && (multi || !totalReplaced)) {
                        totalReplaced++;
                        let newDoc = modifyDoc(oldDoc, updateQuery);
                        if (newUpdatedAt) {
                            newDoc.createdAt = oldDoc.createdAt;
                            newDoc.updatedAt = newUpdatedAt;
                        }
                        modifications.push({oldDoc, newDoc});
                    }
                }
            } catch (err) {
                return callback(err);
            }

            // Change the docs in memory
            try {
                this._updateIndexes(modifications);
            } catch (err) {
                return callback(err);
            }

            // Update the datafile
            let updatedDocs = modifications.map(m => m.newDoc);
            
            this.persistence.persistNewState(updatedDocs, (err) => {
                if (err) {
                    return callback(err);
                }
                if (!returnUpdatedDocs) {
                    return callback(null, totalReplaced);
                }
                let updatedDocClones = multi ? updatedDocs.map(doc => cloneDeep(doc)) : cloneDeep(updatedDocs[0]);

                callback(null, totalReplaced, updatedDocClones);
            });
        });
    }

    /**
     * @param {Object} query
     * @param {Object} updateQuery
     * @param {NedbUpdateOptions} [options]
     * @param {NedbUpdateCallback} [callback]
     */
    update(query, updateQuery, options, callback) {
        this.executor.push({this: this, fn: this._updateOrInsert, arguments});
    }

    /**
     * Remove all docs matching the query
     * For now very naive implementation (similar to update)
     * 
     * @param {Object} query
     * @param {NedbRemovalOptions} [options] 
     * @param {NedbRemovalCallback} [cb]
     */
    _remove(query, options, cb) {
        if (typeof options === 'function') {
            cb = options;
            options = {};
        }
        
        let callback = cb || NOP;

        this.getCandidates(query, (err, candidates) => {
            if (err) {
                return callback(err);
            }
            let multi = !!options.multi,
                removedDocs = [],
                totalRemoved = 0;
            
            try {
                for (const d of candidates) {
                    if (matchQuery(d, query) && (multi || !totalRemoved)) {
                        totalRemoved++;
                        removedDocs.push({$$deleted: true, _id: d._id});
                        this._removeFromIndexes(d);
                    }
                }
            } catch (err) {
                return callback(err);
            }

            this.persistence.persistNewState(removedDocs, (err) => err ? callback(err) : callback(null, totalRemoved));
        }, true /* dontExpireStaleDocs */);
    }

    /**
     * Remove a single doc (default) or multiple docs matching the query.
     * Set options.multi=true to allow removing multiple.
     * 
     * @param {Object} query
     * @param {NedbRemovalOptions} [options]
     * @param {NedbRemovalCallback} [cb]
     */
    remove(query, options, cb) {
        this.executor.push({this: this, fn: this._remove, arguments});
    }
}


/**
 * @typedef {Object} NedbUpdateOptions
 * @property {boolean} [multi] - If true, can update multiple documents (defaults to false)
 * @property {boolean} [upsert] - If true, document is inserted if the query doesn't match anything
 * @property {boolean} [returnUpdatedDocs] - if true, return as third argument the array of updated matched documents
 *                                                  (even if no change actually took place)
 */

/**
 * @callback NedbUpdateCallback
 * @param {?Error} err
 * @param {number} [numAffected]
 * @param {number} [affectedDocuments] - one of the following:
 *                           * for an upsert: the upserted document
 *                           * for an update with returnUpdatedDocs=false: null
 *                           * for an update with returnUpdatedDocs=true & multi=false: the updated document
 *                           * for an update with returnUpdatedDocs=true & multi=true: an array of updated documents
 * @param {boolean} [upsert] - true if the update was an upsert
 */

/**
 * @typedef {Object} NedbRemovalOptions
 * @property {boolean} [multi] - Set true to remove multiple (all) matched documents.
 *                             By default (false), only the first matching document will be removed.
 */

/**
 * @callback NedbRemovalCallback
 * @param {?Error} error
 * @param {number} totalRemoved
 */
