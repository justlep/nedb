import {hasOwnProp, hasAnyOwnPropOf, generateUid, isDate} from './customUtils.js';
import * as model from './model.js';
import async from 'async';
import {Index} from './indexes.js';
import {Executor} from './executor.js';
import {Persistence} from './persistence.js';
import {Cursor} from './cursor.js';
import _ from 'underscore';
import {EventEmitter} from 'events';

const NOP = () => null;

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
        this.indexes = Object.create(null);
        this.indexes._id = new Index({fieldName: '_id', unique: true});
        this.ttlIndexes = Object.create(null);

        // Queue a load of the database right away and call the onload handler
        // By default (no onload handler), if there is an error there, no operation will be possible so warn the user by throwing an exception
        if (this.autoload) {
            this.loadDatabase(onload || function(err) {
                if (err) {
                    throw err;
                }
            });
        }
    }

    /**
     * Load the database from the datafile, and trigger the execution of buffered commands if any
     */
    loadDatabase() {
        this.executor.push({this: this.persistence, fn: this.persistence.loadDatabase, arguments}, true);
    }

    /**
     * Get an array of all the data in the database
     */
    getAllData() {
        return this.indexes._id.getAll();
    }

    /**
     * Reset all currently defined indexes
     */
    resetIndexes(newData) {
        for (const index of Object.values(this.indexes)) {
            index.reset(newData);
        }
    }

    /**
     * Ensure an index is kept for this field. Same parameters as lib/indexes
     * For now this function is synchronous, we need to test how much time it takes
     * We use an async API for consistency with the rest of the code
     * @param {Object} options
     * @param {String} options.fieldName
     * @param {Boolean} options.unique
     * @param {Boolean} options.sparse
     * @param {Number} options.expireAfterSeconds - Optional, if set this index becomes a TTL index (only works on Date fields, not arrays of Date)
     * @param {Function} [cb] Optional callback, signature: err
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
     * @param {Function} cb Optional callback, signature: err
     */
    removeIndex(fieldName, cb) {
        delete this.indexes[fieldName];
        this.persistence.persistNewState([{$$indexRemoved: fieldName}], (err) => cb?.(err || null));
    }

    /**
     * Add one or several document(s) to all indexes
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
     * Remove one or several document(s) from all indexes
     */
    removeFromIndexes(doc) {
        for (const key of Object.keys(this.indexes)) {
            this.indexes[key].remove(doc);
        }
    }

    /**
     * Update one or several documents in all indexes
     * To update multiple documents, oldDoc must be an array of { oldDoc, newDoc } pairs
     * If one update violates a constraint, all changes are rolled back
     */
    updateIndexes(oldDoc, newDoc) {
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
     * @param {Query} query
     * @param {Boolean} dontExpireStaleDocs Optional, defaults to false, if true don't remove stale docs. Useful for the remove function which shouldn't be impacted by expirations
     * @param {Function} callback Signature err, candidates
     */
    getCandidates(query, dontExpireStaleDocs, callback) {
        let indexNames = Object.keys(this.indexes);

        if (typeof dontExpireStaleDocs === 'function') {
            callback = dontExpireStaleDocs;
            dontExpireStaleDocs = false;
        }

        async.waterfall([
            // STEP 1: get candidates list by checking indexes from most to least frequent usecase
            (cb) => {
                const allQueryKeys = Object.keys(query);
                
                // For a basic match
                let usableQueryKeys = allQueryKeys.filter(key => {
                    let valType = typeof query[key];
                    return valType === 'string' || valType === 'number' || valType === 'boolean' || isDate(query[key]) || query[key] === null;
                });
                usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
                if (usableQueryKeys.length) {
                    return cb(null, this.indexes[usableQueryKeys[0]].getMatching(query[usableQueryKeys[0]]));
                }

                // For a $in match
                usableQueryKeys = allQueryKeys.filter(key => hasOwnProp(query[key], '$in'));
                usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
                if (usableQueryKeys.length) {
                    return cb(null, this.indexes[usableQueryKeys[0]].getMatching(query[usableQueryKeys[0]].$in));
                }

                // For a comparison match
                usableQueryKeys = allQueryKeys.filter(key => hasAnyOwnPropOf(query[key], '$lt', '$lte', '$gt', '$gte'));
                usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
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

                let expiredDocsIds = [], 
                    validDocs = [], 
                    ttlIndexesFieldNames = Object.keys(this.ttlIndexes),
                    now = Date.now();

                for (const doc of docs) {
                    if (ttlIndexesFieldNames.find(f => isDate(doc[f]) && now > doc[f].getTime() + this.ttlIndexes[f] * 1000)) {
                        expiredDocsIds.push(doc._id);
                    } else {
                        validDocs.push(doc);
                    }
                }

                async.eachSeries(expiredDocsIds, (_id, _cb) => this._remove({_id}, {}, _cb),
                                                 (/* err ignored */) => cb(null, validDocs));
                
            }
        ], (err, docs) => callback(err || null, docs));
    }

    /**
     * Insert a new document
     * @param {Object|Object[]} newDoc
     * @param {Function} cb Optional callback, signature: err, insertedDoc
     *
     * @api private Use Datastore.insert which has the same signature
     */
    _insert(newDoc, cb) {
        let preparedDoc,
            isDocsArray = false;

        try {
            preparedDoc = this.prepareDocumentForInsertion(newDoc);
            // add new doc/docs to the cache
            if (Array.isArray(preparedDoc)) {
                isDocsArray = true;
                this._insertMultipleDocsInCache(preparedDoc);
            } else {
                this.addToIndexes(preparedDoc);
            }
        } catch (err) {
            return cb?.(err);
        }

        this.persistence.persistNewState(isDocsArray ? preparedDoc : [preparedDoc], (err) => {
            if (err) {
                return cb?.(err);
            }
            return cb?.(null, model.deepCopy(preparedDoc));
        });
    }

    /**
     * Create a new _id that's not already in use
     * @return {string}
     */
    createNewId() {
        let id;
        // Try as many times as needed to get an unused _id. As explained in customUtils, the probability of this ever happening is extremely small, so this is O(1)
        do {
            id = generateUid(16);
        } while (this.indexes._id.getMatching(id).length);
        return id;
    }

    /**
     * Prepare a document (or array of documents) to be inserted in a database
     * Meaning adds _id and timestamps if necessary on a copy of newDoc to avoid any side effect on user input
     * @api private
     */
    prepareDocumentForInsertion(newDoc) {
        let preparedDoc;

        if (Array.isArray(newDoc)) {
            preparedDoc = newDoc.map(doc => this.prepareDocumentForInsertion(doc));
        } else {
            preparedDoc = model.deepCopy(newDoc);
            if (preparedDoc._id === undefined) {
                preparedDoc._id = this.createNewId();
            }
            let now = new Date();
            if (this.timestampData && preparedDoc.createdAt === undefined) {
                preparedDoc.createdAt = now;
            }
            if (this.timestampData && preparedDoc.updatedAt === undefined) {
                preparedDoc.updatedAt = now;
            }
            model.checkObject(preparedDoc);
        }

        return preparedDoc;
    }

    /**
     * If one insertion fails (e.g. because of a unique constraint), roll back all previous
     * inserts and throws the error
     * @param {Object[]} preparedDocs
     * @api private
     */
    _insertMultipleDocsInCache(preparedDocs) {
        let totalRevertible = 0;
        try {
            for (const doc of preparedDocs) {
                this.addToIndexes(doc);
                totalRevertible++;
            }
        } catch (err) {
            while (totalRevertible) {
                this.removeFromIndexes(preparedDocs[--totalRevertible]);
            }
            throw err;
        }
    }

    insert() {
        this.executor.push({this: this, fn: this._insert, arguments});
    }

    /**
     * Count all documents matching the query
     * @param {Object} query MongoDB-style query
     * @param {function} callback
     */
    count(query, callback) {
        let cursor = new Cursor(this, query, (err, docs, callback) => err ? callback(err) : callback(null, docs.length));

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
     * @param {function} [callback] if not given, will return cursor
     * @return {Cursor|void}
     */
    find(query, projection = {}, callback) {
        if (arguments.length === 2 && typeof projection === 'function') {
            callback = projection;
            projection = {};
        }

        let cursor = new Cursor(this, query, (err, docs, cb) => err ? cb(err) : cb(null, docs.map(doc => model.deepCopy(doc))));

        cursor.projection(projection);
        if (typeof callback === 'function') {
            cursor.exec(callback);
        } else {
            return cursor;
        }
    }

    /**
     * Find one document matching the query
     * @param {Object} query MongoDB-style query
     * @param {Object} [projection] MongoDB-style projection
     * @param {function} [callback]
     */
    findOne(query, projection = {}, callback) {
        if (arguments.length === 2 && typeof projection === 'function') {
            callback = projection;
            projection = {};
        }

        let cursor = new Cursor(this, query, (err, docs, cb) => err ? cb(err) : cb(null, docs.length === 1 ? model.deepCopy(docs[0]) : null));
        cursor.projection(projection).limit(1);
        
        if (typeof callback === 'function') {
            cursor.exec(callback);
        } else {
            return cursor;
        }
    }

    /**
     * Update all docs matching query
     * @param {Object} query
     * @param {Object} updateQuery
     * @param {Object} options Optional options
     *                 options.multi If true, can update multiple documents (defaults to false)
     *                 options.upsert If true, document is inserted if the query doesn't match anything
     *                 options.returnUpdatedDocs Defaults to false, if true return as third argument the array of updated matched documents (even if no change actually took place)
     * @param {Function} cb Optional callback, signature: (err, numAffected, affectedDocuments, upsert)
     *                      If update was an upsert, upsert flag is set to true
     *                      affectedDocuments can be one of the following:
     *                        * For an upsert, the upserted document
     *                        * For an update with returnUpdatedDocs option false, null
     *                        * For an update with returnUpdatedDocs true and multi false, the updated document
     *                        * For an update with returnUpdatedDocs true and multi true, the array of updated documents
     *
     * WARNING: The API was changed between v1.7.4 and v1.8, for consistency and readability reasons. Prior and including to v1.7.4,
     *          the callback signature was (err, numAffected, updated) where updated was the updated document in case of an upsert
     *          or the array of updated documents for an update if the returnUpdatedDocs option was true. That meant that the type of
     *          affectedDocuments in a non multi update depended on whether there was an upsert or not, leaving only two ways for the
     *          user to check whether an upsert had occured: checking the type of affectedDocuments or running another find query on
     *          the whole dataset to check its size. Both options being ugly, the breaking change was necessary.
     *
     * @api private Use Datastore.update which has the same signature
     */
    _updateOrInsert(query, updateQuery, options, callback = NOP) {
        if (typeof options === 'function') {
            callback = options;
            options = {};
        }
        
        if (!options.upsert) {
            return this._update(query, updateQuery, options, callback);
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
                model.checkObject(updateQuery);
                // updateQuery is a simple object with no modifier, use it as the document to insert
                toBeInserted = updateQuery;
            } catch (e) {
                // updateQuery contains modifiers, use the find query as the base,
                // strip it from all operators and update it according to updateQuery
                try {
                    toBeInserted = model.modify(model.deepCopy(query, true), updateQuery);
                } catch (err) {
                    return callback(err);
                }
            }

            this._insert(toBeInserted, (err, newDoc) => err ? callback(err) : callback(null, 1, newDoc, true));
        });
    }

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
                    if (model.match(oldDoc, query) && (multi || !totalReplaced)) {
                        totalReplaced++;
                        let newDoc = model.modify(oldDoc, updateQuery);
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
                this.updateIndexes(modifications);
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
                let updatedDocClones = multi ? updatedDocs.map(doc => model.deepCopy(doc)) : model.deepCopy(updatedDocs[0]);

                callback(null, totalReplaced, updatedDocClones);
            });
        });
    }
    
    update() {
        this.executor.push({this: this, fn: this._updateOrInsert, arguments});
    }

    /**
     * Remove all docs matching the query
     * For now very naive implementation (similar to update)
     * @param {Object} query
     * @param {Object} options Optional options
     *                 options.multi If true, can update multiple documents (defaults to false)
     * @param {Function} cb Optional callback, signature: err, numRemoved
     *
     * @api private Use Datastore.remove which has the same signature
     */
    _remove(query, options, cb) {
        if (typeof options === 'function') {
            cb = options;
            options = {};
        }
        
        let callback = cb || NOP;

        this.getCandidates(query, true, (err, candidates) => {
            if (err) {
                return callback(err);
            }
            let multi = !!options.multi,
                removedDocs = [],
                totalRemoved = 0;
            
            try {
                for (const d of candidates) {
                    if (model.match(d, query) && (multi || !totalRemoved)) {
                        totalRemoved++;
                        removedDocs.push({$$deleted: true, _id: d._id});
                        this.removeFromIndexes(d);
                    }
                }
            } catch (err) {
                return callback(err);
            }

            this.persistence.persistNewState(removedDocs, (err) => err ? callback(err) : callback(null, totalRemoved));
        });
    }

    remove() {
        this.executor.push({this: this, fn: this._remove, arguments});
    }
}
