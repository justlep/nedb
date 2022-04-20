import {dirname} from 'path';
import {Index} from './indexes.js';
import {generateUid} from './customUtils.js';
import {appendFile, readFile} from 'fs';
import {crashSafeWriteFile, ensureDatafileIntegrity, ensureDirectoryExists} from './storage.js';
import {deserialize, serialize} from './model.js';

const NOP = () => {};

export class Persistence {

    /**
     * @param {Datastore} db
     * @param {number} [corruptAlertThreshold]
     * @param {function} [afterSerialization]
     * @param {function} [beforeDeserialization]
     */
    constructor({db, corruptAlertThreshold, afterSerialization, beforeDeserialization}) {
        this.db = db;
        this.inMemoryOnly = this.db.inMemoryOnly;
        this.filename = this.db.filename;
        this.corruptAlertThreshold = corruptAlertThreshold ?? 0.1;

        if (!this.inMemoryOnly && this.filename?.endsWith('~')) {
            throw new Error('The datafile name can\'t end with a ~, which is reserved for crash safe backup files');
        }
        
        if (afterSerialization || beforeDeserialization) {
            if (!afterSerialization !== !beforeDeserialization) {
                throw new Error('Custom serialization + deserialization hook must be defined together or not at all, ' +
                                'cautiously refusing to start NeDB to prevent data-loss');
            }
            
            // check round-trip with custom (de)serializer 
            for (let i = 1; i < 30; i++) {
                for (let j = 0; j < 10; j++) {
                    let randomString = generateUid(i);
                    if (beforeDeserialization(afterSerialization(randomString)) !== randomString) {
                        throw new Error('beforeDeserialization is not the reverse of afterSerialization, cautiously refusing to start NeDB to prevent dataloss');
                    }
                }
            }
        }

        this.afterSerialization = afterSerialization || (s => s);
        this.beforeDeserialization = beforeDeserialization || (s => s);
    }

    /**
     * Persist cached database
     * This serves as a compaction function since the cache always contains only the number of documents in the collection
     * while the data file is append-only so it may grow larger
     * @param {Function} [cb] Optional callback, signature: err
     */
    persistCachedDatabase(cb = NOP) {
        if (this.inMemoryOnly) {
            return cb(null);
        }

        let toPersist = '';
        
        for (const doc of this.db.getAllData()) {
            toPersist += this.afterSerialization(serialize(doc)) + '\n';
        }
        
        for (const fieldName of Object.keys(this.db.indexes)) {
            if (fieldName !== '_id') {   // The special _id index is managed by datastore.js, the others need to be persisted
                let {unique, sparse} = this.db.indexes[fieldName];
                toPersist += this.afterSerialization(serialize({$$indexCreated: {fieldName, unique, sparse}})) + '\n';
            }
        }

        crashSafeWriteFile(this.filename, toPersist, (err) => {
            if (err) {
                return cb(err);
            }
            this.db.emit('compaction.done');
            return cb(null);
        });
    }

    /**
     * Queue a rewrite of the datafile
     */
    compactDatafile() {
        this.db.executor.push({this: this, fn: this.persistCachedDatabase, arguments: []});
    }

    /**
     * Set automatic compaction every interval ms
     * @param {Number} interval in milliseconds, with an enforced minimum of 5 seconds
     */
    setAutocompactionInterval(interval) {
        let realInterval = Math.max(5000, interval || 0);
        this.stopAutocompaction();
        this.autocompactionIntervalId = setInterval(() => this.compactDatafile(), realInterval);
    }

    /**
     * Stop autocompaction (do nothing if autocompaction was not running)
     */
    stopAutocompaction() {
        if (this.autocompactionIntervalId) {
            this.autocompactionIntervalId = clearInterval(this.autocompactionIntervalId);
        }
    }

    /**
     * Persist new state for the given newDocs (can be insertion, update or removal)
     * Use an append-only format
     * @param {Object[]} newDocs Can be empty if no doc was updated/removed
     * @param {?Function} [callback] Optional, signature: err
     */
    persistNewState(newDocs, callback) {
        // In-memory only datastore
        if (this.inMemoryOnly) {
            return callback?.(null);
        }
        let toPersist = '';
        for (const doc of newDocs) {
            toPersist += this.afterSerialization(serialize(doc)) + '\n';
        }
        if (!toPersist.length) {
            return callback?.(null);
        }
        appendFile(this.filename, toPersist, 'utf8', (err) => callback?.(err));
    }

    /**
     * From a database's raw data, return the corresponding
     * machine understandable collection
     * @param {string} rawData
     */
    treatRawData(rawData) {
        let lines = rawData.split('\n'),
            dataById = new Map(),
            indexes = Object.create(null),
            totalCorrupt = -1;   // Last line of every data file is usually blank so not really corrupt

        for (const line of lines) {
            try {
                let doc = deserialize(this.beforeDeserialization(line));
                if (doc._id) {
                    if (doc.$$deleted === true) {
                        dataById.delete(doc._id);
                    } else {
                        dataById.set(doc._id, doc);
                    }
                } else if (doc.$$indexCreated?.fieldName !== undefined) {
                    indexes[doc.$$indexCreated.fieldName] = doc.$$indexCreated;
                } else if (typeof doc.$$indexRemoved === 'string') {
                    delete indexes[doc.$$indexRemoved];
                }
            } catch (e) {
                totalCorrupt++;
            }
        }

        // A bit lenient on corruption
        if (lines.length && (totalCorrupt / lines.length) > this.corruptAlertThreshold) {
            throw new Error('More than ' + Math.floor(100 * this.corruptAlertThreshold) + '% of the data file is corrupt, ' +
                            'the wrong beforeDeserialization hook may be used. Cautiously refusing to start NeDB to prevent dataloss');
        }

        return {data: [...dataById.values()], indexes};
    }

    /**
     * Load the database
     * 1) Create all indexes
     * 2) Insert all data
     * 3) Compact the database
     * This means pulling data out of the data file or creating it if it doesn't exist
     * Also, all data is persisted right away, which has the effect of compacting the database file
     * This operation is very quick at startup for a big collection (60ms for ~10k docs)
     * @param {Function} [callback] Optional callback, signature: err
     */
    loadDatabase(cb = NOP) {
        this.db.resetIndexes();

        // In-memory only datastore
        if (this.inMemoryOnly) {
            return cb(null);
        }
        
        // should do this in waterfall, but hey...
        ensureDirectoryExists(dirname(this.filename), (err) => {
            if (err) {
                return cb(err);
            }
            ensureDatafileIntegrity(this.filename, (err) => err ? cb(err) : readFile(this.filename, 'utf8', (err, rawData) => {
                if (err) {
                    return cb(err);
                }
                let treatedData;

                try {
                    treatedData = this.treatRawData(rawData);
                } catch (e) {
                    return cb(e);
                }

                // Recreate all indexes in the datafile
                for (const key of Object.keys(treatedData.indexes)) {
                    this.db.indexes[key] = new Index(treatedData.indexes[key]);
                }

                // Fill cached database (i.e. all indexes) with data
                try {
                    this.db.resetIndexes(treatedData.data);
                } catch (e) {
                    this.db.resetIndexes();   // Rollback any index which didn't fail
                    return cb(e);
                }

                this.db.persistence.persistCachedDatabase(err => {
                    if (!err) {
                        this.db.executor.processBuffer();
                    }
                    cb(err || null);
                });
            }));
        });
    }
}
