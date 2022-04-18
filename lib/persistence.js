import {dirname} from 'path';
import * as model from './model.js';
import {Index} from './indexes.js';
import {generateUid} from './customUtils.js';
import {appendFile, readFile} from 'fs';
import {crashSafeWriteFile, ensureDatafileIntegrity, mkdirp} from './storage.js';

const NOP = () => {};

export class Persistence {
    /**
     * Check if a directory exists and create it on the fly if it is not the case
     * cb is optional, signature: err
     */
    static ensureDirectoryExists(dir, cb) {
        mkdirp(dir, cb || NOP);
    }

    /**
     * @param {Object} options
     * @param {Datastore} options.db
     */
    constructor(options) {
        let i, j, randomString;

        this.db = options.db;
        this.inMemoryOnly = this.db.inMemoryOnly;
        this.filename = this.db.filename;
        this.corruptAlertThreshold = options.corruptAlertThreshold !== undefined ? options.corruptAlertThreshold : 0.1;

        if (!this.inMemoryOnly && this.filename && this.filename.charAt(this.filename.length - 1) === '~') {
            throw new Error('The datafile name can\'t end with a ~, which is reserved for crash safe backup files');
        }

        // After serialization and before deserialization hooks with some basic sanity checks
        if (options.afterSerialization && !options.beforeDeserialization) {
            throw new Error('Serialization hook defined but deserialization hook undefined, cautiously refusing to start NeDB to prevent dataloss');
        }
        if (!options.afterSerialization && options.beforeDeserialization) {
            throw new Error('Serialization hook undefined but deserialization hook defined, cautiously refusing to start NeDB to prevent dataloss');
        }
        this.afterSerialization = options.afterSerialization || function (s) {
            return s;
        };
        this.beforeDeserialization = options.beforeDeserialization || function (s) {
            return s;
        };
        for (i = 1; i < 30; i += 1) {
            for (j = 0; j < 10; j += 1) {
                randomString = generateUid(i);
                if (this.beforeDeserialization(this.afterSerialization(randomString)) !== randomString) {
                    throw new Error('beforeDeserialization is not the reverse of afterSerialization, cautiously refusing to start NeDB to prevent dataloss');
                }
            }
        }
    }

    /**
     * Persist cached database
     * This serves as a compaction function since the cache always contains only the number of documents in the collection
     * while the data file is append-only so it may grow larger
     * @param {Function} cb Optional callback, signature: err
     */
    persistCachedDatabase(cb) {
        let callback = cb || NOP,
            toPersist = '',
            self = this;

        if (this.inMemoryOnly) {
            return callback(null);
        }

        this.db.getAllData().forEach(function (doc) {
            toPersist += self.afterSerialization(model.serialize(doc)) + '\n';
        });
        Object.keys(this.db.indexes).forEach(function (fieldName) {
            if (fieldName !== '_id') {   // The special _id index is managed by datastore.js, the others need to be persisted
                toPersist += self.afterSerialization(model.serialize({
                    $$indexCreated: {
                        fieldName: fieldName,
                        unique: self.db.indexes[fieldName].unique,
                        sparse: self.db.indexes[fieldName].sparse
                    }
                })) + '\n';
            }
        });

        crashSafeWriteFile(this.filename, toPersist, function (err) {
            if (err) {
                return callback(err);
            }
            self.db.emit('compaction.done');
            return callback(null);
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
        let minInterval = 5000,
            realInterval = Math.max(interval || 0, minInterval);

        this.stopAutocompaction();

        this.autocompactionIntervalId = setInterval(() => this.compactDatafile(), realInterval);
    }

    /**
     * Stop autocompaction (do nothing if autocompaction was not running)
     */
    stopAutocompaction() {
        if (this.autocompactionIntervalId) {
            clearInterval(this.autocompactionIntervalId);
        }
    }

    /**
     * Persist new state for the given newDocs (can be insertion, update or removal)
     * Use an append-only format
     * @param {Array} newDocs Can be empty if no doc was updated/removed
     * @param {Function} cb Optional, signature: err
     */
    persistNewState(newDocs, cb) {
        let self = this,
            toPersist = '',
            callback = cb || NOP;

        // In-memory only datastore
        if (self.inMemoryOnly) {
            return callback(null);
        }

        newDocs.forEach(function (doc) {
            toPersist += self.afterSerialization(model.serialize(doc)) + '\n';
        });

        if (toPersist.length === 0) {
            return callback(null);
        }

        appendFile(self.filename, toPersist, 'utf8', function (err) {
            return callback(err);
        });
    }

    /**
     * From a database's raw data, return the corresponding
     * machine understandable collection
     */
    treatRawData(rawData) {
        let data = rawData.split('\n'),
            dataById = {},
            tdata = [],
            i,
            indexes = {},
            corruptItems = -1;   // Last line of every data file is usually blank so not really corrupt

        for (i = 0; i < data.length; i += 1) {
            let doc;

            try {
                doc = model.deserialize(this.beforeDeserialization(data[i]));
                if (doc._id) {
                    if (doc.$$deleted === true) {
                        delete dataById[doc._id];
                    } else {
                        dataById[doc._id] = doc;
                    }
                } else if (doc.$$indexCreated && doc.$$indexCreated.fieldName !== undefined) {
                    indexes[doc.$$indexCreated.fieldName] = doc.$$indexCreated;
                } else if (typeof doc.$$indexRemoved === 'string') {
                    delete indexes[doc.$$indexRemoved];
                }
            } catch (e) {
                corruptItems += 1;
            }
        }

        // A bit lenient on corruption
        if (data.length > 0 && corruptItems / data.length > this.corruptAlertThreshold) {
            throw new Error('More than ' + Math.floor(100 * this.corruptAlertThreshold) + '% of the data file is corrupt, the wrong beforeDeserialization hook may be used. Cautiously refusing to start NeDB to prevent dataloss');
        }

        Object.keys(dataById).forEach(function (k) {
            tdata.push(dataById[k]);
        });

        return {data: tdata, indexes: indexes};
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
        Persistence.ensureDirectoryExists(dirname(this.filename), (err) => {
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
