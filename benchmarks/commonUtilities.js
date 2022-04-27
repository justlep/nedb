import fs from 'fs';
import path from 'path';
import {Datastore} from '../index.js';
import {ensureDirectoryExists, fileExists} from '../lib/storage.js';
import {program} from 'commander';
import execTime from 'exec-time';
import {resolveProjectPath} from '../test/utils.js';

// to be used in waterfall for pausing it
export const pause500 = cb => setTimeout(cb, 500);

/**
 * Functions that are used in several benchmark tests
 */

/**
 * Configure the benchmark
 */
export function getConfiguration(configName) {
    if (arguments.length !== 1 || !/^[a-z]+$/i.test(configName)) {
        throw new Error('Invalid config name: ' + configName);
    }
    const dbFilePath = resolveProjectPath(`benchmarks/workspace/${configName}.bench.db`);
    program
        .option('-n --number [number]', 'Size of the collection to test on', parseInt)
        .option('-i --with-index', 'Use an index')
        .option('-m --in-memory', 'Test with an in-memory only store')
        .parse(process.argv);

    const {inMemory = false, withIndex = false, number = 10000} = program.opts();

    console.log('----------------------------');
    console.log('Test with ' + number + ' documents');
    console.log(`${withIndex ? 'With index' : 'Without index'}`);
    console.log(inMemory ? 'Use an in-memory datastore' : 'Use a persistent datastore');
    console.log('----------------------------');

    return {
        d: new Datastore({filename: dbFilePath, inMemoryOnly: inMemory}),
        profiler: new execTime(`${configName.toUpperCase()} BENCH`), 
        inMemory,
        withIndex,
        dbFilePath,
        n: number,
        number
    };
}


/**
 * Ensure the workspace exists and the db datafile is empty
 */
export function prepareDb(filename, cb) {
    ensureDirectoryExists(path.dirname(filename), function () {
        fileExists(filename, exists => exists ? fs.unlink(filename, cb) : cb());
    });
}


/**
 * Return an array with the numbers from 0 to n-1, in a random order
 * Uses Fisher Yates algorithm
 * Useful to get fair tests
 */
export function getRandomArray(n) {
    let res = [];
    for (let i = 0; i < n; i++) {
        res[i] = i;
    }
    for (let i = n - 1, j, temp; i >= 1; i--) {
        j = Math.floor((i + 1) * Math.random());
        temp = res[i];
        res[i] = res[j];
        res[j] = temp;
    }
    return res;
}


/**
 * Insert a certain number of documents for testing
 */
export function insertDocs(d, n, profiler, cb) {
    const order = getRandomArray(n);

    profiler.step('Begin inserting ' + n + ' docs');

    function runFrom(i) {
        if (i === n) {   // Finished
            let opsPerSecond = Math.floor(1000 * n / profiler.elapsedSinceLastStep());
            console.log('===== RESULT (insert) ===== ' + opsPerSecond + ' ops/s');
            profiler.step('Finished inserting ' + n + ' docs');
            profiler.insertOpsPerSecond = opsPerSecond;
            return cb();
        }

        d.insert({docNumber: order[i]}, err => runFrom(i + 1));  // rather recurse than having inconsistent delays with setImmediate 
    }

    runFrom(0);
}


/**
 * Find documents with find
 */
export function findDocs(d, n, profiler, cb) {
    const order = getRandomArray(n);

    profiler.step('Finding ' + n + ' documents');

    function runFrom(i) {
        if (i === n) {   // Finished
            console.log('===== RESULT (find) ===== ' + Math.floor(1000 * n / profiler.elapsedSinceLastStep()) + ' ops/s');
            profiler.step('Finished finding ' + n + ' docs');
            return cb();
        }

        d.find({docNumber: order[i]}, function (err, docs) {
            if (docs.length !== 1 || docs[0].docNumber !== order[i]) {
                return cb('One find didnt work');
            }
            runFrom(i + 1); // rather recurse than having inconsistent delays with setImmediate 
        });
    }

    runFrom(0);
}


/**
 * Find documents with find and the $in operator
 */
export function findDocsWithIn(d, n, profiler, cb) {
    const ins = [];
    const arraySize = Math.min(10, n);   // The array for $in needs to be smaller than n (inclusive)

    // Preparing all the $in arrays, will take some time
    for (let i = 0; i < n; i++) {
        ins[i] = [];
        for (let j = 0; j < arraySize; j++) {
            ins[i].push((i + j) % n);
        }
    }

    profiler.step('Finding ' + n + ' documents WITH $IN OPERATOR');

    function runFrom(i) {
        if (i === n) {   // Finished
            console.log('===== RESULT (find with in selector) ===== ' + Math.floor(1000 * n / profiler.elapsedSinceLastStep()) + ' ops/s');
            profiler.step('Finished finding ' + n + ' docs');
            return cb();
        }

        d.find({docNumber: {$in: ins[i]}}, function (err, docs) {
            if (docs.length !== arraySize) {
                return cb('One find didnt work');
            }
            runFrom(i + 1); // rather recurse than having inconsistent delays with setImmediate 
        });
    }

    runFrom(0);
}


/**
 * Find documents with findOne
 */
export function findOneDoc(d, n, profiler, cb) {
    const order = getRandomArray(n);

    profiler.step('FindingOne ' + n + ' documents');

    function runFrom(i) {
        if (i === n) {   // Finished
            console.log('===== RESULT (findOne) ===== ' + Math.floor(1000 * n / profiler.elapsedSinceLastStep()) + ' ops/s');
            profiler.step('Finished finding ' + n + ' docs');
            return cb();
        }

        d.findOne({docNumber: order[i]}, function (err, doc) {
            if (!doc || doc.docNumber !== order[i]) {
                return cb('One find didnt work');
            }
            runFrom(i + 1);  // rather recurse than having inconsistent delays with setImmediate 
        });
    }

    runFrom(0);
}

/**
 * Find documents with findOne
 */
export function findOneDocById(d, n, profiler, cb) {
    const allIds = d.getAllData().map(doc => doc._id);

    profiler.step('FindingOne ' + n + ' documents');

    function runFrom(i) {
        if (i === n) {   // Finished
            console.log('===== RESULT (findOne) ===== ' + Math.floor(1000 * n / profiler.elapsedSinceLastStep()) + ' ops/s');
            profiler.step('Finished finding ' + n + ' docs');
            return cb();
        }

        d.findOne({_id: allIds[i]}, function (err, doc) {
            if (!doc || doc._id !== allIds[i]) {
                return cb('One find didnt work');
            }
            runFrom(i + 1); // rather recurse than having inconsistent delays with setImmediate 
        });
    }

    runFrom(0);
}

/**
 * Update documents
 * options is the same as the options object for update
 */
export function updateDocs(options, d, n, profiler, cb) {
    const order = getRandomArray(n);

    profiler.step('Updating ' + n + ' documents');

    function runFrom(i) {
        if (i === n) {   // Finished
            console.log('===== RESULT (update) ===== ' + Math.floor(1000 * n / profiler.elapsedSinceLastStep()) + ' ops/s');
            profiler.step('Finished updating ' + n + ' docs');
            return cb();
        }

        // Will not actually modify the document but will take the same time
        d.update({docNumber: order[i]}, {docNumber: order[i]}, options, function (err, nr) {
            if (err) {
                return cb(err);
            }
            if (nr !== 1) {
                return cb('One update didnt work');
            }
            runFrom(i + 1); // rather recurse than having inconsistent delays with setImmediate 
        });
    }

    runFrom(0);
}


/**
 * Remove documents
 * options is the same as the options object for update
 */
export function removeDocs(options, d, n, profiler, cb) {
    const order = getRandomArray(n);

    profiler.step('Removing ' + n + ' documents');

    function runFrom(i) {
        if (i === n) {   // Finished
            // opsPerSecond corresponds to 1 insert + 1 remove, needed to keep collection size at 10,000
            // We need to subtract the time taken by one insert to get the time actually taken by one remove
            let opsPerSecond = Math.floor(1000 * n / profiler.elapsedSinceLastStep());
            let removeOpsPerSecond = Math.floor(1 / ((1 / opsPerSecond) - (1 / profiler.insertOpsPerSecond)));
            console.log('===== RESULT (remove) ===== ' + removeOpsPerSecond + ' ops/s');
            profiler.step('Finished removing ' + n + ' docs');
            return cb();
        }

        d.remove({docNumber: order[i]}, options, function (err, nr) {
            if (err) {
                return cb(err);
            }
            if (nr !== 1) {
                return cb('One remove didnt work');
            }
            d.insert({docNumber: order[i]}, function (err) {   // We need to reinsert the doc so that we keep the collection's size at n
                // So actually we're calculating the average time taken by one insert + one remove
                runFrom(i + 1); // rather recurse than having inconsistent delays with setImmediate 
            });
        });
    }

    runFrom(0);
}


/**
 * Load database
 */
export function loadDatabase(d, n, profiler, cb) {
    profiler.step('Loading the database ' + n + ' times');

    function runFrom(i) {
        if (i === n) {   // Finished
            console.log('===== RESULT ===== ' + Math.floor(1000 * n / profiler.elapsedSinceLastStep()) + ' ops/s');
            profiler.step('Finished loading a database' + n + ' times');
            return cb();
        }

        d.loadDatabase(function (err) {
            runFrom(i + 1); // rather recurse than having inconsistent delays with setImmediate 
        });
    }

    runFrom(0);
}
