import async from 'async';
import * as commonUtilities from './commonUtilities.js';
import {getConfiguration} from './commonUtilities.js';

const DB_FILENAME = 'workspace/ensureIndex.bench.db';

let {d, n, profiler} = getConfiguration(DB_FILENAME, 'ENSUREINDEX BENCH');


console.log('----------------------------');
console.log('Test with ' + n + ' documents');
console.log('----------------------------');

async.waterfall([
    async.apply(commonUtilities.prepareDb, DB_FILENAME), function (cb) {
        d.loadDatabase(function (err) {
            if (err) {
                return cb(err);
            }
            cb();
        });
    },
    function (cb) {
        profiler.beginProfiling();
        return cb();
    },
    async.apply(commonUtilities.insertDocs, d, n, profiler),
    function (cb) {
        profiler.step('Begin calling ensureIndex ' + n + ' times');

        for (let i = 0; i < n; i++) {
            d.ensureIndex({fieldName: 'docNumber'});
            delete d.indexes.docNumber;
        }

        console.log('Average time for one ensureIndex: ' + (profiler.elapsedSinceLastStep() / n) + 'ms');
        profiler.step('Finished calling ensureIndex ' + n + ' times');
    }
], function (err) {
    profiler.step('Benchmark finished');

    if (err) {
        return console.log('An error was encountered: ', err);
    }
});
