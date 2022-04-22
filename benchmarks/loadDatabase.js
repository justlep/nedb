import async from 'async';
import * as commonUtilities from './commonUtilities.js';
import {getConfiguration} from './commonUtilities.js';

const DB_FILENAME = 'workspace/loaddb.bench.db';

let {d, n, profiler} = getConfiguration(DB_FILENAME, 'LOADDB BENCH');

async.waterfall([
    async.apply(commonUtilities.prepareDb, DB_FILENAME),
    (cb) => d.loadDatabase(cb),
    (cb) => {
        profiler.beginProfiling();
        return cb();
    },
    async.apply(commonUtilities.insertDocs, d, n, profiler),
    async.apply(commonUtilities.loadDatabase, d, n, profiler)
], (err) => {
    profiler.step('Benchmark finished');
    if (err) {
        return console.log('An error was encountered: ', err);
    }
});
