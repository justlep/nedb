import async from 'async';
import * as commonUtilities from './commonUtilities.js';
import {getConfiguration} from './commonUtilities.js';


let {d, n, profiler, dbFilePath} = getConfiguration('loadDb');

async.waterfall([
    async.apply(commonUtilities.prepareDb, dbFilePath),
    (cb) => d.loadDatabase(cb),
    (cb) => {
        profiler.beginProfiling();
        cb();
    },
    async.apply(commonUtilities.insertDocs, d, n, profiler),
    async.apply(commonUtilities.loadDatabase, d, n, profiler)
], (err) => {
    profiler.step('Benchmark finished');
    if (err) {
        return console.log('An error was encountered: ', err);
    }
});
