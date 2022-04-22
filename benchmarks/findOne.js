import async from 'async';
import * as commonUtilities from './commonUtilities.js';
import {getConfiguration} from './commonUtilities.js';

const DB_FILENAME = 'workspace/findOne.bench.db';

let {d, n, profiler, withIndex} = getConfiguration(DB_FILENAME, 'FINDONE BENCH');


async.waterfall([
    async.apply(commonUtilities.prepareDb, DB_FILENAME), function (cb) {
        d.loadDatabase(function (err) {
            if (err) {
                return cb(err);
            }
            if (withIndex) {
                d.ensureIndex({fieldName: 'docNumber'});
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
        setTimeout(function () {
            cb();
        }, 500);
    },
    async.apply(commonUtilities.findOneDocs, d, n, profiler)
], function (err) {
    profiler.step('Benchmark finished');

    if (err) {
        return console.log('An error was encountered: ', err);
    }
});
