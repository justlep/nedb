import async from 'async';
import * as commonUtilities from './commonUtilities.js';
import {getConfiguration} from './commonUtilities.js';


let {d, n, profiler, withIndex, dbFilePath} = getConfiguration('remove');

async.waterfall([
    async.apply(commonUtilities.prepareDb, dbFilePath), function (cb) {
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

    // Test with remove only one document
    function (cb) {
        profiler.step('MULTI: FALSE');
        return cb();
    },
    async.apply(commonUtilities.removeDocs, {multi: false}, d, n, profiler),
    
    // Test with multiple documents
    function (cb) {
        d.remove({}, {multi: true}, function () {
            return cb();
        });
    },
    async.apply(commonUtilities.insertDocs, d, n, profiler),
    function (cb) {
        profiler.step('MULTI: TRUE');
        return cb();
    },
    async.apply(commonUtilities.removeDocs, {multi: true}, d, n, profiler)
    
], function (err) {
    profiler.step('Benchmark finished');

    if (err) {
        return console.log('An error was encountered: ', err);
    }
});
