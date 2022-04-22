import async from 'async';
import * as commonUtilities from './commonUtilities.js';
import {getConfiguration} from './commonUtilities.js';


let {d, n, profiler, withIndex, dbFilePath} = getConfiguration('insert');

async.waterfall([
    async.apply(commonUtilities.prepareDb, dbFilePath), (cb) => {
        d.loadDatabase(function (err) {
            if (err) {
                return cb(err);
            }
            if (withIndex) {
                d.ensureIndex({fieldName: 'docNumber'});
                n = 2 * n;   // We will actually insert twice as many documents
                             // because the index is slower when the collection is already
                             // big. So the result given by the algorithm will be a bit worse than
                             // actual performance
            }
            cb();
        });
    },
    function (cb) {
        profiler.beginProfiling();
        return cb();
    },
    async.apply(commonUtilities.insertDocs, d, n, profiler)
    
], function (err) {
    profiler.step('Benchmark finished');
    if (err) {
        return console.log('An error was encountered: ', err);
    }
});
