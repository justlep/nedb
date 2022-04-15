/**
 * Way data is stored for this database
 * For a Node.js/Node Webkit database it's the file system
 * For a browser-side database it's localforage which chooses the best option depending on user browser (IndexedDB then WebSQL then localStorage)
 *
 * This version is the Node.js/Node Webkit version
 * It's essentially fs, mkdirp and crash safe write and read functions
 */

import {dirname} from 'path';
import {exists, rename, writeFile as writeFileNative, unlink, appendFile, readFile, open, mkdir, fsync, close} from 'fs';
import async from 'async';

export {exists, rename, unlink, appendFile, readFile};

export const mkdirp = (path, cb) => mkdir(path, {recursive: true}, cb);

export let writeFile = writeFileNative;

/**
 * Allows using a custom writeFile function instead of Node's native one for internal testing purposes.  
 * @param {function} customWriteFileFn
 * @internal
 */
export const _overrideWriteFile = (customWriteFileFn) => writeFile = customWriteFileFn;

const NOP = () => {};

/**
 * Explicit name ...
 */
export function ensureFileDoesntExist(file, callback) {
    exists(file, function (exists) {
        if (!exists) {
            return callback(null);
        }

        unlink(file, function (err) {
            return callback(err);
        });
    });
}


/**
 * Flush data in OS buffer to storage if corresponding option is set
 * @param {String} options.filename
 * @param {Boolean} options.isDir Optional, defaults to false
 * If options is a string, it is assumed that the flush of the file (not dir) called options was requested
 */
export function flushToStorage(options, callback) {
    let filename, flags;
    if (typeof options === 'string') {
        filename = options;
        flags = 'r+';
    } else {
        filename = options.filename;
        flags = options.isDir ? 'r' : 'r+';
    }

    // Windows can't fsync (FlushFileBuffers) directories. We can live with this as it cannot cause 100% dataloss
    // except in the very rare event of the first time database is loaded and a crash happens
    if (flags === 'r' && (process.platform === 'win32' || process.platform === 'win64')) {
        return callback(null);
    }

    open(filename, flags, function (err, fd) {
        if (err) {
            return callback(err);
        }
        fsync(fd, function (errFS) {
            close(fd, function (errC) {
                if (errFS || errC) {
                    let e = new Error('Failed to flush to storage');
                    e.errorOnFsync = errFS;
                    e.errorOnClose = errC;
                    return callback(e);
                } 
                return callback(null);
            });
        });
    });
}


/**
 * Fully write or rewrite the datafile, immune to crashes during the write operation (data will not be lost)
 * @param {String} filename
 * @param {String} data
 * @param {Function} cb Optional callback, signature: err
 */
export function crashSafeWriteFile(filename, data, cb) {
    let callback = cb || NOP,
        tempFilename = filename + '~';

    async.waterfall([
        async.apply(flushToStorage, {filename: dirname(filename), isDir: true}),
        function (cb) {
            exists(filename, function (exists) {
                if (exists) {
                    flushToStorage(filename, function (err) {
                        return cb(err);
                    });
                } else {
                    return cb();
                }
            });
        },
        function (cb) {
            writeFile(tempFilename, data, function (err) {
                return cb(err);
            });
        },
        async.apply(flushToStorage, tempFilename),
        function (cb) {
            rename(tempFilename, filename, function (err) {
                return cb(err);
            });
        },
        async.apply(flushToStorage, {filename: dirname(filename), isDir: true})
    ], function (err) {
        return callback(err);
    });
}


/**
 * Ensure the datafile contains all the data, even if there was a crash during a full file write
 * @param {String} filename
 * @param {Function} callback signature: err
 */
export function ensureDatafileIntegrity(filename, callback) {
    let tempFilename = filename + '~';

    exists(filename, function (filenameExists) {
        // Write was successful
        if (filenameExists) {
            return callback(null);
        }

        exists(tempFilename, function (oldFilenameExists) {
            // New database
            if (!oldFilenameExists) {
                return writeFile(filename, '', 'utf8', function (err) {
                    callback(err);
                });
            }

            // Write failed, use old version
            rename(tempFilename, filename, function (err) {
                return callback(err);
            });
        });
    });
}
