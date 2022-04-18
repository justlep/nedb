/**
 * Way data is stored for this database
 * For a Node.js/Node Webkit database it's the file system
 * For a browser-side database it's localforage which chooses the best option depending on user browser (IndexedDB then WebSQL then localStorage)
 *
 * This version is the Node.js/Node Webkit version
 * It's essentially fs, mkdirp and crash safe write and read functions
 */

import {dirname} from 'path';
import {access, rename, writeFile as nativeWriteFile, unlink, open, mkdir, fsync, close} from 'fs';
import async from 'async';

const IS_WINDOWS = process.platform === 'win32' || process.platform === 'win64';

/**
 * Allows using a custom writeFile function instead of Node's native one for internal testing purposes.
 * @param {function} customWriteFileFn
 * @internal
 */
export function _overrideWriteFile(customWriteFileFn) {
    __writeFile = customWriteFileFn;
}
let __writeFile = nativeWriteFile;

/**
 * @param {string} path
 * @param {function(boolean):void} cb
 */
const fileExists = (path, cb) => access(path, err => cb(!err));

/**
 * Explicit name ...
 */
export function ensureFileDoesntExist(file, callback) {
    fileExists(file, (exists) => exists ? unlink(file, (err) => callback(err)) : callback(null));
}

const NOP = () => {};

/**
 * Check if a directory exists and create it on the fly if it is not the case
 * @param {string} dir
 * @param {function(err:boolean):void} [cb] - optional callback
 */
export function ensureDirectoryExists(dir, cb = NOP) {
    mkdir(dir, {recursive: true}, cb);
}

/**
 * Flush data in OS buffer to storage if corresponding option is set
 * @param {string|{filename: string, [isDir]: boolean}} opts - just the filename or filename+isDir object 
                * If opts is a string, it is assumed that the flush of the file (not dir) called options was requested
 * @param {string} filename
 * @param {boolean} isDir
 * @param {function(?Error):void} callback
 */
function flushToStorage(filename, isDir, callback) {
    const flags = isDir ? 'r' : 'r+';
    
    if (isDir && IS_WINDOWS) {
        // Windows can't fsync (FlushFileBuffers) directories. We can live with this as it cannot cause 100% dataloss
        // except in the very rare event of the first time database is loaded and a crash happens
        return callback(null);
    }

    open(filename, flags, (err, fd) => {
        if (err) {
            return callback(err);
        } 
        fsync(fd, (errFS) => close(fd, (errC) => {
            if (errFS || errC) {
                let e = new Error('Failed to flush to storage');
                e.errorOnFsync = errFS;
                e.errorOnClose = errC;
                return callback(e);
            }
            return callback(null);
        }));
    });
}

/**
 * Fully write or rewrite the datafile, immune to crashes during the write operation (data will not be lost)
 * @param {String} filename
 * @param {String} data
 * @param {Function} callback Optional callback, signature: err
 */
export function crashSafeWriteFile(filename, data, callback) {
    let tempFilename = filename + '~';

    async.waterfall([
        (cb) => flushToStorage(dirname(filename), true, cb),
        (cb) => fileExists(filename, (exists) => exists ? flushToStorage(filename, false, cb) : cb()),
        (cb) => __writeFile(tempFilename, data, cb),
        (cb) => flushToStorage(tempFilename, false, cb),
        (cb) => rename(tempFilename, filename, cb),
        (cb) => flushToStorage(dirname(filename), true, cb)
    ], (err) => callback?.(err));
}


/**
 * Ensure the datafile contains all the data, even if there was a crash during a full file write
 * @param {String} filename
 * @param {Function} callback signature: err
 */
export function ensureDatafileIntegrity(filename, callback) {
    let tempFilename = filename + '~';

    fileExists(filename, function (filenameExists) {
        // Write was successful
        if (filenameExists) {
            return callback(null);
        }

        fileExists(tempFilename, function (oldFilenameExists) {
            // New database
            if (!oldFilenameExists) {
                return __writeFile(filename, '', 'utf8', function (err) {
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
