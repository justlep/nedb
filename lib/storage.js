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
 * @param {string} path
 * @param {function(?Error):void} cb
 */
export const mkdirp = (path, cb) => mkdir(path, {recursive: true}, cb);

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

/**
 * Flush data in OS buffer to storage if corresponding option is set
 * @param {string|{filename: string, [isDir]: boolean}} opts - just the filename or filename+isDir object 
                * If opts is a string, it is assumed that the flush of the file (not dir) called options was requested
 */
function flushToStorage(opts, callback) {
    let filename, 
        flags = 'r+';
    
    if (typeof opts === 'string') {
        filename = opts;
    } else {
        filename = opts.filename;
        if (opts.isDir) {
            flags = 'r';
            // Windows can't fsync (FlushFileBuffers) directories. We can live with this as it cannot cause 100% dataloss
            // except in the very rare event of the first time database is loaded and a crash happens
            if (IS_WINDOWS) {
                return callback(null);
            }
        }
    }

    open(filename, flags, (err, fd) => err ? callback(err) : fsync(fd, (errFS) => close(fd, (errC) => {
        if (errFS || errC) {
            let e = new Error('Failed to flush to storage');
            e.errorOnFsync = errFS;
            e.errorOnClose = errC;
            return callback(e);
        } 
        return callback(null);
    })));
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
        (cb) => flushToStorage({filename: dirname(filename), isDir: true}, err => cb(err || null)),
        (cb) => fileExists(filename, (exists) => exists ? flushToStorage(filename, err => cb(err || null)) : cb()),
        (cb) => __writeFile(tempFilename, data, err => cb(err || null)),
        (cb) => flushToStorage(tempFilename, err => cb(err || null)),
        (cb) => rename(tempFilename, filename, err => cb(err || null)),
        (cb) => flushToStorage({filename: dirname(filename), isDir: true}, err => cb(err || null))
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
