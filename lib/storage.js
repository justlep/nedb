import {dirname} from 'path';
import {access, rename, writeFile as nativeWriteFile, unlink, open, mkdir, fsync, close} from 'fs';
import {legacyWaterfall} from './async-legacy.js';

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
export const fileExists = (path, cb) => access(path, err => cb(!err));

/**
 * Explicit name ...
 * @param {string} filePath
 * @param {NedbErrorCallback} callback - invoked with {@code null} if the file was deleted or didn't exist,
 *                                        otherwise with an Error
 */
export function ensureFileDoesntExist(filePath, callback) {
    fileExists(filePath, (exists) => exists ? unlink(filePath, (err) => callback(err)) : callback(null));
}

const NOP = () => {};

/**
 * Check if a directory exists and create it on the fly if it is not the case
 * @param {string} dir
 * @param {NedbErrorCallback} [cb] 
 */
export function ensureDirectoryExists(dir, cb = NOP) {
    mkdir(dir, {recursive: true}, cb);
}

/**
 * Flush data in OS buffer to storage if corresponding option is set
 * @param {string} filePath
 * @param {boolean} isDir
 * @param {NedbErrorCallback} callback
 */
function flushToStorage(filePath, isDir, callback) {
    const flags = isDir ? 'r' : 'r+';
    
    if (isDir && IS_WINDOWS) {
        // Windows can't fsync (FlushFileBuffers) directories. We can live with this as it cannot cause 100% dataloss
        // except in the very rare event of the first time database is loaded and a crash happens
        return callback(null);
    }

    open(filePath, flags, (err, fd) => {
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
 * @param {NedbErrorCallback} callback
 */
export function crashSafeWriteFile(filename, data, callback = NOP) {
    let tempFilename = filename + '~';

    legacyWaterfall([
        (cb) => flushToStorage(dirname(filename), true, cb),
        (cb) => fileExists(filename, (exists) => exists ? flushToStorage(filename, false, cb) : cb()),
        (cb) => __writeFile(tempFilename, data, cb),
        (cb) => flushToStorage(tempFilename, false, cb),
        (cb) => rename(tempFilename, filename, cb),
        (cb) => flushToStorage(dirname(filename), true, cb)
    ], callback);
}


/**
 * Ensure the datafile contains all the data, even if there was a crash during a full file write
 * @param {String} filePath
 * @param {NedbErrorCallback} callback
 */
export function ensureDatafileIntegrity(filePath, callback) {
    let tempFilename = filePath + '~';

    fileExists(filePath, function (exists) {
        if (exists) {
            // Write was successful
            return callback(null);
        }

        fileExists(tempFilename, function (tempFileExists) {
            if (!tempFileExists) {
                // New database
                return __writeFile(filePath, '', 'utf8', (err) => callback(err));
            }

            // Write failed, use old version
            rename(tempFilename, filePath, (err) => callback(err));
        });
    });
}
