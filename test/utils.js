import chai from 'chai';
import {join, resolve} from 'path';
import {fileURLToPath} from 'url';

const PROJECT_ROOT_PATH = resolve(fileURLToPath(import.meta.url), '../..');

/**
 * @param {string} [relPath]
 * @return {string}
 */
export const resolveProjectPath = (relPath) => relPath ? join(PROJECT_ROOT_PATH, relPath) : PROJECT_ROOT_PATH;

/**
 * @param {string} dbFilename
 * @return {string} - absolute path to './test/workspace/{dbFilename}'
 */
export const pp = dbFilename => {
    if (!/^[a-z0-9]+\.?[a-z0-9~]+$/i.test(dbFilename)) {
        throw new Error('unexpected dbFilename: ' + dbFilename);
    }
    return resolveProjectPath('test/workspace/' + dbFilename);
};

chai.should();

export const {assert, expect} = chai;
