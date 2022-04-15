import {randomBytes} from 'crypto';
import {types as utilTypes} from 'util';

/**
 * Return a random alphanumerical string of length len
 * There is a very small probability (less than 1/1,000,000) for the length to be less than len
 * (il the base64 conversion yields too many pluses and slashes) but
 * that's not an issue here
 * The probability of a collision is extremely small (need 3*10^12 documents to have one chance in a million of a collision)
 * See http://en.wikipedia.org/wiki/Birthday_problem
 */
export function generateUid(len) {
    return randomBytes(Math.ceil(Math.max(8, len * 2)))
        .toString('base64')
        .replace(/[+/]/g, '')
        .slice(0, len);
}

const _hasOwnProperty = Object.prototype.hasOwnProperty;

/**
 * @param {*} obj
 * @param {string} prop
 * @return {boolean}
 */
export const hasOwnProp = (obj, prop) => (obj??false) && _hasOwnProperty.call(obj, prop);

/**
 * @param {*} obj
 * @param {string} props
 * @return {boolean}
 */
export const hasAnyOwnPropOf = (obj, ...props) => {
    if (obj === null || obj === void 0) {
        return false;
    }
    for (let prop of props) {
        if (_hasOwnProperty.call(obj, prop)) {
            return true;
        }
    }
    return false;
};

export const {isDate, isRegExp} = utilTypes;
