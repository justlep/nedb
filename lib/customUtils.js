import {randomBytes} from 'crypto';
import {types as utilTypes} from 'util';

export const {isDate, isRegExp} = utilTypes;

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
 * @param {*} obj - anything truthy; ignoring 0 or '' here is good enough
 * @param {string} prop
 * @return {boolean}
 */
export const hasOwnProp = (obj, prop) => obj ? _hasOwnProperty.call(obj, prop) : false;

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


/** Helper set storing projected values during {@link toDateSafeUnique} */
const _uniqSet = new Set();

/**
 * Creates a duplicate-free copy of given array, 
 * where Date objects are considered 'identical' if their timecodes equal.  
 * @param {*[]} values
 * @return {*[]} 
 */
export const toDateSafeUnique = (values) => {
    if (values.length <= 1) {
        return values.slice();
    }
    
    let total = 0,
        res = [];

    _uniqSet.clear();
    
    for (const val of values) {
        if (_uniqSet.add(typeof val === 'string' ? ('$s' + val) : isDate(val) ? ('$date' + val.getTime()) : val).size > total) {
            total = res.push(val);
        }
    }
    return res;
};

/** 
 * @callback NedbErrorCallback
 * @param {?Error} error
 */
