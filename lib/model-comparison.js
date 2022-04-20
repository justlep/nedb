import {isDate, isRegExp} from './customUtils.js';
import {areThingsEqual, matchQuery} from './model.js';

/**
 * Check that two values are comparable
 */
const areComparable = (a, b, aType = typeof a, bType = typeof b) => 
    (aType === bType) && (aType === 'string' || aType === 'number' || (isDate(a) && isDate(b)));

/**
 * @param {*} needle
 * @param {Array} haystack
 * @param {string} [__op]
 * @return {boolean}
 */
const $in = (needle, haystack, __op = '$in') => {
    if (!Array.isArray(haystack)) {
        throw new Error(`${__op} operator called with a non-array`);
    }
    for (const item of haystack) {
        if (areThingsEqual(needle, item)) {
            return true;
        }
    }
    return false;
};

export const comparisonFunctions = Object.assign(Object.create(null), {
    /**
     * Arithmetic and comparison operators
     * @param {*} a - native value in the object
     * @param {*} b -native value in the query
     */
    $lt:  (a, b) => areComparable(a, b) && a < b,
    $lte: (a, b) => areComparable(a, b) && a <= b,
    $gt:  (a, b) => areComparable(a, b) && a > b,
    $gte: (a, b) => areComparable(a, b) && a >= b,
    $ne:  (a, b) => (a === undefined) || !areThingsEqual(a, b),
    $in,
    $nin: (a, b) => !$in(a, b, '$nin'),

    /**
     * @param {string} a
     * @param {RegExp} b
     * @return {boolean}
     */
    $regex(a, b) {
        if (isRegExp(b)) {
            return (typeof a === 'string') && b.test(a);
        }
        throw new Error('$regex operator called with non regular expression');
    },

    $exists(value, exists) {
        // This will be true for all values of exists except false, null, undefined and 0
        // That's strange behaviour (we should only use true/false) but that's the way Mongo does it...
        return (!!exists || exists === '') ^ (value === undefined);
    },

    // Specific to arrays
    $size(obj, value) {
        if (!Array.isArray(obj)) {
            return false;
        }
        if (value % 1 !== 0) {
            throw new Error('$size operator called without an integer');
        }

        return (obj.length === value);
    },
    
    $elemMatch(obj, value) {
        if (Array.isArray(obj)) {
            for (const item of obj) {
                if (matchQuery(item, value)) {
                    return true;
                }
            }
        }
        return false;
    }
});


export const arrayComparisonFunctions = Object.create(null);

arrayComparisonFunctions.$size = true;
arrayComparisonFunctions.$elemMatch = true;
