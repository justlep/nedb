import {isDate, isRegExp} from './customUtils.js';
import {modifierFunctions} from './model-modifiers.js';
import {arrayComparisonFunctions, comparisonFunctions} from './model-comparison.js';
import {logicalOperators} from './model-operators.js';


/**
 * Check a key, throw an error if the key is non valid
 * @param {String} k key
 * @param {*} v value, needed to treat the Date edge case
 * Non-treatable edge cases here: if part of the object if of the form { $$date: number } or { $$deleted: true }
 * Its serialized-then-deserialized version it will transformed into a Date object
 * But you really need to want it to trigger such behaviour, even when warned not to use '$' at the beginning of the field names...
 */
function checkKey(k, v) {
    if (typeof k === 'number') {
        k = '' + k;
    } else if (k[0] === '$' && (k !== '$$date' || typeof v !== 'number') && (k !== '$$deleted' || v !== true) && k !== '$$indexCreated' && k !== '$$indexRemoved') {
        throw new Error('Field names cannot begin with the $ character');
    }

    if (~k.indexOf('.')) {
        throw new Error('Field names cannot contain a .');
    }
}


/**
 * Check a DB object and throw an error if it's not valid
 * Works by applying the above checkKey function to all fields recursively
 * @param {...*} objsToCheck
 */
export function checkObject(...objsToCheck) {
    while (objsToCheck.length) {
        let obj = objsToCheck.pop();
        if (!obj) {
            continue;
        }
        if (Array.isArray(obj)) {
            objsToCheck.push(...obj);
        } else if (typeof obj === 'object') {
            for (const key of Object.keys(obj)) {
                checkKey(key, obj[key]);
            }
            objsToCheck.push(...Object.values(obj));
        }
    }
}

function _serializingReplacer(k, v) {
    checkKey(k, v);

    // Hackish way of checking if object is Date (this way it works between execution contexts in node-webkit).
    // We can't use value directly because for dates it is already string in this function (date.toJSON was already called), so we use this
    if (v && isDate(this[k])) {
        return {$$date: this[k].getTime()};
    }

    return v;
}

/**
 * Serialize an object to be persisted to a one-line string
 * For serialization/deserialization, we use the native JSON parser and not eval or Function
 * That gives us less freedom but data entered in the database may come from users
 * so eval and the like are not safe
 * Accepted primitive types: Number, String, Boolean, Date, null
 * Accepted secondary types: Objects, Arrays
 */
export const serialize = (obj) => JSON.stringify(obj, _serializingReplacer);


function _deserializingReplacer(k, v) {
    if (k === '$$date') {
        return new Date(v);
    }
    
    if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') {
        return v;
    }
    
    return v?.$$date || v;
}

/**
 * From a one-line representation of an object generate by the serialize function
 * Return the object itself
 */
export const deserialize = (rawData) => JSON.parse(rawData, _deserializingReplacer);


/**
 * Deep copy a DB object
 * The optional strictKeys flag (defaulting to false) indicates whether to copy everything or only fields
 * where the keys are valid, i.e. don't begin with $ and don't contain a .
 */
export function cloneDeep(obj, strictKeys) {
    /** conditions from {@see isPrimitiveType} */
    if (typeof obj === 'boolean' || typeof obj === 'number' || typeof obj === 'string' || obj === null || isDate(obj)) {
        return obj;
    }
    
    if (Array.isArray(obj)) {
        return obj.map(o => cloneDeep(o, strictKeys));
    }

    if (typeof obj === 'object') {
        let res = Object.create(null);
        for (const k of Object.keys(obj)) {
            if (!strictKeys || (k[0] !== '$' && k.indexOf('.') < 0)) {
                res[k] = cloneDeep(obj[k], strictKeys);
            }
        }
        return res;
    }

    // For now everything else is undefined. We should probably throw an error instead
    // TODO re-check throw
    return undefined;   
}


/**
 * Tells if an object is a primitive type or a "real" object
 * Arrays are considered primitive
 */
export const isPrimitiveType = (obj) => obj === null || typeof obj === 'boolean' || typeof obj === 'number' || typeof obj === 'string' 
                                          || isDate(obj) || Array.isArray(obj);

/**
 * Utility functions for comparing things
 * Assumes type checking was already done (a and b already have the same type)
 * compareNSB works for numbers, strings and booleans
 */
const compareNSB = (a, b) => (a < b) ? -1 : (a > b) ? 1 : 0;

function compareArrays(a, b) {
    for (let i = 0, len = Math.min(a.length, b.length), comp; i < len; i++) {
        comp = compareThings(a[i], b[i]);
        if (comp) {
            return comp;
        }
    }

    // Common section was identical, longest one wins
    return compareNSB(a.length, b.length);
}


/**
 * Compare { things U undefined }
 * Things are defined as any native types (string, number, boolean, null, date) and objects
 * We need to compare with undefined as it will be used in indexes
 * In the case of objects and arrays, we deep-compare
 * If two objects dont have the same type, the (arbitrary) type hierarchy is: undefined, null, number, strings, boolean, dates, arrays, objects
 * Return -1 if a < b, 1 if a > b and 0 if a = b (note that equality here is NOT the same as defined in areThingsEqual!)
 *
 * @param {*} a
 * @param {*} b
 * @param {?Function} [_compareStrings] String comparing function, returning -1, 0 or 1, overriding default string comparison (useful for languages with accented letters)
 */
export function compareThings(a, b, _compareStrings = compareNSB) {
    // undefined
    if (a === undefined) {
        return b === undefined ? 0 : -1;
    }
    if (b === undefined) {
        return 1;
    }

    // null
    if (a === null) {
        return b === null ? 0 : -1;
    }
    if (b === null) {
        return 1;
    }

    // Numbers
    if (typeof a === 'number') {
        return typeof b === 'number' ? compareNSB(a, b) : -1;
    }
    if (typeof b === 'number') {
        return 1;
    }

    // Strings
    if (typeof a === 'string') {
        return typeof b === 'string' ? _compareStrings(a, b) : -1;
    }
    if (typeof b === 'string') {
        return 1;
    }

    // Booleans
    if (typeof a === 'boolean') {
        return typeof b === 'boolean' ? compareNSB(a, b) : -1;
    }
    if (typeof b === 'boolean') {
        return 1;
    }

    // Dates
    if (isDate(a)) {
        return isDate(b) ? compareNSB(a.getTime(), b.getTime()) : -1;
    }
    if (isDate(b)) {
        return 1;
    }

    // Arrays (first element is most significant and so on)
    if (Array.isArray(a)) {
        return Array.isArray(b) ? compareArrays(a, b) : -1;
    }
    if (Array.isArray(b)) {
        return 1;
    }

    // Objects
    let aKeys = Object.keys(a).sort(),
        bKeys = Object.keys(b).sort();

    for (let i = 0, len = Math.min(aKeys.length, bKeys.length), comp; i < len; i++) {
        comp = compareThings(a[aKeys[i]], b[bKeys[i]]);
        if (comp) {
            return comp;
        }
    }

    return compareNSB(aKeys.length, bKeys.length);
}


// ==============================================================
// Updating documents
// ==============================================================


/**
 * Modify a DB object according to an update query
 * @param {Object} doc - a document object
 * @param {Object} updateQuery
 */
export function modifyDoc(doc, updateQuery) {
    let keys = Object.keys(updateQuery),
        totalDollarKeys = keys.reduce((n, key) => key[0] === '$' ? n+1 : n, 0),
        newDoc;

    if (~keys.indexOf('_id') && updateQuery._id !== doc._id) {
        throw new Error('You cannot change a document\'s _id');
    }

    if (totalDollarKeys && totalDollarKeys !== keys.length) {
        throw new Error('You cannot mix modifiers and normal fields');
    }

    if (!totalDollarKeys) {
        // Simply replace the object with the update query contents
        newDoc = cloneDeep(updateQuery);
        newDoc._id = doc._id;
    } else {
        // Apply modifiers
        newDoc = cloneDeep(doc);
        for (const m of new Set(keys)) {
            let modifierFn = modifierFunctions[m];
            if (!modifierFn) {
                throw new Error('Unknown modifier ' + m);
            }

            let query = updateQuery[m];
            
            // Can't rely on Object.keys throwing on non objects since ES6
            // Not 100% satisfying as non objects can be interpreted as objects but no false negatives so we can live with it
            if (typeof query !== 'object') {
                throw new Error(`Modifier ${m}'s argument must be an object`);
            }
            for (const k of Object.keys(query)) {
                modifierFn(newDoc, k, query[k]);
            }
        }
    }

    // Check result is valid and return it
    checkObject(newDoc);

    if (doc._id !== newDoc._id) {
        throw new Error('You can\'t change a document\'s _id');
    }
    return newDoc;
}


// ==============================================================
// Finding documents
// ==============================================================

const DIGITS_REGEX = /^\d+$/;

/**
 * Get a value from object with dot notation
 * @param {Object} obj
 * @param {string|string[]} dotPath
 */
export function getDotValue(obj, dotPath) {
    if (!dotPath || obj === null || obj === undefined) {
        return obj;
    }
    if (typeof dotPath === 'string') {
        if (dotPath.indexOf('.') < 0) {
            return obj[dotPath];
        }

        dotPath = dotPath.split('.');
    }
    
    while (dotPath.length) {
        if (!obj) {
            return undefined;
        }
        let prop = dotPath.shift();
        if (!prop) {
            return obj;
        }
        if (Array.isArray(obj)) {
            // using regex here to distinguish array index vs. property name: 
            //   - faster than parseInt()+isNaN()
            //   - stricter than parseInt -> parseInt('123xyz') === 123 
            //   - no need to parse index at all since a[1]===a['1'] with no performance hit
            let isSegIndex = DIGITS_REGEX.test(prop);
            if (dotPath.length) {
                let restDotPath = dotPath.length > 1 ? dotPath : dotPath[0]; 
                return isSegIndex ? getDotValue(obj[prop], restDotPath) : obj.map(o => getDotValue(o[prop], restDotPath));  
            }
            return isSegIndex ? obj[prop] : obj.map(o => o[prop]);
        } 
        obj = obj[prop];
    }
    
    return obj;
}


/**
 * Check whether 'things' are equal
 * Things are defined as any native types (string, number, boolean, null, date) and objects
 * In the case of object, we check deep equality
 * Returns true if they are, false otherwise
 */
export function areThingsEqual(a, b) {
    if (a === undefined || b === undefined) {
        return false;
    }
    
    let _type;
    if (a === null || b === null || (_type = typeof a) === 'string' || _type === 'boolean' || _type === 'number'    // eslint-disable-line
                                 || (_type = typeof b) === 'string' || _type === 'boolean' || _type === 'number') { // eslint-disable-line
        return a === b;
    }
    
    // Dates
    let isDateA = isDate(a),
        isDateB = isDate(b);
    if (isDateA || isDateB) {
        return isDateA && isDateB && a.getTime() === b.getTime();
    }

    // Arrays (no match since arrays are used as a $in)
    // undefined (no match since they mean field doesn't exist and can't be serialized)
    if (Array.isArray(a) ^ Array.isArray(b)) {
        return false;
    }

    // General objects (check for deep equality)
    // a and b should be objects (or (holy) arrays!) at this point
    let aKeys = Object.keys(a),
        bKeys = Object.keys(b);

    if (aKeys.length !== bKeys.length) {
        return false;
    }
    
    if (aKeys.length > 25) {
        bKeys = new Set(bKeys); // from N > 25, new Set() + N * Set.has() becomes faster than N * Array.indexOf()   
        for (const key of aKeys) {
            if (!bKeys.has(key) || !areThingsEqual(a[key], b[key])) {
                return false;
            }
        }
    } else {
        for (const key of aKeys) {
            if (bKeys.indexOf(key) < 0 || !areThingsEqual(a[key], b[key])) {
                return false;
            }
        }
    }
   
    return true;
}


/**
 * Tell if a given document matches a query
 * @param {Object} doc - Document to check
 * @param {Object} query
 * @return {boolean}
 */
export function matchQuery(doc, query) {
    if (isPrimitiveType(doc) || isPrimitiveType(query)) {
        return matchQueryPart({doc}, 'doc', query);
    }

    // Normal query
    for (const key of Object.keys(query)) {
        if (key[0] === '$') {
            if (!logicalOperators[key]) {
                throw new Error('Unknown logical operator ' + key);
            }
            if (!logicalOperators[key](doc, query[key])) {
                return false;
            }
        } else if (!matchQueryPart(doc, key, query[key])) {
            return false;
        }
    }
    return true;
}


/**
 * Match an object against a specific { key: value } part of a query
 * if the treatObjAsValue flag is set, don't try to match every part separately, but the array as a whole
 * @param {Object} obj
 * @param {string} queryKey
 * @param {*} queryValue
 * @param {boolean} [treatObjAsValue]
 */
function matchQueryPart(obj, queryKey, queryValue, treatObjAsValue) {
    let objValue = getDotValue(obj, queryKey);

    if (Array.isArray(objValue) && !treatObjAsValue) {
        if (Array.isArray(queryValue)) {
            return matchQueryPart(obj, queryKey, queryValue, true); // try exact match
        }

        // Check if we are using an array-specific comparison function
        if (queryValue && typeof queryValue === 'object' && !isRegExp(queryValue)) {
            for (const key of Object.keys(queryValue)) {
                if (arrayComparisonFunctions[key]) {
                    return matchQueryPart(obj, queryKey, queryValue, true);
                }
            }
        }

        // If not, treat it as an array of { obj, query } where there needs to be at least one match
        for (const it of objValue) {
            if (matchQueryPart({it}, 'it', queryValue)) {
                return true;
            }
        }
        return false;
    }

    // queryValue is an actual object. Determine whether it contains comparison operators
    // or only normal fields. Mixed objects are not allowed
    if (queryValue && typeof queryValue === 'object' && !isRegExp(queryValue) && !Array.isArray(queryValue)) {
        let queryKeys = Object.keys(queryValue),
            totalDollarKeys = queryKeys.reduce((n, key) => key[0] === '$' ? n+1 : n, 0);

        if (totalDollarKeys) {
            // queryValue is an object of this form: { $comparisonOperator1: value1, ... }
            if (totalDollarKeys !== queryKeys.length) {
                throw new Error('You cannot mix operators and normal fields');
            }
            
            for (const key of queryKeys) {
                let compare = comparisonFunctions[key];
                if (!compare) {
                    throw new Error('Unknown comparison function ' + key);
                }
                if (!compare(objValue, queryValue[key])) {
                    return false;
                }
            }
            return true;
        }
    }
    
    // Using regular expressions with basic querying
    if (isRegExp(queryValue)) {
        return comparisonFunctions.$regex(objValue, queryValue);
    }
    
    // queryValue is either a native value or a normal object
    // Basic matching is possible
    return areThingsEqual(objValue, queryValue);
}
