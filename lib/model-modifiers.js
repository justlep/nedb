import {hasOwnProp} from './customUtils.js';
import {compareThings, matchQuery} from './model.js';


/**
 * Set a field to a new value
 */
function $set(obj, field, value) {
    // assigning to primitives will throw an error here
    // however, looks like only falsy objs are supposed to be silently ignored 
    // according to model test "Doesn't replace a falsy field by an object when recursively following dot notation"
    if (obj) {
        obj[field] = value; // throws when assigning to primitives
    }
}

/**
 * Unset a field
 */
function $unset(obj, field /*, value */) {
    delete obj[field];
}

/**
 * Push an element to the end of an array field
 * Optional modifier $each instead of value to push several values
 * Optional modifier $slice to slice the resulting array, see https://docs.mongodb.org/manual/reference/operator/update/slice/
 * Différeence with MongoDB: if $slice is specified and not $each, we act as if value is an empty array
 */
function $push(obj, field, value) {
    // Create the array if it doesn't exist
    if (!hasOwnProp(obj, field)) {
        obj[field] = [];
    }

    if (!Array.isArray(obj[field])) {
        throw new Error('Can\'t $push an element on non-array values');
    }

    if (value && typeof value === 'object' && value.$slice && value.$each === undefined) {
        value.$each = [];
    }

    if (value && typeof value === 'object' && value.$each) {
        if (Object.keys(value).length >= 3 || (Object.keys(value).length === 2 && value.$slice === undefined)) {
            throw new Error('Can only use $slice in conjunction with $each when $push to array');
        }
        if (!Array.isArray(value.$each)) {
            throw new Error('$each requires an array value');
        }

        obj[field].push(...value.$each);

        if (value.$slice === undefined || typeof value.$slice !== 'number') {
            return;
        }

        if (value.$slice === 0) {
            obj[field] = [];
        } else {
            let start, 
                end, 
                n = obj[field].length;
            
            if (value.$slice < 0) {
                start = Math.max(0, n + value.$slice);
                end = n;
            } else if (value.$slice > 0) {
                start = 0;
                end = Math.min(n, value.$slice);
            }
            obj[field] = obj[field].slice(start, end);
        }
    } else {
        obj[field].push(value);
    }
}

/**
 * Add an element to an array field only if it is not already in it
 * No modification if the element is already in the array
 * Note that it doesn't check whether the original array contains duplicates
 */
function $addToSet(obj, field, value) {
    // Create the array if it doesn't exist
    if (!hasOwnProp(obj, field)) {
        obj[field] = [];
    }

    if (!Array.isArray(obj[field])) {
        throw new Error('Can\'t $addToSet an element on non-array values');
    }

    if (value && typeof value === 'object' && value.$each) {
        if (Object.keys(value).length > 1) {
            throw new Error('Can\'t use another field in conjunction with $each');
        }
        if (!Array.isArray(value.$each)) {
            throw new Error('$each requires an array value');
        }

        for (const v of value.$each) {
            $addToSet(obj, field, v);
        }
    } else {
        for (const v of obj[field]) {
            if (compareThings(v, value) === 0) {
                return; // already there
            }
        }
        obj[field].push(value);
    }
}

/**
 * Remove the first or last element of an array
 */
function $pop(obj, field, value) {
    if (!Array.isArray(obj[field])) {
        throw new Error('Can\'t $pop an element from non-array values');
    }
    if (typeof value !== 'number') {
        throw new Error(value + ' isn\'t an integer, can\'t use it with $pop');
    }
    if (value === 0) {
        return;
    }

    if (value > 0) {
        obj[field] = obj[field].slice(0, obj[field].length - 1);
    } else {
        obj[field] = obj[field].slice(1);
    }
}

/**
 * Removes all instances of a value from an existing array
 */
function $pull(obj, field, value) {
    let arr, i;

    if (!Array.isArray(obj[field])) {
        throw new Error('Can\'t $pull an element from non-array values');
    }

    arr = obj[field];
    for (i = arr.length - 1; i >= 0; --i) {
        if (matchQuery(arr[i], value)) {
            arr.splice(i, 1);
        }
    }
}

/**
 * Increment a numeric field's value
 */
function $inc(obj, field, value) {
    if (typeof value !== 'number') {
        throw new Error(value + ' must be a number');
    }

    if (typeof obj[field] !== 'number') {
        if (hasOwnProp(obj, field)) {
            throw new Error('Don\'t use the $inc modifier on non-number fields');
        }
        obj[field] = value;
    } else {
        obj[field] += value;
    }
}

/**
 * Updates the value of the field, only if specified field is greater than the current value of the field
 */
function $max(obj, field, value) {
    if (typeof obj[field] === 'undefined') {
        obj[field] = value;
    } else if (value > obj[field]) {
        obj[field] = value;
    }
}

/**
 * Updates the value of the field, only if specified field is smaller than the current value of the field
 */
function $min(obj, field, value) {
    if (typeof obj[field] === 'undefined') {
        obj[field] = value;
    } else if (value < obj[field]) {
        obj[field] = value;
    }
}


/**
 * The structure of modifier functions is always the same: recursively follow the dot notation while creating
 * the nested documents if needed, then apply the "last step modifier"
 * @type {Object.<string, NedbModifierFunction>}
 */
export const modifierFunctions = Object.create(null); 
    
Object.entries({$min, $max, $addToSet, $push, $pop, $pull, $inc, $set, $unset}).forEach(([modifier, lastStepFn]) => {
    
    const nonLastStepFn = function (obj, field, value) {
        let fieldParts = typeof field === 'string' ? field.split('.') : field;

        if (fieldParts.length === 1) {
            return lastStepFn(obj, field, value);
        }
        let [firstFieldPart, ...moreFieldParts] = fieldParts;
        if (obj[firstFieldPart] === undefined) {
            if (lastStepFn === $unset) {
                return;
            }
            // Bad looking specific fix, needs to be generalized modifiers that behave like $unset are implemented
            obj[firstFieldPart] = Object.create(null);
        }
        nonLastStepFn(obj[firstFieldPart], moreFieldParts, value);
    };
    
    modifierFunctions[modifier] = nonLastStepFn;
});


/**
 * @typedef {Function} NedbModifierFunction
 * @param {Object} obj - The model to modify
 * @param {string} field - Can contain dots, in that case that means we will set a subfield recursively
 * @param {*} value
 */
