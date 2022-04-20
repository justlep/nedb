import {matchQuery} from './model.js';

export const logicalOperators = Object.create(null);

/**
 * Match any of the subqueries
 * @param {Object} model
 * @param {Object[]} queries
 */
logicalOperators.$or = function (model, queries) {
    if (!Array.isArray(queries)) {
        throw new Error('$or operator used without an array');
    }
    for (const query of queries) {
        if (matchQuery(model, query)) {
            return true;
        }
    }
    return false;
};


/**
 * Match all of the subqueries
 * @param {Object} model
 * @param {Object[]} queries
 */
logicalOperators.$and = function (model, queries) {
    if (!Array.isArray(queries)) {
        throw new Error('$and operator used without an array');
    }
    for (const query of queries) {
        if (!matchQuery(model, query)) {
            return false;
        }
    }
    return true;
};


/**
 * Inverted match of the query
 * @param {Object} model
 * @param {Object} query
 */
logicalOperators.$not = function (model, query) {
    return !matchQuery(model, query);
};


/**
 * Use a function to match
 * @param {Object} model
 * @param {function(*):boolean} fn
 */
logicalOperators.$where = function (model, fn) {
    if (typeof fn !== 'function') {
        throw new Error('$where operator used without a function');
    }
    
    let result = fn.call(model);
    if (typeof result !== 'boolean') {
        throw new Error('$where function must return boolean');
    }
    return result;
};
