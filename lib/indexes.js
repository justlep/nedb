import {AVLTree as BinarySearchTree} from 'binary-search-tree';
import {compareThings, getDotValue} from './model.js';
import {toDateSafeUnique} from './customUtils.js';

/**
 * All methods on an index guarantee that either the whole operation was successful and the index changed
 * or the operation was unsuccessful and an error is thrown while the index is unchanged
 * @param {Object} opts
 * @param {String} opts.fieldName On which field should the index apply (can use dot notation to index on sub fields)
 * @param {Boolean} [opts.unique] Optional, enforce a unique constraint (default: false)
 * @param {Boolean} [opts.sparse] Optional, allow a sparse index (we can have documents for which fieldName is undefined) (default: false)
 */
export class Index {
    
    constructor(opts) {
        this.fieldName = opts.fieldName;
        this.unique = !!opts.unique;
        this.sparse = !!opts.sparse;

        this.treeOptions = {unique: this.unique, compareKeys: compareThings, checkValueEquality: (a, b) => a === b};

        this.reset();   // No data in the beginning
    }

    /**
     * Reset an index
     * @param {Object|Object[]} [newData] Optional, document or array of documents to initialize the index with
     *                                    If an error is thrown during insertion, the index is not modified
     */
    reset(newData) {
        this.tree = new BinarySearchTree(this.treeOptions);

        if (newData) {
            this.insert(newData);
        }
    }

    /**
     * Insert a new document in the index
     * If an array is passed, we insert all its elements (if one insertion fails the index is not modified)
     * O(log(n))
     */
    insert(doc) {
        if (Array.isArray(doc)) {
            this.insertMultipleDocs(doc);
            return;
        }

        let fieldValue = getDotValue(doc, this.fieldName);

        if (fieldValue === undefined && this.sparse) {
            // We don't index documents that don't contain the field if the index is sparse
            return;
        }

        if (!Array.isArray(fieldValue)) {
            this.tree.insert(fieldValue, doc);
            return;
        }
        
        let values = toDateSafeUnique(fieldValue),
            totalRevertible = 0;

        try {
            for (const value of values) {
                this.tree.insert(value, doc);
                totalRevertible++;
            }
        } catch (err) {
            // If an insert fails due to a unique constraint, roll back all inserts before it
            while (totalRevertible) {
                this.tree.delete(values[--totalRevertible], doc);
            }
            throw err;
        }
    }

    /**
     * Insert an array of documents in the index
     * If a constraint is violated, the changes should be rolled back and an error thrown
     *
     * @API private
     */
    insertMultipleDocs(docs) {
        let totalRevertible = 0;
        try {
            for (const doc of docs) {
                this.insert(doc);
                totalRevertible++;
            }
        } catch (err) {
            while (totalRevertible) {
                this.remove(docs[--totalRevertible]);
            }
            throw err;
        }
    }

    /**
     * Remove a document from the index
     * If an array is passed, we remove all its elements
     * The remove operation is safe with regards to the 'unique' constraint
     * O(log(n))
     */
    remove(docOrDocs) {
        if (Array.isArray(docOrDocs)) {
            for (const doc of docOrDocs) {
                this.remove(doc);
            }
            return;
        }

        let fieldValue = getDotValue(docOrDocs, this.fieldName);
        if (fieldValue === undefined && this.sparse) {
            return;
        }

        if (Array.isArray(fieldValue)) {
            for (const val of toDateSafeUnique(fieldValue)) {
                this.tree.delete(val, docOrDocs);
            }
        } else {
            this.tree.delete(fieldValue, docOrDocs);
        }
    }

    /**
     * Update a document in the index
     * If a constraint is violated, changes are rolled back and an error thrown
     * Naive implementation, still in O(log(n))
     */
    update(oldDoc, newDoc) {
        if (Array.isArray(oldDoc)) {
            this.updateMultipleDocs(oldDoc);
            return;
        }

        this.remove(oldDoc);

        try {
            this.insert(newDoc);
        } catch (e) {
            this.insert(oldDoc);
            throw e;
        }
    }

    /**
     * @typedef {Object} OldNewDocObject
     * @property {Object} oldDoc
     * @property {Object} newDoc
     */
    
    /**
     * Update multiple documents in the index
     * If a constraint is violated, the changes need to be rolled back
     * and an error thrown
     * @param {OldNewDocObject[]} pairs
     *
     * @API private
     */
    updateMultipleDocs(pairs) {
        for (const {oldDoc} of pairs) {
            this.remove(oldDoc);
        }

        let totalAdded = 0;
        try {
            for (const {newDoc} of pairs) {
                this.insert(newDoc);
                totalAdded++;
            }
        } catch (err) {
            // roll back changes upon error
            while (totalAdded) {
                this.remove(pairs[--totalAdded].newDoc);
            }
            for (const {oldDoc} of pairs) {
                this.insert(oldDoc);
            }
            throw err;
        }
    }

    /**
     * Revert an update
     * @param {Object|OldNewDocObject[]} oldDoc
     * @param {Object} [newDoc]
     */
    revertUpdate(oldDoc, newDoc) {
        if (Array.isArray(oldDoc)) {
            this.update(oldDoc.map(({oldDoc, newDoc}) => ({oldDoc: newDoc, newDoc: oldDoc})));
        } else {
            this.update(newDoc, oldDoc);
        }
    }

    /**
     * Get all documents in index whose key match value (if it is a Thing) or one of the elements of value (if it is an array of Things)
     * @param {*} value Value to match the key against
     * @return {Object[]} - array of documents
     */
    getMatching(value) {
        if (!Array.isArray(value)) {
            return this.tree.search(value);
        }
        let _res = Object.create(null); 

        for (const v of value) {
            for (const doc of this.getMatching(v)) {
                _res[doc._id] = doc;
            }
        }

        return Object.values(_res);
    }

    /**
     * Get all documents in index whose key is between bounds are they are defined by query
     * Documents are sorted by key
     * @param {Object} query
     * @return {Object[]} - array of documents
     */
    getBetweenBounds(query) {
        return this.tree.betweenBounds(query);
    }

    /**
     * Get all elements in the index
     * @return {Object[]} - array of documents
     */
    getAll() {
        let res = [];
        this.tree.executeOnEveryNode(node => res.push(...node.data));
        return res;
    }
}
