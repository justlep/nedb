import {AVLTree as BinarySearchTree} from '@justlep/binary-search-tree';
import {compareThings, getDotValue} from './model.js';
import {toDateSafeUnique} from './customUtils.js';

/**
 * All methods on an index guarantee that either the whole operation was successful and the index changed
 * or the operation was unsuccessful and an error is thrown while the index is unchanged.
 * 
 * @implements NedbIndex
 */
export class Index {

    /**
     * @param {Object} opts
     * @param {String} opts.fieldName On which field should the index apply (can use dot notation to index on sub fields)
     * @param {Boolean} [opts.unique] Optional, enforce a unique constraint (default: false)
     * @param {Boolean} [opts.sparse] Optional, allow a sparse index (we can have documents for which fieldName is undefined) (default: false)
     */
    constructor(opts) {
        this.fieldName = opts.fieldName;
        this.unique = !!opts.unique;
        this.sparse = !!opts.sparse;

        this.treeOptions = {unique: this.unique, compareKeys: compareThings, checkValueEquality: (a, b) => a === b};

        /** @type {BinarySearchTree} */
        this.tree = null;
        
        this.reset();   // No data in the beginning
    }

    /**
     * @inheritDoc
     */
    reset(newDocOrDocs) {
        this.tree = new BinarySearchTree(this.treeOptions);

        if (newDocOrDocs) {
            this.insert(newDocOrDocs);
        }
    }

    /**
     * O(log(n))
     * @inheritDoc
     */
    insert(docOrDocs) {
        if (Array.isArray(docOrDocs)) {
            this._insertMultipleDocs(docOrDocs);
            return;
        }

        let fieldValue = getDotValue(docOrDocs, this.fieldName);

        if (fieldValue === undefined && this.sparse) {
            // We don't index documents that don't contain the field if the index is sparse
            return;
        }

        if (!Array.isArray(fieldValue)) {
            this.tree.insert(fieldValue, docOrDocs);
            return;
        }
        
        let values = toDateSafeUnique(fieldValue),
            totalRevertible = 0;

        try {
            for (const value of values) {
                this.tree.insert(value, docOrDocs);
                totalRevertible++;
            }
        } catch (err) {
            // If an insert fails due to a unique constraint, roll back all inserts before it
            while (totalRevertible) {
                this.tree.delete(values[--totalRevertible], docOrDocs);
            }
            throw err;
        }
    }

    /**
     * Insert an array of documents in the index
     * If a constraint is violated, the changes should be rolled back and an error thrown
     * @param {Object[]} docs
     */
    _insertMultipleDocs(docs) {
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
     * O(log(n))
     * @inheritDoc
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
     * Naive implementation, still in O(log(n))
     * @inheritDoc
     */
    update(oldDocOrPairs, newDoc) {
        if (Array.isArray(oldDocOrPairs)) {
            this._updateMultipleDocs(oldDocOrPairs);
            return;
        }

        this.remove(oldDocOrPairs);

        try {
            this.insert(newDoc);
        } catch (e) {
            this.insert(oldDocOrPairs);
            throw e;
        }
    }

    /**
     * Update multiple documents in the index
     * If a constraint is violated, the changes need to be rolled back
     * and an error thrown
     * @param {NedbOldNewDocPair[]} pairs
     * @private
     */
    _updateMultipleDocs(pairs) {
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
     * @inheritDoc
     */
    revertUpdate(oldDoc, newDoc) {
        if (Array.isArray(oldDoc)) {
            this.update(oldDoc.map(({oldDoc, newDoc}) => ({oldDoc: newDoc, newDoc: oldDoc})));
        } else {
            this.update(newDoc, oldDoc);
        }
    }

    /**
     * @inheritDoc
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
     * @inheritDoc
     */
    getBetweenBounds(query) {
        return this.tree.betweenBounds(query);
    }

    /**
     * @inheritDoc
     */
    getAll() {
        let res = [];
        this.tree.executeOnEveryNode(node => res.push(...node.data));
        return res;
    }

    /**
     * @inheritDoc
     */
    _getNumberOfKeys() {
        return this.tree.getNumberOfKeys();
    }
}
