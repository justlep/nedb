
/**
 * A special Index intended for unique, non-sparse string fields, like the '_id' field.
 * (!) Does not support dot notation field names.
 * 
 * @implements NedbIndex
 */
export class UniqueStringIndex {

    /**
     * @param {string} fieldName
     */
    constructor(fieldName) {
        if (!fieldName || typeof fieldName !== 'string' || fieldName.trim() !== fieldName) {
            throw new Error('Invalid fieldName');
        }
        if (~fieldName.indexOf('.')) {
            throw new Error('UniqueStringIndex does not support dot notation field names');
        }
        
        /** @type {Map<string, Object>} */
        this._map = new Map();
        
        this._fieldName = fieldName; 
    }

    /**
     * @inheritDoc
     */
    getAll() {
        return Array.from(this._map.values());
    }

    /**
     * @throws Error will always be thrown for UniqueStringIndex type.
     */
    getBetweenBounds(query) {
        throw new Error('UniqueStringIndex#getBetweenBounds() is not supported');
    }

    /**
     * @inheritDoc
     */
    getMatching(valOrValues) {
        let res = [];
        for (const val of Array.isArray(valOrValues) ? valOrValues : arguments) {
            let doc = this._map.get(val);
            if (doc) {
                res.push(doc);
            }
        }
        return res;
    }

    /**
     * @param {string} id
     * @return {Object[]} - array with max. 1 document
     */
    getMatchingForSingle(id) {
        let doc = this._map.get(id);
        return doc ? [doc] : [];
    }
    
    /**
     * @param {Object} docOrDocs
     * @return {Object[]|IArguments<Object>} docs - validated docs
     * @throws Error if any of the given documents is falsy or does not contain a valid value for {@link #_fieldName}
     * @private
     */
    _assertValidDocs(docOrDocs) {
        let docs = Array.isArray(docOrDocs) ? docOrDocs : arguments;
        for (const doc of docs) {
            if (!doc || typeof doc !== 'object' || typeof doc[this._fieldName] !== 'string') {
                throw new Error(`Invalid document or ${this._fieldName} value`);
            }
        }
        return docs;
    }

    /**
     * @inheritDoc
     */
    insert(docOrDocs) {
        let docs = this._assertValidDocs(docOrDocs);
        this._insert(docs);
    }

    /**
     * @param {Object[]} docs - pre-validated documents (in the meaning they're objects with an [_fieldName]:string field)
     * @private
     */
    _insert(docs) {
        let totalRevertible = 0;

        for (const doc of docs) {
            let fieldValue = doc[this._fieldName];
            if (this._map.has(fieldValue)) {
                let err = new Error(`Can't insert key ${fieldValue}, it violates the unique constraint`);
                err.key = fieldValue;
                err.errorType = 'uniqueViolated';

                // If an insert fails due to a unique constraint, roll back all inserts before it
                while (totalRevertible) {
                    this._map.delete(docs[--totalRevertible][this._fieldName]);
                }
                throw err;
            }
            this._map.set(doc[this._fieldName], doc);
            totalRevertible++;
        }
    }

    /**
     * @inheritDoc
     */
    remove(docOrDocs) {
        this._remove(this._assertValidDocs(docOrDocs));
    }

    /**
     * @param {Object} docs - pre-validated documents (in the meaning they're object with a [_fieldName]:string field)
     * @private
     */
    _remove(docs) {
        const f = this._fieldName;
        for (const doc of docs) {
            this._map.delete(doc[f]);
        }
    }

    /**
     * @inheritDoc
     */
    reset(newDocOrDocs) {
        this._map.clear();
        if (newDocOrDocs) {
            this.insert(newDocOrDocs);
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
    update(oldDocOrPairs, newDoc) {
        let oldDocs = [],
            newDocs = [];
        
        if (Array.isArray(oldDocOrPairs)) {
            const f = this._fieldName;
            for (const pair of oldDocOrPairs) {
                if (!pair || !pair.oldDoc || !pair.newDoc || typeof pair.oldDoc[f] !== 'string' || typeof pair.newDoc[f] !== 'string') {
                    throw new Error('Invalid oldDoc/newDoc pair');
                }
                oldDocs.push(pair.oldDoc);
                newDocs.push(pair.newDoc);
            }
        } else {
            this._assertValidDocs(arguments);
            oldDocs.push(oldDocOrPairs);
            newDocs.push(newDoc);
        }
        
        this._remove(oldDocs);

        try {
            this._insert(newDocs);
            
        } catch (err) {
            // no need to rollback any inserted docs - rollback is already done by _insert()  
            this.insert(oldDocs);
            throw err;
        }
    }

    /**
     * @inheritDoc
     */
    _getNumberOfKeys() {
        return this._map.size;
    }
}
