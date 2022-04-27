/**
 * All methods on an index guarantee that either the whole operation was successful and the index changed
 * or the operation was unsuccessful and an error is thrown while the index is unchanged.
 * @interface
 */
export class NedbIndex {

    /**
     * Reset an index
     * @param {Object|Object[]} [newDocOrDocs] Optional document or array of documents to initialize the index with.
     *                                         If an error is thrown during insertion, the index is not modified
     */
    reset(newDocOrDocs) {}

    /**
     * Insert a new document in the index
     * If an array is passed, we insert all its elements (if one insertion fails the index is not modified)
     * @param {Object|Object[]} docOrDocs
     */
    insert(docOrDocs) {}

    /**
     * Remove a document from the index
     * If an array is passed, we remove all its elements
     * The remove operation is safe with regards to the 'unique' constraint
     */
    remove(docOrDocs) {}

    /**
     * Update a document in the index
     * If a constraint is violated, changes are rolled back and an error thrown
     * @param {Object|NedbOldNewDocPair[]} oldDocOrPairs
     * @param {Object} [newDoc]
     */
    update(oldDocOrPairs, newDoc) {}

    /**
     * Revert an update
     * @param {Object|NedbOldNewDocPair[]} oldDoc
     * @param {Object} [newDoc]
     */
    revertUpdate(oldDoc, newDoc) {}

    /**
     * Get all documents in index whose key match value (if it is a Thing) or one of the elements of value (if it is an array of Things)
     * @param {*} value Value to match the key against
     * @return {Object[]} - array of documents
     */
    getMatching(value) {}

    /**
     * Get all documents in index whose key is between bounds are they are defined by query
     * Documents are sorted by key
     * @param {Object} query
     * @return {Object[]} - array of documents
     */
    getBetweenBounds(query) {}

    /**
     * Get all elements in the index
     * @return {Object[]} - array of documents
     */
    getAll() {}

    /**
     * @return {number}
     * @internal
     */
    _getNumberOfKeys() {}
}


/**
 * @typedef {Object} NedbOldNewDocPair
 * @property {Object} oldDoc
 * @property {Object} newDoc
 */
