import async from 'async';

/**
 * Responsible for sequentially executing actions on the database
 * @internal
 */
export class Executor {

    /**
     * @param  {boolean} initialReady
     */
    constructor(initialReady) {
        this.buffer = [];
        this.ready = initialReady;

        // This queue will execute all commands, one-by-one in order
        this.queue = async.queue(function (task, cb) {
            let newArgs = [...task.arguments],
                lastArgIndex = newArgs.length - 1,
                lastArg = lastArgIndex >= 0 && newArgs[lastArgIndex];

            // Always tell the queue task is complete. Execute callback if any was given.
            if (typeof lastArg === 'function') {
                // Callback was supplied
                newArgs[lastArgIndex] = function () {
                    setImmediate(cb);
                    lastArg.apply(null, arguments);    
                };
            } else if (!lastArg && lastArgIndex >= 0) {
                // falsy value supplied as callback
                newArgs[lastArgIndex] = function () {
                    cb();
                };
            } else {
                // Nothing supplied as callback
                newArgs.push(function () {
                    cb();
                });
            }

            task.fn.apply(task.this, newArgs);
        }, 1);
    }

    /**
     * If executor is ready, queue task (and process it immediately if executor was idle)
     * If not, buffer task for later processing
     * @param {Object} task
     *                 task.this - Object to use as this
     *                 task.fn - Function to execute
     *                 task.arguments - Array of arguments, IMPORTANT: only the last argument may be a function (the callback)
     *                                                                 and the last argument cannot be false/undefined/null
     * @param {Boolean} [forceQueuing] Optional (defaults to false) force executor to queue task even if it is not ready
     */
    push(task, forceQueuing) {
        if (this.ready || forceQueuing) {
            this.queue.push(task);
        } else {
            this.buffer.push(task);
        }
    }

    /**
     * Queue all tasks in buffer (in the same order they came in)
     * Automatically sets executor as ready
     */
    processBuffer() {
        this.ready = true;
        if (this.buffer.length) {
            this.queue.push(this.buffer); // no spread here - queue is an async queue, not an array! 
            this.buffer = [];
        }
    }
}
