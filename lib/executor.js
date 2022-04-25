
/**
 * Responsible for sequentially executing actions on the database in order
 * @param {boolean} immediatelyReady
 * @constructor
 * @internal
 */
export function Executor(immediatelyReady) {

    let _isReady = immediatelyReady;
    
    const NO_TASK = Symbol();
    
    /** @type {NedbExecutorTask[]} */
    let buffer = [],
        tasks = [];
    
    /** @type {NedbExecutorTask|symbol} */
    let currentTask = NO_TASK,
        lastFinishedTask = NO_TASK;

    const _onTaskFinished = () => {
        if (currentTask === lastFinishedTask) {
            throw new Error('Callback was already called.'); // assuming we're never reusing same task data
        }
        lastFinishedTask = currentTask;
        currentTask = NO_TASK;
        setImmediate(_processNext); // decouple old task's callback from next task's fn 
    };
        
    const _processNext = () => {
        if (currentTask !== NO_TASK || !tasks.length) {
            return;
        }
        
        currentTask = tasks.shift();

        let taskArgs = [...currentTask.arguments],
            lastArgIndex = taskArgs.length - 1,
            lastArg = lastArgIndex >= 0 && taskArgs[lastArgIndex];

        // Always tell the queue task is complete. Execute callback if any was given.
        if (typeof lastArg === 'function') { // Callback was supplied
            taskArgs[lastArgIndex] = function() {
                _onTaskFinished();
                lastArg.apply(null, arguments);
            };
        } else if (lastArgIndex >= 0 && !taskArgs[lastArgIndex]) { // falsy value supplied as callback
            taskArgs[lastArgIndex] = _onTaskFinished;
        } else {
            // Nothing supplied as callback
            taskArgs.push(_onTaskFinished);
        }

        currentTask.fn.apply(currentTask.this, taskArgs);
    };
    
    
    /**
     * If executor is ready, queue task (and process it immediately if executor was idle)
     * If not, buffer task for later processing
     * 
     * @param {NedbExecutorTask|NedbExecutorTask[]} taskOrTasks - 
     * @param {Boolean} [forceQueuing] if true, force executor to queue task even if it is not ready
     */
    this.push = (taskOrTasks, forceQueuing) => {
        let target = (_isReady || forceQueuing) ? tasks : buffer;
        if (Array.isArray(taskOrTasks)) {
            target.push(...taskOrTasks);
        } else {
            target.push(taskOrTasks);
        }
        setImmediate(_processNext);
    };

    
    /**
     * Queue all tasks in buffer (in the same order they came in)
     * Automatically sets executor as ready
     */
    this.processBuffer = () => {
        _isReady = true;
        if (buffer.length) {
            tasks.push(...buffer);
            buffer = [];
            setImmediate(_processNext);
        }
    };
    
}

/**
 * @typedef {Object} NedbExecutorTask
 * @property {?Object} this - Object to use as this
 * @property {function} fn - Function to execute
 * @property {Array|IArguments} arguments - Array of arguments,
 *                                          IMPORTANT: only the last argument may be a function (the callback)
 *                                                     and the last argument cannot be false/undefined/null
 */
