/**
 * We need waterfall behavior from async 0.2.10 which was invoking tasks deferred via 'setImmediate', which async@ does not.
 * Code below is based on npm package async@0.2.10, with adjustments.
 * 
 * Copy of the async@0.2.10 LICENSE file:
 * 
 * Copyright (c) 2010 Caolan McMahon

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

const asyncIterator = (tasks) => {
    let makeCallback = (index) => {
        let fn = function() {
            if (tasks.length) {
                tasks[index].apply(null, arguments);
            }
            return fn.next();
        };
        fn.next = () => (index < tasks.length - 1) ? makeCallback(index + 1): null;
        return fn;
    };
    return makeCallback(0);
};

const _slice = Array.prototype.slice;

export const legacyWaterfall = function (tasks, callback) {
    if (!Array.isArray(tasks)) {
        return callback?.(new Error('First argument to waterfall must be an array of functions'));
    }
    if (!tasks.length) {
        return callback?.();
    }
    let wrapIterator = (iterator) => {
        return function (err) {
            if (err) {
                return callback = void callback?.apply(null, arguments);
            }
            let next = iterator.next(),
                args = _slice.call(arguments, 1);
            
            args.push(next ? wrapIterator(next) : callback);
            
            setImmediate(() => iterator.apply(null, args));
        };
    };
    wrapIterator(asyncIterator(tasks))();
}; 
