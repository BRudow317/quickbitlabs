/**
 * !!!! Not a memo !!!!
 * 
 * memoizer - Memoization helper for expensive functions
 * @param {Function} fn - Function to memoize
 * @param {Function} keyResolver - Custom cache key resolver
 * @returns {Function} - Memoized function
 * 
 * @example
 * const expensiveCalc = memoizer((n) => { ...slowFunction() });
 * const result = expensiveCalc(42); // Computed
 * const cached = expensiveCalc(42); // From cache
 */
export default memoizer;
export {memoizer};

const memoizer = (fn, keyResolver = (...args) => JSON.stringify(args)) => {
  const cache = new Map();
  
  const memoized = (...args) => {
    const key = keyResolver(...args);
    if (cache.has(key)) {
      return cache.get(key);
    }
    const result = fn(...args);
    cache.set(key, result);
    return result;
  };
  
  memoized.clear = () => cache.clear();
  memoized.delete = (...args) => cache.delete(keyResolver(...args));
  memoized.has = (...args) => cache.has(keyResolver(...args));
  
  return memoized;
};