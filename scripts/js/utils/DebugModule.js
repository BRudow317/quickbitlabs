/**
 * DebugUtils.jsx - A collection of debugging utility functions for JavaScript/React applications.
 * @module DebugUtils
 */

/**
 * debugTable - Display array/object data in table format
 * @param {Array|Object} data - Data to display
 * @param {string} label - Optional label
 * 
 * @example
 * debugTable(users, 'All Users');
 * debugTable({ name: 'John', age: 30 }, 'User Details');
 */
export const debugTable = (data, label = 'Data') => {
  if (process.env.NODE_ENV === 'production') return;

  console.group(`${label}`);
  console.table(data);
  console.groupEnd();
};

/**
 * debugTime - Measure execution time of a function
 * @param {Function} fn - Function to measure
 * @param {string} label - Label for the measurement
 * @returns {any} - Result of the function
 * 
 * @example
 * const result = debugTime(() => expensiveOperation(), 'Expensive Op');
 * const data = await debugTime(() => fetchData(), 'API Call');
 */
export const debugTime = async (fn, label = 'Execution') => {
  if (process.env.NODE_ENV === 'production') return fn();

  console.time(`${label}`);
  const result = await fn();
  console.timeEnd(`${label}`);
  return result;
};

/**
 * debugMemory - Log current memory usage (Chrome only)
 * @param {string} label - Label for the measurement
 * 
 * @example
 * debugMemory('Before heavy operation');
 * // ... do stuff
 * debugMemory('After heavy operation');
 */
export const debugMemory = (label = 'Memory') => {
  if (process.env.NODE_ENV === 'production') return;

  if (performance.memory) {
    const { usedJSHeapSize, totalJSHeapSize } = performance.memory;
    const used = (usedJSHeapSize / 1048576).toFixed(2);
    const total = (totalJSHeapSize / 1048576).toFixed(2);
    console.log(`${label}: ${used}MB / ${total}MB (${((usedJSHeapSize / totalJSHeapSize) * 100).toFixed(1)}%)`);
  } else {
    console.log(`${label}: Memory API not available`);
  }
};

/**
 * debugNetwork - Log network request details
 * @param {string} url - Request URL
 * @param {Object} options - Fetch options
 * @param {Response} response - Fetch response
 * 
 * @example
 * const response = await fetch(url, options);
 * debugNetwork(url, options, response);
 */
export const debugNetwork = (url, options = {}, response = null) => {
  if (process.env.NODE_ENV === 'production') return;

  console.groupCollapsed(`${options.method || 'GET'} ${url}`);
  console.log('Options:', options);
  if (response) {
    console.log('Status:', response.status, response.statusText);
    console.log('Headers:', Object.fromEntries(response.headers.entries()));
  }
  console.groupEnd();
};