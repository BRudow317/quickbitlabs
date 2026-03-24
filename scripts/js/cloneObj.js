/**
 * Clone objects (handles circular refs)
 * 
 * @param {any} obj - Object to clone
 * @returns {any} - Cloned object
 * 
 * @example
 * const clone = cloneObj(complexObject);
 */
export default cloneObj;
export {cloneObj};

const cloneObj = (obj) => {
  if (obj === null || typeof obj !== 'object') return obj;
  
  // Use structuredClone if available (modern browsers)
  if (typeof structuredClone === 'function') {
    try {
      return structuredClone(obj);
    } catch {
      // Fall through to manual clone
    }
  }
  
  // Handle Date
  if (obj instanceof Date) return new Date(obj);
  
  // Handle Array
  if (Array.isArray(obj)) return obj.map(cloneObj);
  
  // Handle Object
  const clone = {};
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      clone[key] = cloneObj(obj[key]);
    }
  }
  return clone;
};