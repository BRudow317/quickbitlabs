/**
 * Cache data with automatic expiration
 * 
 * @example
 * cacheForTime.set('apiData', data, 5 * 60 * 1000); // 5 minutes
 * const data = cacheForTime.get('apiData');
 * const isValid = cacheForTime.isValid('apiData');
 * 
 */

export default cacheForTime;
export {cacheForTime};

const cacheForTime = {
  set: (key, value, ttlMs) => {
    const item = {
      value,
      expiry: Date.now() + ttlMs,
    };
    localStorage.setItem(key, JSON.stringify(item));
  },
  
  get: (key) => {
    const itemStr = localStorage.getItem(key);
    if (!itemStr) return null;
    
    try {
      const item = JSON.parse(itemStr);
      if (Date.now() > item.expiry) {
        localStorage.removeItem(key);
        return null;
      }
      return item.value;
    } catch {
      return null;
    }
  },
  
  remove: (key) => localStorage.removeItem(key),
  
  isValid: (key) => {
    const itemStr = localStorage.getItem(key);
    if (!itemStr) return false;
    try {
      const item = JSON.parse(itemStr);
      return Date.now() < item.expiry;
    } catch {
      return false;
    }
  },
};