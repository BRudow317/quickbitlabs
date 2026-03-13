/**
 * Session-only cache using sessionStorage.
 * Provides methods to set, get, remove, and clear items in sessionStorage.
 * 
 * Cache clears when window closes.
 * 
 * @example
 * cacheForSession.set('formData', { name: 'John' });
 * const formData = cacheForSession.get('formData');
 */
export const cacheForSession = {
  set: (key, value) => {
    try {
      sessionStorage.setItem(key, JSON.stringify(value));
      return true;
    } catch (e) {
      console.error('cacheForSession error:', e);
      return false;
    }
  },
  
  get: (key, defaultValue = null) => {
    try {
      const item = sessionStorage.getItem(key);
      return item ? JSON.parse(item) : defaultValue;
    } catch (e) {
      console.error('cacheForSession error:', e);
      return defaultValue;
    }
  },
  
  remove: (key) => sessionStorage.removeItem(key),
  
  clear: () => sessionStorage.clear(),
  
  keys: () => Object.keys(sessionStorage),

};