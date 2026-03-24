/**
 * setUrlParams - Update URL parameters without reload
 * @param {Object} params - Parameters to set
 * @param {boolean} replace - Replace history instead of push
 * 
 * @example
 * setUrlParams({ page: 2, filter: 'active' });
 * setUrlParams({ search: 'hello' }, true); // Replace history
 */
export default setUrlParams;
export {setUrlParams};

const setUrlParams = (params, replace = false) => {
  const url = new URL(window.location.href);
  Object.entries(params).forEach(([key, value]) => {
    if (value === null || value === undefined) {
      url.searchParams.delete(key);
    } else {
      url.searchParams.set(key, value);
    }
  });
  
  if (replace) {
    window.history.replaceState({}, '', url);
  } else {
    window.history.pushState({}, '', url);
  }
};