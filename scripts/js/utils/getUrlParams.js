/**
 * getUrlParams - Parse URL query parameters
 * @param {string} url - URL to parse (defaults to current)
 * @returns {Object} - Parsed parameters
 * 
 * @example
 * const params = getUrlParams(); // Current URL
 * const params = getUrlParams('https://example.com?foo=bar&baz=123');
 * // { foo: 'bar', baz: '123' }
 */
export default getUrlParams;
export {getUrlParams};

const getUrlParams = (url = window.location.href) => {
  const params = new URL(url).searchParams;
  return Object.fromEntries(params.entries());
};
