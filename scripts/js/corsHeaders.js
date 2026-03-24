/**
 * Builds CORS headers for a given origin.
 * @param {string} origin - Request origin.
 * @returns {Object} - Headers map.
 */
export default corsHeaders;
export { corsHeaders };

function corsHeaders(origin) {
  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Max-Age": "86400",
    "Vary": "Origin",
  };
}
