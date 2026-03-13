import { corsHeaders } from "./corsHeaders.js";
/**
 * Creates a JSON Response with optional CORS headers.
 * @param {Object} body - Response body.
 * @param {Object} [options]
 * @param {number} [options.status=200] - HTTP status.
 * @param {string} [options.origin=""] - Request origin.
 * @returns {Response} - JSON Response.
 */
export default jsonResponse;
export { jsonResponse };

function jsonResponse(body, { status = 200, origin = "" } = {}) {
  const headers = { "content-type": "application/json; charset=utf-8" };
  if (origin) Object.assign(headers, corsHeaders(origin));
  return new Response(JSON.stringify(body), { status, headers });
}
