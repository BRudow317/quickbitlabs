import { base64UrlEncodeBytes } from "./base64UrlEncodeBytes.js";
/**
 * Encodes a JSON object to a base64url string.
 * @param {Object} obj - JSON object to encode.
 * @returns {string} - base64url string.
 */
export default base64UrlEncodeJson;
export { base64UrlEncodeJson };

function base64UrlEncodeJson(obj) {
  const bytes = new TextEncoder().encode(JSON.stringify(obj));
  return base64UrlEncodeBytes(bytes);
}
