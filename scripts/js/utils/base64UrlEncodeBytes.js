/**
 * Encodes bytes to a base64url string.
 * @param {Uint8Array} bytes - Bytes to encode.
 * @returns {string} - base64url string.
 */
export default base64UrlEncodeBytes;
export { base64UrlEncodeBytes };

function base64UrlEncodeBytes(bytes) {
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}
