/**
 * Decodes a base64url string to bytes.
 * @param {string} b64url - base64url string.
 * @returns {Uint8Array} - Decoded bytes.
 */
export default base64UrlDecodeToBytes;
export { base64UrlDecodeToBytes };

function base64UrlDecodeToBytes(b64url) {
  const b64 = b64url.replace(/-/g, "+").replace(/_/g, "/") + "===".slice((b64url.length + 3) % 4);
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}
