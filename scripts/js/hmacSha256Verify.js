/**
 * Verifies a HMAC SHA-256 signature.
 * @param {string} secret - Secret key.
 * @param {string} data - Signed data.
 * @param {Uint8Array} signatureBytes - Signature bytes.
 * @returns {Promise<boolean>} - True if valid.
 */
export default hmacSha256Verify;
export { hmacSha256Verify };

async function hmacSha256Verify(secret, data, signatureBytes) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"]
  );
  return crypto.subtle.verify("HMAC", key, signatureBytes, new TextEncoder().encode(data));
}
