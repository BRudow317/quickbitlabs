/**
 * Signs a string with HMAC SHA-256.
 * @param {string} secret - Secret key.
 * @param {string} data - Data to sign.
 * @returns {Promise<Uint8Array>} - Signature bytes.
 */
export default hmacSha256Sign;
export { hmacSha256Sign };

async function hmacSha256Sign(secret, data) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(data));
  return new Uint8Array(sig);
}
