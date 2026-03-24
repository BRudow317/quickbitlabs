import { base64UrlDecodeToBytes } from "./base64UrlDecodeToBytes.js";
import { hmacSha256Verify } from "./hmacSha256Verify.js";
/**
 * Verifies a JWT signed with HS256.
 * @param {Object} params
 * @param {string} params.secret - The secret key used for verification.
 * @param {string} params.token - The JWT to verify.
 * 
 * @returns {Promise<Object>} - The decoded payload if verification is successful.
 * 
 * @throws {Error} - If the token is malformed, has a bad signature, or is expired.
 * 
 * @example
 * const payload = await verifyJwtHS256(
 * { secret: "mysecret", 
 * token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." 
 * });  
 */
export default verifyJwtHS256;
export {verifyJwtHS256};

async function verifyJwtHS256({ secret, token }) {
  const parts = token.split(".");
  if (parts.length !== 3) throw new Error("Malformed token");
  const [h, p, s] = parts;

  const signingInput = `${h}.${p}`;
  const sigBytes = base64UrlDecodeToBytes(s);

  const ok = await hmacSha256Verify(secret, signingInput, sigBytes);
  if (!ok) throw new Error("Bad signature");

  const payloadBytes = base64UrlDecodeToBytes(p);
  const payload = JSON.parse(new TextDecoder().decode(payloadBytes));

  const now = Math.floor(Date.now() / 1000);
  if (typeof payload.exp !== "number" || payload.exp <= now) throw new Error("Expired token");

  return payload;
}
