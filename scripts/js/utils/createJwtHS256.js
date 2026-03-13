import { base64UrlEncodeBytes } from "./base64UrlEncodeBytes.js";
import { base64UrlEncodeJson } from "./base64UrlEncodeJson.js";
import { hmacSha256Sign } from "./hmacSha256Sign.js";
/**
 * Creates a JWT signed with HS256.
 * @param {Object} params
 * @param {string} params.secret - The secret key used for signing.
 * @param {Object} params.payload - The payload to encode.
 * @param {number} params.ttlSeconds - Token TTL in seconds.
 * @returns {Promise<{token: string, payload: Object}>} - Token and full payload.
 */
export default createJwtHS256;
export { createJwtHS256 };

async function createJwtHS256({ secret, payload, ttlSeconds }) {
  const header = { alg: "HS256", typ: "JWT" };
  const now = Math.floor(Date.now() / 1000);
  const fullPayload = { ...payload, iat: now, exp: now + ttlSeconds };

  const encodedHeader = base64UrlEncodeJson(header);
  const encodedPayload = base64UrlEncodeJson(fullPayload);
  const signingInput = `${encodedHeader}.${encodedPayload}`;

  const sigBytes = await hmacSha256Sign(secret, signingInput);
  const encodedSig = base64UrlEncodeBytes(sigBytes);

  return { token: `${signingInput}.${encodedSig}`, payload: fullPayload };
}
