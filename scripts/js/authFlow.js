/**
 * AuthFlow process: handles user authentication and protected resource access.
 * 
 * @param {Request} request - The incoming HTTP request.
 * @param {Object} env - The environment variables, including JWT_SECRET.
 * 
 * @example
 * addEventListener("fetch", (event) => {
 *   event.respondWith(authFlow.fetch(event.request, { JWT_SECRET: "your-secret" }));
 * });
 * 
 */
export default authFlow;
export { authFlow };

import  * as AuthFlow from ".";
var authFlow = {
  async fetch(request, env) {
    const origin = request.headers.get("Origin") || "";
    const allowOrigin = AuthFlow.ALLOWED_ORIGINS.has(origin) ? origin : "";
    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: allowOrigin ? AuthFlow.corsHeaders(allowOrigin) : {} });
    }

    const url = new URL(request.url);

    // POST /auth/login
    if (url.pathname === "/auth/login" && request.method === "POST") {
      let body;
      try {
        body = await request.json();
      } catch {
        return AuthFlow.jsonResponse({ error: "Invalid JSON" }, { status: 400, origin: allowOrigin });
      }

      const username = String(body.username || "").trim();
      const password = String(body.password || "");

      // Never log passwords
      const expected = USERS[username];
      const ok = expected && password === expected;
      console.log("login_attempt", { username, ok });

      if (!ok) return AuthFlow.jsonResponse({ error: "Invalid credentials" }, { status: 401, origin: allowOrigin });

      const { token, payload } = await AuthFlow.createJwtHS256({
        secret: env.JWT_SECRET,
        payload: { sub: username, role: "demo-user" },
        ttlSeconds: 60 * 15, // 15 minutes
      });

      return json({ accessToken: token, expiresAt: payload.exp }, { origin: allowOrigin });
    }

    // GET /auth/me (requires Authorization: Bearer ...)
    if (url.pathname === "/auth/me" && request.method === "GET") {
      try {
        const auth = request.headers.get("Authorization") || "";
        const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
        if (!token) throw new Error("Missing token");

        const payload = await AuthFlow.verifyJwtHS256({ secret: env.JWT_SECRET, token });
        return AuthFlow.jsonResponse({ user: { username: payload.sub, role: payload.role }, exp: payload.exp }, { origin: allowOrigin });
      } catch (e) {
        return AuthFlow.jsonResponse({ error: "Unauthorized" }, { status: 401, origin: allowOrigin });
      }
    }

     // GET /api/secret (protected resource)
    if (url.pathname === "/api/secret" && request.method === "GET") {
      try {
        const auth = request.headers.get("Authorization") || "";
        const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
        if (!token) throw new Error("Missing token");

        const payload = await AuthFlow.verifyJwtHS256({ secret: env.JWT_SECRET, token });
        return AuthFlow.jsonResponse(
          { message: `Authorized as ${payload.sub}`, data: { proof: "This came from the protected API." } },
          { origin: allowOrigin }
        );
      } catch {
        return AuthFlow.jsonResponse({ error: "Unauthorized" }, { status: 401, origin: allowOrigin });
      }
    }

    return new Response("Not found", { status: 404 });
  },
};