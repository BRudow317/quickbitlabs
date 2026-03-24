/**
 * 
 * Auth_Util.jsx - Utility functions for JWT handling and secure storage
 * @module Auth_Util
 * 
 */
export {
    parseJwt,
    isJwtExpired,
    getJwtTimeRemaining,
    authHeader,
    secureStorage
};
/**
 * parseJwt - Decode JWT token payload (without verification)
 * @param {string} token - JWT token string
 * @returns {Object|null} - Decoded payload or null
 * 
 * @example
 * const payload = parseJwt(token);
 * console.log(payload.sub, payload.exp);
 */
const parseJwt = (token) => {
  try {
    const base64Url = token.split('.')[1];
    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
    const jsonPayload = decodeURIComponent(
      atob(base64)
        .split('')
        .map((c) => '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2))
        .join('')
    );
    return JSON.parse(jsonPayload);
  } catch (e) {
    console.error('Invalid JWT:', e);
    return null;
  }
};

/**
 * isJwtExpired - Check if JWT token is expired
 * @param {string} token - JWT token string
 * @param {number} bufferSeconds - Buffer time before actual expiry
 * @returns {boolean} - True if expired
 * 
 * @example
 * if (isJwtExpired(token)) { refreshToken(); }
 * if (isJwtExpired(token, 300))
 */
const isJwtExpired = (token, bufferSeconds = 0) => {
  const payload = parseJwt(token);
  if (!payload || !payload.exp) return true;
  
  const expiryTime = payload.exp * 1000; // Convert to milliseconds
  const bufferTime = bufferSeconds * 1000;
  return Date.now() >= expiryTime - bufferTime;
};

/**
 * getJwtTimeRemaining - Get time remaining until JWT expires
 * @param {string} token - JWT token string
 * @returns {Object} - Time remaining breakdown
 * 
 * @example
 * const remaining = getJwtTimeRemaining(token);
 * console.log(`Expires in ${remaining.minutes} minutes`);
 */
const getJwtTimeRemaining = (token) => {
  const payload = parseJwt(token);
  if (!payload || !payload.exp) return { expired: true, total: 0 };
  
  const remaining = (payload.exp * 1000) - Date.now();
  
  if (remaining <= 0) return { expired: true, total: 0 };
  
  return {
    expired: false,
    total: remaining,
    days: Math.floor(remaining / (1000 * 60 * 60 * 24)),
    hours: Math.floor((remaining % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60)),
    minutes: Math.floor((remaining % (1000 * 60 * 60)) / (1000 * 60)),
    seconds: Math.floor((remaining % (1000 * 60)) / 1000),
  };
};

/**
 * authHeader - Generate authorization header
 * @param {string} token - Auth token
 * @param {string} type - Token type (Bearer, Basic, etc.)
 * @returns {Object} - Headers object
 * 
 * @example
 * fetch(url, { headers: authHeader(token) });
 * fetch(url, { headers: { ...authHeader(token), 'Content-Type': 'application/json' } });
 */
const authHeader = (token, type = 'Bearer') => {
  return token ? { Authorization: `${type} ${token}` } : {};
};

/**
 * secureStorage - Encrypted storage wrapper (simple obfuscation)
 * Note: For sensitive data, use proper encryption libraries
 * 
 * @example
 * secureStorage.set('user', { id: 1, name: 'John' });
 * const user = secureStorage.get('user');
 * secureStorage.remove('user');
 */
const secureStorage = {
  _encode: (data) => btoa(encodeURIComponent(JSON.stringify(data))),
  _decode: (data) => JSON.parse(decodeURIComponent(atob(data))),
  
  set: (key, value) => {
    try {
      localStorage.setItem(`_sec_${key}`, secureStorage._encode(value));
      return true;
    } catch (e) {
      console.error('SecureStorage set error:', e);
      return false;
    }
  },
  
  get: (key) => {
    try {
      const item = localStorage.getItem(`_sec_${key}`);
      return item ? secureStorage._decode(item) : null;
    } catch (e) {
      console.error('SecureStorage get error:', e);
      return null;
    }
  },
  
  remove: (key) => localStorage.removeItem(`_sec_${key}`),
  
  clear: () => {
    Object.keys(localStorage)
      .filter((k) => k.startsWith('_sec_'))
      .forEach((k) => localStorage.removeItem(k));
  },
};
