/**
 * getDeviceInfo - Get device and browser information
 * @returns {Object} - Device info object
 * 
 * @example
 * const info = getDeviceInfo();
 * console.log(info.isMobile, info.browser, info.os);
 */
export default getDeviceInfo;
export {getDeviceInfo};

const getDeviceInfo = () => {

  const ua = navigator.userAgent;
  
  return {
    isMobile: /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(ua),
    isIOS: /iPad|iPhone|iPod/.test(ua),
    isAndroid: /Android/.test(ua),
    isSafari: /^((?!chrome|android).)*safari/i.test(ua),
    isChrome: /Chrome/.test(ua) && !/Edge/.test(ua),
    isFirefox: /Firefox/.test(ua),
    isEdge: /Edge/.test(ua),
    browser: ua.match(/(Chrome|Safari|Firefox|Edge|Opera|MSIE|Trident)[\/\s](\d+)/)?.[1] || 'Unknown',
    os: ua.match(/(Windows|Mac|Linux|Android|iOS)/i)?.[1] || 'Unknown',
    screenWidth: window.screen.width,
    screenHeight: window.screen.height,
    viewportWidth: window.innerWidth,
    viewportHeight: window.innerHeight,
    pixelRatio: window.devicePixelRatio,
    online: navigator.onLine,
    language: navigator.language,
    cookiesEnabled: navigator.cookieEnabled,
  };
};
