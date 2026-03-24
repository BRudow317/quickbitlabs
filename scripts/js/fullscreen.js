/**
 * fullscreen - Toggle or control fullscreen mode
 * @param {HTMLElement} element - Element to fullscreen (default: document)
 * @param {boolean} enter - true to enter, false to exit, undefined to toggle
 * 
 * @example
 * fullscreen(); // Toggle document fullscreen
 * fullscreen(videoElement, true); // Enter fullscreen for video
 * fullscreen(null, false); // Exit fullscreen
 */
const fullscreen = (element = document.documentElement, enter) => {
  const isFullscreen = !!document.fullscreenElement;
  const shouldEnter = enter ?? !isFullscreen;
  
  if (shouldEnter && !isFullscreen) {
    element.requestFullscreen?.() || 
    element.webkitRequestFullscreen?.() || 
    element.mozRequestFullScreen?.();
  } else if (!shouldEnter && isFullscreen) {
    document.exitFullscreen?.() || 
    document.webkitExitFullscreen?.() || 
    document.mozCancelFullScreen?.();
  }
};
export default fullscreen;
export {fullscreen};