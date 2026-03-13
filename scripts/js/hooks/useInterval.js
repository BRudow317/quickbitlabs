/**
 * setInterval as a hook (handles cleanup)
 * @param {Function} callback - Function to call
 * @param {number|null} delay - Interval delay (null to pause)
 * 
 * @example
 * const [count, setCount] = useState(0);
 * useInterval(() => setCount(c => c + 1), 1000); // Increment every second
 * useInterval(callback, isPaused ? null : 1000); // Pausable
 */
export const useInterval = (callback, delay) => {
  const { useEffect, useRef } = require('react');
  const savedCallback = useRef();

  useEffect(() => {
    savedCallback.current = callback;
  }, [callback]);

  useEffect(() => {
    if (delay === null) return;
    
    const tick = () => savedCallback.current();
    const id = setInterval(tick, delay);
    
    return () => clearInterval(id);
  }, [delay]);
};