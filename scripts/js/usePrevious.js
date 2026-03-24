import { useRef, useEffect } from 'react';

/**
 * usePrevious - Get previous value of a variable
 * @param {any} value - Current value
 * @returns {any} - Previous value
 * 
 * @example
 * const [count, setCount] = useState(0);
 * const prevCount = usePrevious(count);
 * // prevCount is the value from last render
 */
export const usePrevious = (value) => {
  const ref = useRef();
  
  useEffect(() => {
    ref.current = value;
  }, [value]);
  
  return ref.current;
};
