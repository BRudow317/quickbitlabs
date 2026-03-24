import { useEffect } from 'react';

/**
 * useOnClickOutside - Detect clicks outside an element
 * @param {React.RefObject} ref - Ref to the element
 * @param {Function} handler - Callback when clicked outside
 * 
 * @example
 * const modalRef = useRef();
 * useOnClickOutside(modalRef, () => setIsOpen(false));
 * return <div ref={modalRef}>Modal content</div>;
 */
export const useOnClickOutside = (ref, handler) => {
  useEffect(() => {
    const listener = (event) => {
      if (!ref.current || ref.current.contains(event.target)) return;
      handler(event);
    };

    document.addEventListener('mousedown', listener);
    document.addEventListener('touchstart', listener);

    return () => {
      document.removeEventListener('mousedown', listener);
      document.removeEventListener('touchstart', listener);
    };
  }, [ref, handler]);
};
