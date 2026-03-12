/**
 * BreakpointContext.jsx
 * @description
 * Provides a React Context for managing and accessing screen size breakpoints.
 * 
 * @example
 *  import { BreakpointProvider, useBreakpoint } from './BreakpointContext';
 *  
 *  const screenSize = useBreakpoint();
 *  return <div>Current Screen Size: {screenSize}</div>;
 * 
 * 
 */

/* eslint-disable react-refresh/only-export-components */
import { useState, useEffect, createContext, useContext } from "react";
import type { ReactNode } from "react";

export { BreakpointProvider, useBreakpoint };

export type ScreenSize = "xsm" | "sm" | "md" | "lg" | "xl" | "xxl" | "unknown";

const BreakpointContext = createContext<ScreenSize | undefined>(undefined);

const getScreenSize = (): ScreenSize => {
  if (window.matchMedia("(width <= 480px)").matches) return "xsm";
  else if (window.matchMedia("(480px < width <= 720px)").matches) return "sm";
  else if (window.matchMedia("(720px < width <= 960px)").matches) return "md";
  else if (window.matchMedia("(960px < width <= 1200px)").matches) return "lg";
  else if (window.matchMedia("(1200px < width <= 1600px)").matches) return "xl";
  else if (window.matchMedia("(width > 1600px)").matches) return "xxl";
  else return "unknown";
};

type BreakpointProviderProps = {
  children: ReactNode;
};

function BreakpointProvider({ children }: BreakpointProviderProps) {
  const [screenSize, setScreenSize] = useState<ScreenSize>(getScreenSize());

  useEffect(() => {
    const handler = () => setScreenSize(getScreenSize());
    window.addEventListener("resize", handler);
    return () => window.removeEventListener("resize", handler);
  }, []);

  return (
    <BreakpointContext.Provider value={screenSize}>
      {children}
    </BreakpointContext.Provider>
  );

};

// Hook to use elsewhere
function useBreakpoint(): ScreenSize {
  const context = useContext(BreakpointContext);
  if (!context) {
    throw new Error("useBreakpoint must be used within a BreakpointProvider");
  }
  return context;
}

