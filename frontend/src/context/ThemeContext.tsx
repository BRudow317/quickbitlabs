/* eslint-disable react-refresh/only-export-components */
import { createContext, useContext, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
export { ThemeProvider, useTheme };

export type ThemeValue = {
  theme: string;
  toggleTheme: () => void;
  setTheme: (theme: string) => void;
};

const ThemeContext = createContext<ThemeValue | undefined>(undefined);
const THEMES = ["qbl-theme", "dark", "light"] as const;

type ThemeDomSyncProps = {
  theme: string;
};

function ThemeDomSync({ theme }: ThemeDomSyncProps) {
  useEffect(() => {
    // 1. Sync data attribute
    document.documentElement.setAttribute("data-theme", theme);
    
    // 2. Sync CSS classes
    // Remove all possible theme classes first
    THEMES.forEach(t => document.documentElement.classList.remove(t));
    
    // Add the current theme class
    document.documentElement.classList.add(theme);
  }, [theme]);

  return null;
}

type ThemeProviderProps = {
  children: ReactNode;
};

function ThemeProvider({ children }: ThemeProviderProps) {
  const [theme, setTheme] = useState<string>("light");

  const toggleTheme = () => {
    setTheme((curr) => {
      const index = THEMES.indexOf(curr as (typeof THEMES)[number]);
      const nextIndex = index === -1 ? 0 : (index + 1) % THEMES.length;
      return THEMES[nextIndex];
    });
  };

  // Memoize the context value to optimize performance
  const value = useMemo(() => ({ theme, toggleTheme, setTheme }), [theme]);

  return (
    <ThemeContext.Provider value={value}>
      <ThemeDomSync theme={theme} />
      {children}
    </ThemeContext.Provider>
  );
}

function useTheme(): ThemeValue {
  const context = useContext(ThemeContext);
  if (!context) {
    throw new Error("useTheme must be used within a ThemeProvider");
  }
  return context;
}
