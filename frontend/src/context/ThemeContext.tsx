/* eslint-disable react-refresh/only-export-components */
import { createContext, useContext, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
export { ThemeProvider, useTheme };

export type ThemeValue = {
  theme: string;
  toggleTheme: () => void;
};

const ThemeContext = createContext<ThemeValue | undefined>(undefined);
const THEMES = ["my-theme", "dark", "light", "git"] as const;

type ThemeDomSyncProps = {
  theme: string;
};

function ThemeDomSync({ theme }: ThemeDomSyncProps) {
  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
  }, [theme]);

  return null;
}

type ThemeProviderProps = {
  children: ReactNode;
};

function ThemeProvider({ children }: ThemeProviderProps) {
  const [theme, setTheme] = useState<string>("git");

  const toggleTheme = () => {
    setTheme((curr) => {
      const index = THEMES.indexOf(curr as (typeof THEMES)[number]);
      const nextIndex = index === -1 ? 0 : (index + 1) % THEMES.length;
      return THEMES[nextIndex];
    });
  };
// Memoize the context value to optimize performance
  const value = useMemo(() => ({ theme, toggleTheme }), [theme]);

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
