/* eslint-disable react-refresh/only-export-components */
import { createContext, useContext, useEffect, useMemo, useState } from "react";
import type { Dispatch, ReactNode, SetStateAction } from "react";
export { DataProvider, useData };

export type DataContextValue = {
  dataContext: string;
  setDataContext: Dispatch<SetStateAction<string>>;
};

const DataContext = createContext<DataContextValue | undefined>(undefined);

type DataDomSyncProps = {
  dataContext: string;
};

function DataDomSync({ dataContext }: DataDomSyncProps) {
  useEffect(() => {
    document.documentElement.setAttribute("data-context", dataContext);
  }, [dataContext]);

  return null;
}

type DataProviderProps = {
  children: ReactNode;
};

function DataProvider({ children }: DataProviderProps) {
  const [dataContext, setDataContext] = useState<string>("default");

  // Memoize the context value to optimize performance
  const value = useMemo(
    () => ({
      dataContext,
      setDataContext,
    }),
    [dataContext]
  );

  return (
    <DataContext.Provider value={value}>
      <DataDomSync dataContext={dataContext} />
      {children}
    </DataContext.Provider>
  );
}

function useData(): DataContextValue {
  const context = useContext(DataContext);
  if (!context) {
    throw new Error("useData must be used within a DataProvider");
  }
  return context;
}
