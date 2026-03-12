import {
  BrowserRouter,
  Routes,
  Route,
  Navigate,
  // useParams,
} from "react-router";
import { normalizeBasename } from "@/utils/normalizeBasename";
import { HomePage } from "@/pages/HomePage";
import { TablePage } from "@/pages/TablePage";
import { Layout } from "@/layouts/Layout";
import { ThemeProvider } from "@/context/ThemeContext";
import { BreakpointProvider } from "@/context/BreakpointContext";
import { DataProvider } from "@/context/DataContext";
import { AuthProvider } from "@/auth/AuthContext";
// import "@/styles/ColorTokens.css";
// import "@/styles/styles.css";
import "@/styles/fonts.css";
import { client } from "./api/client.gen";
import { queryClient } from "@/context/QueryClientContext";
import { QueryClientProvider } from '@tanstack/react-query';
import { Theme } from '@radix-ui/themes';
import '@radix-ui/themes/styles.css';
// Attach the interceptor to the singleton's Axios instance
client.instance.interceptors.request.use((config) => {
  const token = localStorage.getItem("elysium_token");
  if (token && config.headers) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

export function App() {
  const basename = normalizeBasename(import.meta.env.BASE_URL);

  return (
    <AuthProvider>
      <BrowserRouter basename={basename}>
        <BreakpointProvider>
        <QueryClientProvider client={queryClient}>
          <DataProvider>
          <Theme>
          
            <ThemeProvider>
              <Routes>
                <Route path="/" element={<Layout />}>
                  <Route index element={<HomePage />} />
                  <Route path="/table" element={<TablePage />} />
                  <Route path="*" element={<Navigate to="/" replace />} />
                </Route>
              </Routes>
            </ThemeProvider>
          
          </Theme>
          </DataProvider>
          </QueryClientProvider>
        </BreakpointProvider>
      </BrowserRouter>
    </AuthProvider>
  );
}

export default App;
