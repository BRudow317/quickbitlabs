import { BrowserRouter, Routes, Route, Navigate } from 'react-router';
import { normalizeBasename } from '@/utils/normalizeBasename';
import { HomePage } from '@/pages/HomePage';
import { DataMartPage } from '@/pages/DataMartPage';
import { MigrationPage } from '@/pages/MigrationPage';
import { Layout } from '@/layouts/Layout';
import { AppLayout } from '@/layouts/AppLayout';
import { ThemeProvider } from '@/context/ThemeContext';
import { BreakpointProvider } from '@/context/BreakpointContext';
import { DataProvider } from '@/context/DataContext';
import { AuthProvider } from '@/auth/AuthContext';
import '@/styles/fonts.css';
import { client } from '@/api/openapi/client.gen';
import { queryClient } from '@/context/QueryClientContext';
import { QueryClientProvider } from '@tanstack/react-query';
import { Theme } from '@radix-ui/themes';
import '@radix-ui/themes/styles.css';

// Attach bearer token from localStorage to every request
client.instance.interceptors.request.use((config) => {
  const token = localStorage.getItem('access_token');
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
                    {/* Public — login */}
                    <Route element={<Layout />}>
                      <Route index element={<HomePage />} />
                    </Route>

                    {/* Authenticated — nav layout */}
                    <Route element={<AppLayout />}>
                      <Route path="/datamart" element={<DataMartPage />} />
                      <Route path="/migration" element={<MigrationPage />} />
                    </Route>

                    <Route path="*" element={<Navigate to="/" replace />} />
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
