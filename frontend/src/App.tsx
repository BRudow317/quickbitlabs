import { BrowserRouter, Routes, Route, Navigate } from 'react-router';
import { normalizeBasename } from '@/utils/normalizeBasename';
import { HomePage } from '@/pages/HomePage';
import { DataMartPage } from '@/pages/DataMartPage';
import { MigrationPage } from '@/pages/MigrationPage';
import { ImportPage } from '@/pages/ImportPage';
import { ContactsPage } from '@/pages/ContactsPage';
import { ProfilePage } from '@/pages/ProfilePage';
import { Layout } from '@/layouts/Layout';
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
                    {/* Public */}
                    <Route element={<Layout />}>
                      <Route index element={<HomePage />} />
                    </Route>

                    {/* Authenticated */}
                    <Route element={<Layout requireAuth />}>
                      <Route path="/datamart" element={<DataMartPage />} />
                      <Route path="/migration" element={<MigrationPage />} />
                      <Route path="/import" element={<ImportPage />} />
                      <Route path="/contacts" element={<ContactsPage />} />
                      <Route path="/profile" element={<ProfilePage />} />
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
