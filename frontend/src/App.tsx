import { BrowserRouter, Routes, Route, Navigate } from 'react-router';
import { normalizeBasename } from '@/utils/normalizeBasename';
import { HomePage } from '@/pages/HomePage';
import { MigrationPage } from '@/pages/MigrationPage';
import { ContactsPage } from '@/pages/ContactsPage';
import { ProfilePage } from '@/pages/ProfilePage';
import { PrototypePage } from '@/pages/PrototypePage';
import { PrototypeShadcnPage } from '@/pages/PrototypeShadcnPage';
import { QueryBuilderPage } from '@/pages/QueryBuilderPage';
import { AdminPage } from '@/pages/AdminPage';
import { Layout } from '@/layouts/Layout';
import { ThemeProvider } from '@/context/ThemeContext';
import { BreakpointProvider } from '@/context/BreakpointContext';
import { DataProvider } from '@/context/DataContext';
import { AuthProvider } from '@/auth/AuthContext';
import { ToastProvider } from '@/context/ToastContext';
import { Toaster } from '@/components/radix/AlertToaster';
import { ApiErrorInterceptor } from '@/components/ApiErrorInterceptor';
import { TooltipProvider } from '@/components/ui/tooltip';
import { client } from '@/api/openapi/client.gen';
import { queryClient } from '@/context/QueryClientContext';
import { QueryClientProvider } from '@tanstack/react-query';
import { Theme } from '@radix-ui/themes';
import '@radix-ui/themes/styles.css';
import '@/styles/index.css';
import '@/styles/global.css'

// Send cookies on cross-origin requests (needed for the HttpOnly refresh_token cookie)
client.instance.defaults.withCredentials = true;

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
                <TooltipProvider>
                  <ToastProvider>
                    <ApiErrorInterceptor />
                    <Toaster />
                    <ThemeProvider>
                      <Routes>
                        {/* Public */}
                        <Route element={<Layout />}>
                          <Route index element={<HomePage />} />
                        </Route>

                        {/* Authenticated */}
                        <Route element={<Layout requireAuth />}>
                          <Route path="/migration" element={<MigrationPage />} />
                          <Route path="/query-builder" element={<QueryBuilderPage />} />
                          <Route path="/contacts" element={<ContactsPage />} />
                          <Route path="/profile" element={<ProfilePage />} />
                          <Route path="/prototype" element={<PrototypePage />} />
                          <Route path="/prototype-shadcn" element={<PrototypeShadcnPage />} />
                          <Route path="/admin" element={<AdminPage />} />
                        </Route>

                        <Route path="*" element={<Navigate to="/" replace />} />
                      </Routes>
                    </ThemeProvider>
                  </ToastProvider>
                </TooltipProvider>
              </Theme>
            </DataProvider>
          </QueryClientProvider>
        </BreakpointProvider>
      </BrowserRouter>
    </AuthProvider>
  );
}

export default App;
