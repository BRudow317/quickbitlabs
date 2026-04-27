import { useEffect } from 'react';
import { client } from '@/api/openapi/client.gen';
import { useToast } from '@/context/ToastContext';
import axios, { type InternalAxiosRequestConfig } from 'axios';

// Shared refresh promise — concurrent 401s reuse the same refresh call
let refreshPromise: Promise<string> | null = null;

async function attemptRefresh(): Promise<string> {
  if (refreshPromise) return refreshPromise;

  refreshPromise = client.instance
    .post<{ access_token: string }>('/api/auth/refresh', {})
    .then(({ data }) => {
      const token = data.access_token;
      localStorage.setItem('access_token', token);
      client.instance.defaults.headers.common.Authorization = `Bearer ${token}`;
      return token;
    })
    .finally(() => {
      refreshPromise = null;
    });

  return refreshPromise;
}

export function ApiErrorInterceptor() {
  const { toast } = useToast();

  useEffect(() => {
    const interceptor = client.instance.interceptors.response.use(
      (response) => response,
      async (error) => {
        if (!axios.isAxiosError(error)) {
          toast.error('An unexpected error occurred');
          return Promise.reject(error);
        }

        const status = error.response?.status;
        const originalConfig = error.config as InternalAxiosRequestConfig & { _retry?: boolean };
        const isRefreshUrl = originalConfig.url?.includes('/api/auth/refresh');

        // --- 401: attempt silent refresh then retry ---
        // Never retry the refresh endpoint itself to avoid infinite loops.
        if (status === 401 && !originalConfig._retry && !isRefreshUrl) {
          originalConfig._retry = true;
          try {
            const newToken = await attemptRefresh();
            if (originalConfig.headers) {
              originalConfig.headers.Authorization = `Bearer ${newToken}`;
            }
            return client.instance(originalConfig);
          } catch {
            // Refresh failed — clear token and signal the app to log out
            localStorage.removeItem('access_token');
            delete client.instance.defaults.headers.common.Authorization;
            window.dispatchEvent(new CustomEvent('auth:session-expired'));
            return Promise.reject(error);
          }
        }

        // --- All other errors ---
        if (status !== 401) {
          const message = error.response?.data?.detail || error.message || 'An unexpected error occurred';
          toast.error(message);
        }

        return Promise.reject(error);
      }
    );

    return () => {
      client.instance.interceptors.response.eject(interceptor);
    };
  }, [toast]);

  return null;
}
