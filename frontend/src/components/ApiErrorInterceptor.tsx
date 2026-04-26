import { useEffect } from 'react';
import { client } from '@/api/openapi/client.gen';
import { useToast } from '@/context/ToastContext';
import axios from 'axios';

export function ApiErrorInterceptor() {
  const { toast } = useToast();

  useEffect(() => {
    const interceptor = client.instance.interceptors.response.use(
      (response) => response,
      (error) => {
        if (axios.isAxiosError(error)) {
          const message = error.response?.data?.detail || error.message || 'An unexpected error occurred';
          const status = error.response?.status;
          
          // Don't toast for 401s if they are handled by auth redirect logic
          // but for now, let's toast everything that isn't a 401
          if (status !== 401) {
            toast.error(message);
          }
        } else {
          toast.error('An unexpected error occurred');
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
