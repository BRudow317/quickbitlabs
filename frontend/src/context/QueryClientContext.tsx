import { QueryClient } from '@tanstack/react-query';

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1, // Don't spam the server if it fails
      refetchOnWindowFocus: false, // Prevents annoying jumps while developing
    },
  },
});