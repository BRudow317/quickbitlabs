import React from 'react';
import { Box, Container } from '@radix-ui/themes';
import { Navigate, Outlet } from 'react-router';
import { useAuth } from '@/auth/AuthContext';
import { Navbar } from '@/components/Navbar';

interface LayoutProps {
  requireAuth?: boolean;
}

export function Layout({ requireAuth = false }: LayoutProps): React.JSX.Element {
  const { isAuthenticated, isLoading } = useAuth();

  if (isLoading) return <></>;

  if (requireAuth && !isAuthenticated) {
    return <Navigate to="/" replace />;
  }

  return (
    <Box style={{ minHeight: '100vh', background: 'var(--gray-a1)' }}>
      <Navbar />
      {requireAuth ? (
        <Container size="4" p="5">
          <Outlet />
        </Container>
      ) : (
        <Outlet />
      )}
    </Box>
  );
}
