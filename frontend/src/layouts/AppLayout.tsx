import React from 'react';
import { Box, Button, Flex, Heading, Text } from '@radix-ui/themes';
import { Link, Outlet, useNavigate } from 'react-router';
import { useAuth } from '@/auth/AuthContext';

export function AppLayout(): React.JSX.Element {
  const { user, logout, isAuthenticated } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate('/');
  };

  if (!isAuthenticated) {
    navigate('/');
    return <></>;
  }

  return (
    <Box style={{ minHeight: '100vh', background: 'var(--gray-a1)' }}>
      <Box
        style={{
          borderBottom: '1px solid var(--gray-a5)',
          background: 'var(--color-background)',
          position: 'sticky',
          top: 0,
          zIndex: 10,
        }}
      >
        <Flex px="5" py="3" align="center" justify="between">
          <Flex align="center" gap="6">
            <Heading size="4" style={{ letterSpacing: '-0.5px' }}>
              QuickBitLabs
            </Heading>
            <Flex gap="4">
              <Link to="/datamart" style={{ textDecoration: 'none' }}>
                <Text size="2" color="gray" style={{ cursor: 'pointer' }}>
                  DataMart
                </Text>
              </Link>
              <Link to="/migration" style={{ textDecoration: 'none' }}>
                <Text size="2" color="gray" style={{ cursor: 'pointer' }}>
                  Migration
                </Text>
              </Link>
              <Link to="/import" style={{ textDecoration: 'none' }}>
                <Text size="2" color="gray" style={{ cursor: 'pointer' }}>
                  Import
                </Text>
              </Link>
            </Flex>
          </Flex>
          <Flex align="center" gap="3">
            {user && (
              <Text size="2" color="gray">
                {user.username}
              </Text>
            )}
            <Button size="1" variant="soft" onClick={handleLogout}>
              Logout
            </Button>
          </Flex>
        </Flex>
      </Box>
      <Box p="5">
        <Outlet />
      </Box>
    </Box>
  );
}
