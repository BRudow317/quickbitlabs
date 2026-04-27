import { Box, Container } from '@radix-ui/themes';
import { Outlet } from 'react-router';
import { Navbar } from '@/components/radix/Navbar';

export interface LayoutBaseProps {
  requireAuth?: boolean;
}

/**
 * SmallLayout: Optimized for xsm and sm screens.
 * Uses tighter padding and fluid containers.
 */
export function SmallLayout({ requireAuth }: LayoutBaseProps) {
  return (
    <Box style={{ minHeight: '100vh', background: 'var(--gray-a1)' }}>
      <Navbar />
      {requireAuth ? (
        <Container size="1" px="3" py="4">
          <Outlet />
        </Container>
      ) : (
        <Outlet />
      )}
    </Box>
  );
}
