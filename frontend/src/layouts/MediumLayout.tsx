import { Box, Container } from '@radix-ui/themes';
import { Outlet } from 'react-router';
import { Navbar } from '@/components/radix/Navbar';
import type { LayoutBaseProps } from './SmallLayout';

/**
 * MediumLayout: Optimized for md and lg screens.
 * Balanced padding and size-3 containers.
 */
export function MediumLayout({ requireAuth }: LayoutBaseProps) {
  return (
    <Box style={{ minHeight: '100vh', background: 'var(--gray-a1)' }}>
      <Navbar />
      {requireAuth ? (
        <Container size="3" p="5">
          <Outlet />
        </Container>
      ) : (
        <Outlet />
      )}
    </Box>
  );
}
