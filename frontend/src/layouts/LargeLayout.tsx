import { Box, Container } from '@radix-ui/themes';
import { Outlet } from 'react-router';
import { Navbar } from '@/components/Navbar';
import type { LayoutBaseProps } from './SmallLayout';

/**
 * LargeLayout: Optimized for xl and xxl screens.
 * Generous padding and maximum size-4 containers.
 */
export function LargeLayout({ requireAuth }: LayoutBaseProps) {
  return (
    <Box style={{ minHeight: '100vh', background: 'var(--gray-a1)' }}>
      <Navbar />
      {requireAuth ? (
        <Container size="4" p="6">
          <Outlet />
        </Container>
      ) : (
        <Outlet />
      )}
    </Box>
  );
}
