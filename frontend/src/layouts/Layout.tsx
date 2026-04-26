import { Navigate } from 'react-router';
import { useAuth } from '@/auth/AuthContext';
import { useBreakpoint } from '@/context/BreakpointContext';
import { SmallLayout } from './SmallLayout';
import { MediumLayout } from './MediumLayout';
import { LargeLayout } from './LargeLayout';

interface LayoutProps {
  requireAuth?: boolean;
}

/**
 * Layout: Orchestrator that switches between responsive sub-layouts
 * based on the BreakpointContext.
 */
export function Layout({ requireAuth = false }: LayoutProps) {
  const { isAuthenticated, isLoading } = useAuth();
  const screenSize = useBreakpoint();

  if (isLoading) return <></>;

  if (requireAuth && !isAuthenticated) {
    return <Navigate to="/" replace />;
  }

  // Switch layouts based on breakpoint mapping
  // small: xsm, sm
  // medium: md, lg
  // large: xl, xxl
  switch (screenSize) {
    case 'xsm':
    case 'sm':
      return <SmallLayout requireAuth={requireAuth} />;
    case 'md':
    case 'lg':
      return <MediumLayout requireAuth={requireAuth} />;
    case 'xl':
    case 'xxl':
      return <LargeLayout requireAuth={requireAuth} />;
    default:
      // Fallback to Medium if unknown
      return <MediumLayout requireAuth={requireAuth} />;
  }
}
