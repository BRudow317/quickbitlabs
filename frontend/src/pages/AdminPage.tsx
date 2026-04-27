import { Navigate } from 'react-router';
import {
  Box, Callout, Card, Flex, Heading, Text, Badge,
} from '@radix-ui/themes';
import { ShieldCheck, ShieldAlert } from 'lucide-react';
import { useAuth } from '@/auth/AuthContext';
import type { UserOut } from '@/api/openapi';

// role is added server-side; SDK needs regeneration to reflect it
type UserWithRole = UserOut & { role?: string };

export function AdminPage() {
  const { isAuthenticated, isAdmin, user: rawUser } = useAuth();
  const user = rawUser as UserWithRole | null;

  if (!isAuthenticated) return <Navigate to="/" replace />;

  if (!isAdmin) {
    return (
      <Box>
        <Callout.Root color="red" size="2">
          <Callout.Icon><ShieldAlert size={16} /></Callout.Icon>
          <Callout.Text>
            You do not have permission to access this page. Admin role required.
          </Callout.Text>
        </Callout.Root>
      </Box>
    );
  }

  return (
    <Flex direction="column" gap="5">
      <Flex align="center" gap="2">
        <ShieldCheck size={20} />
        <Heading size="5">Admin Panel</Heading>
        <Badge color="amber" variant="soft">admin</Badge>
      </Flex>

      <Card size="3">
        <Flex direction="column" gap="3">
          <Heading size="3">Session</Heading>
          <Flex direction="column" gap="1">
            <Text size="1" color="gray" weight="bold">LOGGED IN AS</Text>
            <Text size="3">{user?.username}</Text>
          </Flex>
          <Flex direction="column" gap="1">
            <Text size="1" color="gray" weight="bold">ROLE</Text>
            <Badge color="amber" variant="soft" style={{ width: 'fit-content' }}>
              {user?.role ?? 'admin'}
            </Badge>
          </Flex>
        </Flex>
      </Card>

      <Card size="3">
        <Flex direction="column" gap="2">
          <Heading size="3">Coming Soon</Heading>
          <Text size="2" color="gray">
            User management, system health, and audit logs will appear here.
            Assign the <code>admin</code> role in the database to grant access:
          </Text>
          <Box style={{ background: 'var(--gray-3)', borderRadius: 'var(--radius-2)', padding: 'var(--space-3)' }}>
            <Text size="2" style={{ fontFamily: 'monospace', whiteSpace: 'pre' }}>
              UPDATE "USER" SET ROLE = 'admin' WHERE USERNAME = 'your_username';
            </Text>
          </Box>
        </Flex>
      </Card>
    </Flex>
  );
}
