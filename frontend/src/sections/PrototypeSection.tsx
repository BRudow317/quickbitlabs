import { Section, Heading, Flex, Text, Card, Code, Badge, Box, Grid } from '@radix-ui/themes';
import { useAuth } from '@/auth/AuthContext';
import { Shield, Database } from 'lucide-react';

export function PrototypeSection() {
  const { user, isAuthenticated } = useAuth();

  // Safety Gate: Enforce auth and context rules
  if (!isAuthenticated) {
    return (
      <Section size="2">
        <Card variant="surface">
          <Flex align="center" gap="2" style={{ color: 'var(--red-11)' }}>
            <Shield size={16} />
            <Text size="2" weight="bold">Authentication Required</Text>
          </Flex>
          <Text size="2" color="gray" mt="1">
            This section is protected and requires an active session.
          </Text>
        </Card>
      </Section>
    );
  }

  return (
    <Section size="2">
      <Flex direction="column" gap="4">
        <Flex justify="between" align="center">
          <Flex align="center" gap="3">
            <Box style={{ 
              background: 'var(--accent-9)', 
              padding: '8px', 
              borderRadius: '8px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center'
            }}>
              <Database size={20} color="white" />
            </Box>
            <Flex direction="column">
              <Heading size="4">Active Session Discovery</Heading>
              <Text size="1" color="gray">Context-wired Prototype Section</Text>
            </Flex>
          </Flex>
          <Badge color="green" variant="soft">Live Context</Badge>
        </Flex>

        <Card size="2">
          <Flex direction="column" gap="3">
            <Text size="2" weight="bold">Session Metadata</Text>
            <Grid columns="2" gap="3">
              <Flex direction="column" gap="1">
                <Text size="1" color="gray">Logged in as</Text>
                <Code variant="ghost">{user?.username || 'Unknown'}</Code>
              </Flex>
              <Flex direction="column" gap="1">
                <Text size="1" color="gray">Environment</Text>
                <Code variant="ghost">Development</Code>
              </Flex>
            </Grid>
          </Flex>
        </Card>

        <Box>
          <Text size="2" mb="2" weight="bold">Security Status</Text>
          <Code variant="soft" style={{ display: 'block', padding: '12px', overflowX: 'auto' }}>
            {JSON.stringify({
              isAuthenticated,
              userId: user?.username,
              roles: ['Developer'],
              accessLevel: 'Full'
            }, null, 2)}
          </Code>
        </Box>
      </Flex>
    </Section>
  );
}
