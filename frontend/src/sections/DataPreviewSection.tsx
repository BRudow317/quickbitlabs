import { Section, Flex, Heading, Text, Box, Card, Badge } from '@radix-ui/themes';
import { useAuth } from '@/auth/AuthContext';
import { DataTable } from '@/components/radix/DataTable';
import { Table, Shield } from 'lucide-react';

// Mock data for demonstration
const MOCK_DATA = [
  { id: 1, name: 'Oracle DB', type: 'Database', status: 'Healthy', lastSync: new Date() },
  { id: 2, name: 'Salesforce', type: 'SaaS', status: 'Healthy', lastSync: new Date() },
  { id: 3, name: 'S3 Bucket', type: 'Storage', status: 'Warning', lastSync: new Date() },
  { id: 4, name: 'Local CSV', type: 'File', status: 'Healthy', lastSync: new Date() },
];

export function DataPreviewSection() {
  const { isAuthenticated } = useAuth();

  if (!isAuthenticated) {
    return (
      <Section size="2">
        <Card variant="surface">
          <Flex align="center" gap="2" style={{ color: 'var(--red-11)' }}>
            <Shield size={16} />
            <Text size="2" weight="bold">Authentication Required</Text>
          </Flex>
          <Text size="2" color="gray" mt="1">
            Sign in to view the Dynamic Data Table prototype.
          </Text>
        </Card>
      </Section>
    );
  }

  return (
    <Section size="2">
      <Flex direction="column" gap="4">
        {/* Section Header */}
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
              <Table size={20} color="white" />
            </Box>
            <Flex direction="column">
              <Heading size="4">Dynamic Data Lab</Heading>
              <Text size="1" color="gray">Generic TanStack Table implementation</Text>
            </Flex>
          </Flex>
          <Badge color="blue" variant="soft">Preview Mode</Badge>
        </Flex>

        {/* 
            FLEX-STANDARD: 
            Row on Desktop, Column on Mobile 
        */}
        <Flex 
          direction={{ initial: 'column', md: 'row' }} 
          gap="5" 
          align="start"
        >
          {/* Col 1: Description and Controls (Column) */}
          <Flex direction="column" gap="3" style={{ flex: '1 1 300px' }}>
            <Card size="2">
              <Heading size="3" mb="2">Table Features</Heading>
              <Flex direction="column" gap="2">
                <Text size="2">1. **Auto-Discovery:** Infers columns from object keys.</Text>
                <Text size="2">2. **Virtualization:** Uses `ScrollArea` for large sets.</Text>
                <Text size="2">3. **Typed:** Full TypeScript support for generic data types.</Text>
                <Text size="2">4. **Flexible:** Integrates with Radix Table primitives.</Text>
              </Flex>
            </Card>
          </Flex>

          {/* Col 2: The Table Component (Column) */}
          <Flex direction="column" gap="3" style={{ flex: '3 1 600px', width: '100%' }}>
            <DataTable data={MOCK_DATA} />
          </Flex>
        </Flex>
      </Flex>
    </Section>
  );
}
