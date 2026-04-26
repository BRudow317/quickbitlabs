import { Section, Flex, Heading, Text, Box, Card } from '@radix-ui/themes';
import { useAuth } from '@/auth/AuthContext';
import { FileUploader } from '@/components/FileUploader';
import { RegistryList } from '@/components/RegistryList';
import { UploadCloud, Shield } from 'lucide-react';

export function ImportSection() {
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
            You must be logged in to access the Import tools and Registry.
          </Text>
        </Card>
      </Section>
    );
  }

  return (
    <Section size="2">
      <Flex direction="column" gap="4">
        {/* Section Header */}
        <Flex align="center" gap="3">
          <Box style={{ 
            background: 'var(--accent-9)', 
            padding: '8px', 
            borderRadius: '8px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center'
          }}>
            <UploadCloud size={20} color="white" />
          </Box>
          <Flex direction="column">
            <Heading size="4">Data Ingestion Hub</Heading>
            <Text size="1" color="gray">Import local files into the federated registry</Text>
          </Flex>
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
          {/* Col 1: Uploader */}
          <FileUploader />

          {/* Col 2: Registry */}
          <RegistryList />
        </Flex>
      </Flex>
    </Section>
  );
}
