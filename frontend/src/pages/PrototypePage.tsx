import { Container, Heading, Flex, Text, Box } from '@radix-ui/themes';
import { PrototypeSection } from '@/sections/PrototypeSection';
import { ImportSection } from '@/sections/ImportSection';
import { DataPreviewSection } from '@/sections/DataPreviewSection';
import { DataMartSection } from '@/sections/DataMartSection';

export function PrototypePage() {
  return (
    <Box>
      <Box style={{ background: 'var(--gray-2)', borderBottom: '1px solid var(--gray-4)' }}>
        <Container size="4">
          <Flex direction="column" gap="2" py="6">
            <Heading size="8" weight="bold">Prototype Lab</Heading>
            <Text color="gray" size="3">
              A sandbox for developing context-wired reusable sections.
            </Text>
          </Flex>
        </Container>
      </Box>

      <Container size="4">
        <Flex direction="column" gap="4" py="4">
          <DataMartSection />
          <PrototypeSection />
          <ImportSection />
          <DataPreviewSection />
        </Flex>
      </Container>
    </Box>
  );
}
