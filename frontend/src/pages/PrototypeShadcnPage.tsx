import { Container, Heading, Flex, Text, Box } from '@radix-ui/themes';
import { ShadcnDataMartSection } from '@/sections/ShadcnDataMartSection';
import { ShadcnImportSection } from '@/sections/ShadcnImportSection';
import { Rocket } from 'lucide-react';

export function PrototypeShadcnPage() {
  return (
    <Box>
      <Box style={{ background: 'var(--gray-2)', borderBottom: '1px solid var(--gray-4)' }}>
        <Container size="4">
          <Flex direction="column" gap="2" py="6">
            <div className="flex items-center gap-3">
              <Rocket className="h-8 w-8 text-primary" />
              <Heading size="8" weight="bold">Shadcn Lab</Heading>
            </div>
            <Text color="gray" size="3">
              Professional data tools built with Shadcn UI and Tailwind v4.
            </Text>
          </Flex>
        </Container>
      </Box>

      <Container size="4">
        <Flex direction="column" gap="4" py="4">
          
          {/* DataMart Section */}
          <ShadcnDataMartSection />

          {/* Import Section */}
          <ShadcnImportSection />

        </Flex>
      </Container>
    </Box>
  );
}
