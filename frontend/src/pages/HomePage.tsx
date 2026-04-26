import { Box, Card, Container, Flex, Grid, Heading, Section, Text } from '@radix-ui/themes';
import { Database, ArrowRightLeft, Shield } from 'lucide-react';

export function HomePage() {
  return (
    <Box>
      {/* Hero */}
      <Box style={{ background: 'linear-gradient(160deg, var(--accent-2) 0%, var(--gray-a1) 60%)' }}>
        <Container size="3">
          <Section size="4">
            <Flex direction="column" align="center" gap="4">
              <Box style={{
                width: 64, height: 64, borderRadius: 14,
                background: 'var(--accent-9)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <Database size={32} color="white" />
              </Box>
              <Flex direction="column" align="center" gap="2">
                <Heading size="9" align="center" weight="bold" style={{ letterSpacing: '-1px' }}>
                  QuickBitLabs
                </Heading>
                <Text size="5" align="center" color="gray" weight="medium">
                  Universal Data Integration Platform
                </Text>
                <Text size="3" align="center" color="gray" style={{ maxWidth: 520, lineHeight: '1.6' }}>
                  Connect Oracle, Salesforce, and any data source through a single federated interface.
                  Query anything. Migrate everything. No plugin knowledge required.
                </Text>
              </Flex>
              <Text size="2" color="gray">
                Use the Sign In or Register buttons in the top right to get started.
              </Text>
            </Flex>
          </Section>
        </Container>
      </Box>

      {/* Features */}
      <Container size="3">
        <Section size="3">
          <Flex direction="column" gap="5">
            <Heading size="4" align="center" color="gray">
              Built for enterprise data teams
            </Heading>
            <Grid columns={{ initial: '1', sm: '3' }} gap="4">
              <Card size="3">
                <Flex direction="column" gap="3">
                  <Box style={{
                    width: 40, height: 40, borderRadius: 8,
                    background: 'var(--accent-3)',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                  }}>
                    <Database size={20} color="var(--accent-11)" />
                  </Box>
                  <Heading size="3">Data Federation</Heading>
                  <Text size="2" color="gray">
                    Query across Oracle, Salesforce, and file sources with a unified Catalog interface.
                    The service layer is completely blind to underlying systems.
                  </Text>
                </Flex>
              </Card>

              <Card size="3">
                <Flex direction="column" gap="3">
                  <Box style={{
                    width: 40, height: 40, borderRadius: 8,
                    background: 'var(--accent-3)',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                  }}>
                    <ArrowRightLeft size={20} color="var(--accent-11)" />
                  </Box>
                  <Heading size="3">Zero-Copy Migration</Heading>
                  <Text size="2" color="gray">
                    Move full schema and data between systems using Apache Arrow IPC streams.
                    Type mapping, DDL generation, and copy-swap rebuilds handled automatically.
                  </Text>
                </Flex>
              </Card>

              <Card size="3">
                <Flex direction="column" gap="3">
                  <Box style={{
                    width: 40, height: 40, borderRadius: 8,
                    background: 'var(--accent-3)',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                  }}>
                    <Shield size={20} color="var(--accent-11)" />
                  </Box>
                  <Heading size="3">System Agnostic</Heading>
                  <Text size="2" color="gray">
                    Plugins handle translation between native formats and the universal contract.
                    Your application code never changes when systems do.
                  </Text>
                </Flex>
              </Card>
            </Grid>
          </Flex>
        </Section>
      </Container>
    </Box>
  );
}
