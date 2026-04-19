import { useState } from 'react';
import {
  Badge,
  Box,
  Button,
  Card,
  Container,
  Flex,
  Heading,
  Select,
  Spinner,
  Table,
  Text,
  TextArea,
} from '@radix-ui/themes';
import { useMutation, useQuery } from '@tanstack/react-query';
import { listSystems } from '@/api/sessionApi';
import { runMigration, type MigrationResult } from '@/api/migrationApi';

export function MigrationPage() {
  const [sourcePlugin, setSourcePlugin] = useState('');
  const [targetPlugin, setTargetPlugin] = useState('');
  const [entitiesInput, setEntitiesInput] = useState('');
  const [results, setResults] = useState<MigrationResult | null>(null);
  const [migrationError, setMigrationError] = useState<string | null>(null);

  const { data: plugins = [], isLoading: pluginsLoading } = useQuery({
    queryKey: ['plugins'],
    queryFn: listSystems,
  });

  const migrationMutation = useMutation({
    mutationFn: () => {
      const entities = entitiesInput.trim()
        ? entitiesInput
            .split('\n')
            .map((s) => s.trim())
            .filter(Boolean)
        : null;
      return runMigration({
        source_plugin: sourcePlugin,
        target_plugin: targetPlugin,
        entities,
      });
    },
    onSuccess: (data) => {
      setResults(data);
      setMigrationError(null);
    },
    onError: (err: any) => {
      setMigrationError(err?.response?.data?.detail || err.message || 'Migration failed');
      setResults(null);
    },
  });

  const canRun = !!sourcePlugin && !!targetPlugin && sourcePlugin !== targetPlugin;

  return (
    <Container size="3">
      <Flex direction="column" gap="4">
        <Heading size="5">Migration Manager</Heading>

        <Card>
          <Flex direction="column" gap="4">
            <Flex gap="4" wrap="wrap">
              <Box>
                <Text as="div" size="2" weight="medium" mb="1">
                  Source Plugin
                </Text>
                <Select.Root
                  value={sourcePlugin}
                  onValueChange={(v) => {
                    setSourcePlugin(v);
                    setResults(null);
                  }}
                  disabled={pluginsLoading}
                >
                  <Select.Trigger placeholder="Select source..." />
                  <Select.Content>
                    {plugins.map((p) => (
                      <Select.Item key={p} value={p}>
                        {p}
                      </Select.Item>
                    ))}
                  </Select.Content>
                </Select.Root>
              </Box>

              <Box>
                <Text as="div" size="2" weight="medium" mb="1">
                  Target Plugin
                </Text>
                <Select.Root
                  value={targetPlugin}
                  onValueChange={(v) => {
                    setTargetPlugin(v);
                    setResults(null);
                  }}
                  disabled={pluginsLoading}
                >
                  <Select.Trigger placeholder="Select target..." />
                  <Select.Content>
                    {plugins
                      .filter((p) => p !== sourcePlugin)
                      .map((p) => (
                        <Select.Item key={p} value={p}>
                          {p}
                        </Select.Item>
                      ))}
                  </Select.Content>
                </Select.Root>
              </Box>
            </Flex>

            <Box>
              <Text as="div" size="2" weight="medium" mb="1">
                Entity Filter{' '}
                <Text size="1" color="gray">
                  (optional — one entity name per line, empty = all)
                </Text>
              </Text>
              <TextArea
                placeholder={'Account\nContact\nLead'}
                value={entitiesInput}
                onChange={(e) => setEntitiesInput(e.target.value)}
                style={{ height: 100 }}
              />
            </Box>

            {migrationError && (
              <Text color="red" size="2">
                {migrationError}
              </Text>
            )}

            <Flex gap="3" align="center">
              <Button
                disabled={!canRun || migrationMutation.isPending}
                onClick={() => migrationMutation.mutate()}
              >
                {migrationMutation.isPending ? (
                  <Flex align="center" gap="2">
                    <Spinner /> Running...
                  </Flex>
                ) : (
                  'Run Migration'
                )}
              </Button>
              {migrationMutation.isPending && (
                <Text size="2" color="gray">
                  Migration in progress — this may take several minutes.
                </Text>
              )}
            </Flex>
          </Flex>
        </Card>

        {results && <MigrationResults results={results} />}
      </Flex>
    </Container>
  );
}

function MigrationResults({ results }: { results: MigrationResult }) {
  const { succeeded, failed, results: entityResults } = results;

  return (
    <Box>
      <Flex gap="3" align="center" mb="3">
        <Text size="3" weight="medium">
          Results
        </Text>
        <Badge color="green">{succeeded} succeeded</Badge>
        {failed > 0 && <Badge color="red">{failed} failed</Badge>}
      </Flex>
      <Table.Root>
        <Table.Header>
          <Table.Row>
            <Table.ColumnHeaderCell>Source Entity</Table.ColumnHeaderCell>
            <Table.ColumnHeaderCell>Target</Table.ColumnHeaderCell>
            <Table.ColumnHeaderCell>Status</Table.ColumnHeaderCell>
            <Table.ColumnHeaderCell>Message</Table.ColumnHeaderCell>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {entityResults.map((r, i) => (
            <Table.Row key={i}>
              <Table.Cell>{r.entity}</Table.Cell>
              <Table.Cell>{r.target ?? '—'}</Table.Cell>
              <Table.Cell>
                <Badge color={r.status === 'ok' ? 'green' : 'red'}>{r.status}</Badge>
              </Table.Cell>
              <Table.Cell>
                <Text size="1" color="gray">
                  {r.message ?? ''}
                </Text>
              </Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Root>
    </Box>
  );
}
