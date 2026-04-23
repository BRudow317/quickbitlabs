import { useState } from 'react';
import {
  Box, Button, Callout, Flex, Heading, Select,
  Spinner, Table, Text,
} from '@radix-ui/themes';
import { useQuery } from '@tanstack/react-query';
import { getSession, getData, type Catalog } from '@/api/sessionApi';
import { Info } from 'lucide-react';

const ENTITY_NAME = 'Contact';

export function ContactsPage() {
  const [limit, setLimit] = useState(50);
  const [fetchEnabled, setFetchEnabled] = useState(false);

  const { data: session, isLoading: sessionLoading } = useQuery({
    queryKey: ['session'],
    queryFn: getSession,
  });

  const contactEntity = session?.entities?.find(
    (e) => e.name.toLowerCase() === ENTITY_NAME.toLowerCase()
  );

  const {
    data: results,
    isLoading: dataLoading,
    error,
    refetch,
  } = useQuery({
    queryKey: ['contacts', limit],
    queryFn: () => {
      if (!contactEntity) throw new Error('Contact entity not found in session');
      const catalog: Catalog = { entities: [contactEntity], limit };
      return getData(catalog);
    },
    enabled: fetchEnabled && !!contactEntity,
  });

  const handleLoad = () => {
    if (fetchEnabled) {
      refetch();
    } else {
      setFetchEnabled(true);
    }
  };

  if (sessionLoading) {
    return (
      <Flex align="center" gap="2" p="6">
        <Spinner size="3" />
        <Text color="gray">Loading session catalog…</Text>
      </Flex>
    );
  }

  if (!contactEntity) {
    return (
      <Callout.Root color="orange">
        <Callout.Icon><Info size={16} /></Callout.Icon>
        <Callout.Text>
          No <strong>Contact</strong> entity found in the session catalog. Make sure the
          Salesforce plugin is connected and the session has been synced.
        </Callout.Text>
      </Callout.Root>
    );
  }

  return (
    <Flex direction="column" gap="4">
      <Flex justify="between" align="center">
        <Flex direction="column" gap="1">
          <Heading size="5">Contacts</Heading>
          <Text size="2" color="gray">
            {contactEntity.columns?.length ?? 0} columns available
          </Text>
        </Flex>

        <Flex align="center" gap="3">
          <Select.Root value={String(limit)} onValueChange={(v) => setLimit(Number(v))}>
            <Select.Trigger />
            <Select.Content>
              {[25, 50, 100, 250].map((n) => (
                <Select.Item key={n} value={String(n)}>{n} rows</Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
          <Button onClick={handleLoad} loading={dataLoading} style={{ cursor: 'pointer' }}>
            {fetchEnabled ? 'Refresh' : 'Load Contacts'}
          </Button>
        </Flex>
      </Flex>

      {error && (
        <Callout.Root color="red">
          <Callout.Text>{String(error)}</Callout.Text>
        </Callout.Root>
      )}

      {results && (
        <Flex direction="column" gap="2">
          <Text size="2" color="gray">{results.total} rows returned</Text>
          <Box style={{ overflowX: 'auto' }}>
            <Table.Root variant="surface">
              <Table.Header>
                <Table.Row>
                  {results.columns.map((col) => (
                    <Table.ColumnHeaderCell key={col}>{col}</Table.ColumnHeaderCell>
                  ))}
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {results.rows.map((row, i) => (
                  <Table.Row key={i}>
                    {results.columns.map((col) => (
                      <Table.Cell key={col}>
                        <Text size="2">{String(row[col] ?? '')}</Text>
                      </Table.Cell>
                    ))}
                  </Table.Row>
                ))}
              </Table.Body>
            </Table.Root>
          </Box>
        </Flex>
      )}
    </Flex>
  );
}
