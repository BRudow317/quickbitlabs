import { useState, useMemo } from 'react';
import {
  Badge,
  Box,
  Button,
  Card,
  Container,
  Flex,
  Heading,
  ScrollArea,
  Spinner,
  Text,
  TextField,
} from '@radix-ui/themes';
import { Cross2Icon, MagnifyingGlassIcon } from '@radix-ui/react-icons';
import { useMutation, useQuery } from '@tanstack/react-query';
import { getData, getSession, type Catalog, type Entity, type QueryResult } from '@/api/sessionApi';

export function DataMartPage() {
  const [search, setSearch] = useState('');
  const [selectedEntities, setSelectedEntities] = useState<Entity[]>([]);
  const [limit, setLimit] = useState('500');
  const [results, setResults] = useState<QueryResult | null>(null);
  const [queryError, setQueryError] = useState<string | null>(null);

  const { data: session, isLoading: sessionLoading } = useQuery({
    queryKey: ['session'],
    queryFn: getSession,
  });

  const allEntities = session?.entities ?? [];

  const filteredEntities = useMemo(
    () =>
      allEntities.filter(
        (e) =>
          e.name.toLowerCase().includes(search.toLowerCase()) ||
          (e.namespace ?? '').toLowerCase().includes(search.toLowerCase()),
      ),
    [allEntities, search],
  );

  const selectedNames = useMemo(
    () => new Set(selectedEntities.map((e) => e.name)),
    [selectedEntities],
  );

  const toggleEntity = (entity: Entity) => {
    setSelectedEntities((prev) =>
      selectedNames.has(entity.name)
        ? prev.filter((e) => e.name !== entity.name)
        : [...prev, entity],
    );
    setResults(null);
  };

  const queryMutation = useMutation({
    mutationFn: (): Promise<QueryResult> => {
      const catalog: Catalog = {
        entities: selectedEntities,
        limit: parseInt(limit) || 500,
      };
      return getData(catalog);
    },
    onSuccess: (data) => {
      setResults(data);
      setQueryError(null);
    },
    onError: (err: unknown) => {
      const msg =
        err instanceof Error
          ? err.message
          : ((err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ??
            'Query failed');
      setQueryError(msg);
      setResults(null);
    },
  });

  return (
    <Container size="4">
      <Flex direction="column" gap="4">
        <Heading size="5">DataMart</Heading>

        <Flex gap="4" align="start">
          {/* -- Entity Browser ------------------------------------------- */}
          <Box style={{ width: 280, flexShrink: 0 }}>
            <Card>
              <Flex direction="column" gap="3">
                <Text size="2" weight="medium">
                  Available Entities
                </Text>

                <TextField.Root
                  placeholder="Search..."
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                >
                  <TextField.Slot>
                    <MagnifyingGlassIcon />
                  </TextField.Slot>
                </TextField.Root>

                {sessionLoading ? (
                  <Flex justify="center" py="4">
                    <Spinner />
                  </Flex>
                ) : allEntities.length === 0 ? (
                  <Text size="1" color="gray" align="center">
                    No metadata cached. Run sync_systems.py to populate.
                  </Text>
                ) : (
                  <ScrollArea style={{ maxHeight: 480 }}>
                    <Flex direction="column" gap="1">
                      {filteredEntities.map((entity) => {
                        const isSelected = selectedNames.has(entity.name);
                        return (
                          <Box
                            key={entity.name}
                            onClick={() => toggleEntity(entity)}
                            style={{
                              padding: '6px 8px',
                              borderRadius: 4,
                              cursor: 'pointer',
                              background: isSelected
                                ? 'var(--accent-a3)'
                                : 'transparent',
                            }}
                          >
                            <Text size="2" weight={isSelected ? 'medium' : 'regular'}>
                              {entity.name}
                            </Text>
                            {entity.namespace && (
                              <Text as="div" size="1" color="gray">
                                {entity.namespace}
                              </Text>
                            )}
                          </Box>
                        );
                      })}
                    </Flex>
                  </ScrollArea>
                )}

                {allEntities.length > 0 && (
                  <Text size="1" color="gray">
                    {filteredEntities.length} of {allEntities.length}
                  </Text>
                )}
              </Flex>
            </Card>
          </Box>

          {/* -- Query Builder + Results ----------------------------------- */}
          <Box style={{ flex: 1, minWidth: 0 }}>
            <Flex direction="column" gap="3">
              <Card>
                <Flex direction="column" gap="3">
                  <Text size="2" weight="medium">
                    Selection
                  </Text>

                  <Flex gap="2" wrap="wrap" align="center" style={{ minHeight: 28 }}>
                    {selectedEntities.length === 0 ? (
                      <Text size="2" color="gray">
                        Select entities from the browser to begin.
                      </Text>
                    ) : (
                      selectedEntities.map((e) => (
                        <Badge key={e.name} variant="soft" style={{ cursor: 'pointer' }}>
                          {e.name}
                          <Cross2Icon
                            style={{ marginLeft: 4 }}
                            onClick={() => toggleEntity(e)}
                          />
                        </Badge>
                      ))
                    )}
                  </Flex>

                  <Flex gap="3" align="center">
                    <Flex align="center" gap="2">
                      <Text size="2">Limit</Text>
                      <TextField.Root
                        style={{ width: 80 }}
                        type="number"
                        value={limit}
                        min={1}
                        max={10000}
                        onChange={(e) => setLimit(e.target.value)}
                      />
                    </Flex>

                    <Button
                      disabled={selectedEntities.length === 0 || queryMutation.isPending}
                      onClick={() => queryMutation.mutate()}
                    >
                      {queryMutation.isPending ? (
                        <Flex align="center" gap="2">
                          <Spinner /> Querying...
                        </Flex>
                      ) : (
                        'Run Query'
                      )}
                    </Button>

                    {selectedEntities.length > 0 && (
                      <Button
                        variant="ghost"
                        color="gray"
                        onClick={() => {
                          setSelectedEntities([]);
                          setResults(null);
                          setQueryError(null);
                        }}
                      >
                        Clear
                      </Button>
                    )}
                  </Flex>

                  {queryError && (
                    <Text color="red" size="2">
                      {queryError}
                    </Text>
                  )}
                </Flex>
              </Card>

              {results && <ResultsTable results={results} />}
            </Flex>
          </Box>
        </Flex>
      </Flex>
    </Container>
  );
}

function ResultsTable({ results }: { results: QueryResult }) {
  const { columns, rows, total } = results;

  if (rows.length === 0) {
    return (
      <Text color="gray" size="2">
        No results returned.
      </Text>
    );
  }

  return (
    <Box>
      <Text as="div" size="2" color="gray" mb="2">
        {total} row{total !== 1 ? 's' : ''} returned
      </Text>
      <Box style={{ overflowX: 'auto', border: '1px solid var(--gray-a5)', borderRadius: 6 }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
          <thead>
            <tr style={{ background: 'var(--gray-a2)' }}>
              {columns.map((col) => (
                <th
                  key={col}
                  style={{
                    padding: '7px 12px',
                    textAlign: 'left',
                    whiteSpace: 'nowrap',
                    borderBottom: '1px solid var(--gray-a5)',
                    fontWeight: 600,
                  }}
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr
                key={i}
                style={{
                  borderBottom: '1px solid var(--gray-a3)',
                  background: i % 2 === 0 ? 'transparent' : 'var(--gray-a1)',
                }}
              >
                {columns.map((col) => (
                  <td
                    key={col}
                    style={{
                      padding: '5px 12px',
                      whiteSpace: 'nowrap',
                      maxWidth: 220,
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                    }}
                    title={String(row[col] ?? '')}
                  >
                    {row[col] === null || row[col] === undefined ? (
                      <Text color="gray" size="1">
                        null
                      </Text>
                    ) : (
                      String(row[col])
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </Box>
    </Box>
  );
}
