import { useState, useMemo } from 'react';
import {
  Box,
  Flex,
  Heading,
  Text,
  Section,
} from '@radix-ui/themes';
import { Database } from 'lucide-react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { getData, getSession, type Catalog, type Entity, type QueryResult } from '@/api/sessionApi';
import { useToast } from '@/context/ToastContext';
import { EntityBrowser } from '@/components/EntityBrowser';
import { QueryBuilder } from '@/components/QueryBuilder';
import { DataTable } from '@/components/DataTable';

export function DataMartSection() {
  const [selectedEntities, setSelectedEntities] = useState<Entity[]>([]);
  const [limit, setLimit] = useState('500');
  const [results, setResults] = useState<QueryResult | null>(null);
  const { toast } = useToast();

  const { data: session, isLoading: sessionLoading } = useQuery({
    queryKey: ['session'],
    queryFn: getSession,
  });

  const allEntities = useMemo(() => session?.entities ?? [], [session]);
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
    },
    onError: (err: unknown) => {
      if (err instanceof Error && !('isAxiosError' in err)) {
        toast.error(err.message);
      }
      setResults(null);
    },
  });

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
            <Database size={20} color="white" />
          </Box>
          <Flex direction="column">
            <Heading size="4">DataMart Explorer</Heading>
            <Text size="1" color="gray">Discover metadata and query federated sources</Text>
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
          {/* Col 1: Browser */}
          <EntityBrowser 
            entities={allEntities}
            selectedNames={selectedNames}
            onToggleEntity={toggleEntity}
            isLoading={sessionLoading}
          />

          {/* Col 2: Query & Results */}
          <Flex direction="column" gap="3" style={{ flex: 1, minWidth: 0 }}>
            <QueryBuilder 
              selectedEntities={selectedEntities}
              onToggleEntity={toggleEntity}
              onClear={() => {
                setSelectedEntities([]);
                setResults(null);
              }}
              limit={limit}
              onLimitChange={setLimit}
              onRunQuery={() => queryMutation.mutate()}
              isPending={queryMutation.isPending}
            />

            {results && (
              <Box mt="2">
                <Text as="div" size="2" color="gray" mb="2">
                  {results.total} row{results.total !== 1 ? 's' : ''} returned
                </Text>
                <DataTable data={results.rows} />
              </Box>
            )}
          </Flex>
        </Flex>
      </Flex>
    </Section>
  );
}
