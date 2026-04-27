import { useMemo } from 'react';
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
import { useData } from '@/context/DataContext';
import { useBreakpoint } from '@/context/BreakpointContext';
import { EntityBrowser } from '@/components/radix/EntityBrowser';
import { QueryBuilder } from '@/components/radix/QueryBuilder';
import { DataTable } from '@/components/radix/DataTable';

export function DataMartSection() {
  const { 
    selectedEntities, setSelectedEntities, 
    queryResults, setQueryResults,
    queryLimit, setQueryLimit 
  } = useData();
  
  const { toast } = useToast();
  const screenSize = useBreakpoint();

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
    setQueryResults(null);
  };

  const queryMutation = useMutation({
    mutationFn: (): Promise<QueryResult> => {
      const catalog: Catalog = {
        entities: selectedEntities,
        limit: parseInt(queryLimit) || 500,
      };
      return getData(catalog);
    },
    onSuccess: (data) => {
      setQueryResults(data);
    },
    onError: (err: unknown) => {
      if (err instanceof Error && !('isAxiosError' in err)) {
        toast.error(err.message);
      }
      setQueryResults(null);
    },
  });

  // FLEX-STANDARD synchronization with BreakpointContext
  const isStacked = ["xsm", "sm"].includes(screenSize);
  const layoutDirection = isStacked ? "column" : "row";

  return (
    <Section size="2" style={{ width: '100%' }}>
      <Flex direction="column" gap="4" width="100%">
        {/* Section Header */}
        <Flex align="center" gap="3" width="100%">
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
          direction={layoutDirection} 
          gap="5" 
          align="start"
          width="100%"
        >
          {/* Col 1: Browser (Column Component) */}
          <EntityBrowser 
            entities={allEntities}
            selectedNames={selectedNames}
            onToggleEntity={toggleEntity}
            isLoading={sessionLoading}
          />

          {/* Col 2: Query & Results (Column Component) */}
          <Flex direction="column" gap="3" style={{ flex: 1, minWidth: 0 }} width="100%">
            <QueryBuilder 
              selectedEntities={selectedEntities}
              onToggleEntity={toggleEntity}
              onClear={() => {
                setSelectedEntities([]);
                setQueryResults(null);
              }}
              limit={queryLimit}
              onLimitChange={setQueryLimit}
              onRunQuery={() => queryMutation.mutate()}
              isPending={queryMutation.isPending}
            />

            {queryResults && (
              <Box mt="2" width="100%">
                <Text as="div" size="2" color="gray" mb="2">
                  {queryResults.total} row{queryResults.total !== 1 ? 's' : ''} returned
                </Text>
                <DataTable data={queryResults.rows} />
              </Box>
            )}
          </Flex>
        </Flex>
      </Flex>
    </Section>
  );
}
