import { useMemo, useState } from 'react';
import {
  Box,
  Card,
  Flex,
  ScrollArea,
  Spinner,
  Text,
  TextField,
} from '@radix-ui/themes';
import { MagnifyingGlassIcon } from '@radix-ui/react-icons';
import type { Entity } from '@/api/sessionApi';
import { useBreakpoint } from '@/context/BreakpointContext';

interface EntityBrowserProps {
  entities: Entity[];
  selectedNames: Set<string>;
  onToggleEntity: (entity: Entity) => void;
  isLoading?: boolean;
}

export function EntityBrowser({
  entities,
  selectedNames,
  onToggleEntity,
  isLoading,
}: EntityBrowserProps) {
  const [search, setSearch] = useState('');
  const screenSize = useBreakpoint();

  const filteredEntities = useMemo(
    () =>
      entities.filter(
        (e) =>
          e.name.toLowerCase().includes(search.toLowerCase()) ||
          (e.namespace ?? '').toLowerCase().includes(search.toLowerCase()),
      ),
    [entities, search],
  );

  const isStacked = ["xsm", "sm"].includes(screenSize);

  return (
    <Box style={{ width: isStacked ? '100%' : 280, flexShrink: 0 }}>
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

          {isLoading ? (
            <Flex justify="center" py="4">
              <Spinner />
            </Flex>
          ) : entities.length === 0 ? (
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
                      onClick={() => onToggleEntity(entity)}
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

          {entities.length > 0 && (
            <Text size="1" color="gray">
              {filteredEntities.length} of {entities.length}
            </Text>
          )}
        </Flex>
      </Card>
    </Box>
  );
}
