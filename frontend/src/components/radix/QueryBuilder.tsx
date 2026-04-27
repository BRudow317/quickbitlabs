import {
  Badge,
  Button,
  Card,
  Flex,
  Spinner,
  Text,
  TextField,
} from '@radix-ui/themes';
import { Cross2Icon } from '@radix-ui/react-icons';
import type { Entity } from '@/api/sessionApi';

interface QueryBuilderProps {
  selectedEntities: Entity[];
  onToggleEntity: (entity: Entity) => void;
  onClear: () => void;
  limit: string;
  onLimitChange: (value: string) => void;
  onRunQuery: () => void;
  isPending: boolean;
}

export function QueryBuilder({
  selectedEntities,
  onToggleEntity,
  onClear,
  limit,
  onLimitChange,
  onRunQuery,
  isPending,
}: QueryBuilderProps) {
  return (
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
                  onClick={(e_event) => {
                    e_event.stopPropagation();
                    onToggleEntity(e);
                  }}
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
              onChange={(e) => onLimitChange(e.target.value)}
            />
          </Flex>

          <Button
            disabled={selectedEntities.length === 0 || isPending}
            onClick={onRunQuery}
          >
            {isPending ? (
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
              onClick={onClear}
            >
              Clear
            </Button>
          )}
        </Flex>
      </Flex>
    </Card>
  );
}
