import {
  Flex, Heading, Text, Card, Badge, Separator, ScrollArea, Button, Box
} from '@radix-ui/themes';
import { FileIcon, TrashIcon } from '@radix-ui/react-icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { listRegistry, deleteRegistryEntry } from '@/api/openapi';
import { useToast } from '@/context/ToastContext';

export function RegistryList() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  const { data: registryList = [], isLoading } = useQuery({
    queryKey: ['registry'],
    queryFn: async () => {
      const { data, error: err } = await listRegistry();
      if (err) throw err;
      return (data ?? []) as Array<{ registry_key: string; name?: string; [k: string]: unknown }>;
    },
  });

  const deleteMutation = useMutation({
    mutationFn: async (key: string) => {
      const { error: err } = await deleteRegistryEntry({ path: { registry_key: key } });
      if (err) throw err;
    },
    onSuccess: (_, key) => {
      toast.success(`Removed ${key} from registry.`);
      queryClient.invalidateQueries({ queryKey: ['registry'] });
    },
  });

  return (
    <Card size="3" style={{ flex: 1 }}>
      <Flex direction="column" gap="3">
        <Flex justify="between" align="center">
          <Heading size="3">Catalog Registry</Heading>
          <Badge color="gray" radius="full">{isLoading ? '...' : registryList.length}</Badge>
        </Flex>
        <Separator size="4" />
        
        {registryList.length === 0 && !isLoading ? (
          <Flex align="center" justify="center" py="4">
            <Text size="2" color="gray">No files imported yet.</Text>
          </Flex>
        ) : (
          <ScrollArea style={{ maxHeight: 400 }}>
            <Flex direction="column" gap="2">
              {registryList.map((entry) => (
                <Flex key={entry.registry_key} align="center" gap="2" px="1" py="1">
                  <FileIcon />
                  <Box style={{ flexGrow: 1 }}>
                    <Text size="2" weight="medium" style={{ wordBreak: 'break-all' }}>
                      {entry.registry_key}
                    </Text>
                    {entry.name && (
                      <Text as="div" size="1" color="gray">{String(entry.name)}</Text>
                    )}
                  </Box>
                  <Button
                    size="1"
                    variant="ghost"
                    color="red"
                    onClick={() => deleteMutation.mutate(entry.registry_key)}
                    disabled={deleteMutation.isPending && deleteMutation.variables === entry.registry_key}
                  >
                    <TrashIcon />
                  </Button>
                </Flex>
              ))}
            </Flex>
          </ScrollArea>
        )}
      </Flex>
    </Card>
  );
}
