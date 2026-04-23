import { useState } from 'react';
import {
  Box,
  Button,
  Card,
  Container,
  Flex,
  Heading,
  Text,
  Callout,
  Spinner,
  Badge,
  Separator,
  ScrollArea,
} from '@radix-ui/themes';
import {
  InfoCircledIcon,
  CheckCircledIcon,
  FileIcon,
  TrashIcon,
} from '@radix-ui/react-icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { FileDropzone } from '@/components/FileDropzone';
import { uploadFile, listRegistry, deleteRegistryEntry } from '@/api/openapi';
import type { CatalogOutput } from '@/api/openapi';

const ACCEPTED_EXTENSIONS = '.csv,.xlsx,.xls,.parquet,.feather,.arrow,.ipc';
const ACCEPTED_LABEL = 'CSV, Excel, Parquet, Feather, or Arrow file';

type UploadResult = {
  registry_key: string;
  catalog: CatalogOutput;
  registry: Array<{ registry_key: string; name?: string; [k: string]: unknown }>;
};

export function ImportPage() {
  const [file, setFile] = useState<File | null>(null);
  const [uploadResult, setUploadResult] = useState<UploadResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const queryClient = useQueryClient();

  const { data: registryList = [] } = useQuery({
    queryKey: ['registry'],
    queryFn: async () => {
      const { data, error: err } = await listRegistry();
      if (err) throw err;
      return (data ?? []) as Array<{ registry_key: string; name?: string; [k: string]: unknown }>;
    },
  });

  const uploadMutation = useMutation({
    mutationFn: async () => {
      if (!file) throw new Error('Please select a file first');
      const { data, error: err } = await uploadFile({
        body: { file },
      });
      if (err) throw err;
      return data as UploadResult;
    },
    onSuccess: (result) => {
      setUploadResult(result);
      setError(null);
      setFile(null);
      queryClient.invalidateQueries({ queryKey: ['registry'] });
    },
    onError: (err: unknown) => {
      const msg =
        (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ||
        (err instanceof Error ? err.message : 'Upload failed');
      setError(msg);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: async (key: string) => {
      const { error: err } = await deleteRegistryEntry({ path: { registry_key: key } });
      if (err) throw err;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['registry'] });
      if (uploadResult && deleteMutation.variables === uploadResult.registry_key) {
        setUploadResult(null);
      }
    },
  });

  return (
    <Container size="3">
      <Flex direction="column" gap="5">
        <Box>
          <Heading size="5" mb="1">Import Data</Heading>
          <Text size="2" color="gray">
            Upload a file to import it into the catalog. Supported formats: CSV, Excel (.xlsx/.xls),
            Parquet, Feather, and Arrow IPC.
          </Text>
        </Box>

        <Card size="3">
          <Flex direction="column" gap="4">
            <FileDropzone
              onFileSelect={setFile}
              accept={ACCEPTED_EXTENSIONS}
              label="Drop a file here to import"
              description={ACCEPTED_LABEL}
            />

            {error && (
              <Callout.Root color="red">
                <Callout.Icon><InfoCircledIcon /></Callout.Icon>
                <Callout.Text>{error}</Callout.Text>
              </Callout.Root>
            )}

            {uploadResult && (
              <Callout.Root color="green">
                <Callout.Icon><CheckCircledIcon /></Callout.Icon>
                <Callout.Text>
                  Saved as <strong>{uploadResult.registry_key}</strong> — {uploadResult.catalog.entities?.length ?? 0} entity/entities imported.
                </Callout.Text>
              </Callout.Root>
            )}

            <Flex justify="end">
              <Button
                size="3"
                disabled={!file || uploadMutation.isPending}
                onClick={() => { setError(null); uploadMutation.mutate(); }}
              >
                {uploadMutation.isPending ? (
                  <Flex align="center" gap="2"><Spinner /> Uploading…</Flex>
                ) : (
                  'Import File'
                )}
              </Button>
            </Flex>
          </Flex>
        </Card>

        {uploadResult && (
          <Card size="3">
            <Flex direction="column" gap="3">
              <Flex justify="between" align="center">
                <Heading size="3">Imported Entities</Heading>
                <Badge color="blue" radius="full">{uploadResult.catalog.entities?.length ?? 0}</Badge>
              </Flex>
              <Separator size="4" />
              <ScrollArea style={{ maxHeight: 240 }}>
                <Flex direction="column" gap="2">
                  {(uploadResult.catalog.entities ?? []).map((entity) => (
                    <Flex key={entity.name} align="center" gap="2" px="1">
                      <FileIcon />
                      <Text size="2" weight="medium">{entity.name}</Text>
                      <Text size="1" color="gray" ml="auto">
                        {entity.columns?.length ?? 0} columns
                      </Text>
                    </Flex>
                  ))}
                </Flex>
              </ScrollArea>
            </Flex>
          </Card>
        )}

        <Card size="3">
          <Flex direction="column" gap="3">
            <Flex justify="between" align="center">
              <Heading size="3">Catalog Registry</Heading>
              <Badge color="gray" radius="full">{registryList.length}</Badge>
            </Flex>
            <Separator size="4" />
            {registryList.length === 0 ? (
              <Text size="2" color="gray">No files imported yet.</Text>
            ) : (
              <ScrollArea style={{ maxHeight: 300 }}>
                <Flex direction="column" gap="2">
                  {registryList.map((entry) => (
                    <Flex key={entry.registry_key} align="center" gap="2" px="1" py="1">
                      <FileIcon />
                      <Box flexGrow="1">
                        <Text size="2" weight="medium">{entry.registry_key}</Text>
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
      </Flex>
    </Container>
  );
}
