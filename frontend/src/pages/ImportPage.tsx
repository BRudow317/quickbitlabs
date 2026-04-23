import { useState } from 'react';
import {
  Box,
  Button,
  Card,
  Container,
  Flex,
  Heading,
  Text,
  TextArea,
  Callout,
  Spinner,
} from '@radix-ui/themes';
import { InfoCircledIcon, CheckCircledIcon } from '@radix-ui/react-icons';
import { useMutation } from '@tanstack/react-query';
import { FileDropzone } from '@/components/FileDropzone';
import { createDataApiDataInsertPut } from '@/api/openapi';

export function ImportPage() {
  const [file, setFile] = useState<File | null>(null);
  const [catalogJson, setCatalogJson] = useState('{\n  "entities": [],\n  "limit": 100\n}');
  const [status, setStatus] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  const uploadMutation = useMutation({
    mutationFn: async () => {
      if (!file) throw new Error("Please select a file first");
      
      // The API expects catalog_json as a string and file as a Blob/File
      // The SDK's BodyCreateDataApiDataInsertPut should handle this
      const { data, error } = await createDataApiDataInsertPut({
        body: {
          catalog_json: catalogJson,
          file: file as any, // Cast to any if there's a type mismatch with the generated SDK's File type
        }
      });

      if (error) throw error;
      return data;
    },
    onSuccess: () => {
      setStatus({ type: 'success', message: 'Data imported successfully!' });
      setFile(null);
    },
    onError: (err: any) => {
      const msg = err?.response?.data?.detail || err.message || 'Import failed';
      setStatus({ type: 'error', message: msg });
    },
  });

  const handleUpload = () => {
    setStatus(null);
    uploadMutation.mutate();
  };

  return (
    <Container size="3">
      <Flex direction="column" gap="4">
        <Heading size="5">Import Data</Heading>
        <Text size="2" color="gray">
          Upload Arrow IPC Stream binary files to insert or update data in the system.
        </Text>

        <Card size="3">
          <Flex direction="column" gap="4">
            <Box>
              <Text as="div" size="2" weight="bold" mb="2">
                1. Catalog Configuration (JSON)
              </Text>
              <TextArea
                placeholder='{ "entities": [...], "limit": 100 }'
                value={catalogJson}
                onChange={(e) => setCatalogJson(e.target.value)}
                style={{ height: 120, fontFamily: 'monospace' }}
              />
            </Box>

            <Box>
              <Text as="div" size="2" weight="bold" mb="2">
                2. Select Arrow File
              </Text>
              <FileDropzone 
                onFileSelect={setFile} 
                accept=".arrow,.ipc"
                label="Arrow Stream File"
                description="Select the .arrow or .ipc file to upload"
              />
            </Box>

            {status && (
              <Callout.Root color={status.type === 'success' ? 'green' : 'red'}>
                <Callout.Icon>
                  {status.type === 'success' ? <CheckCircledIcon /> : <InfoCircledIcon />}
                </Callout.Icon>
                <Callout.Text>{status.message}</Callout.Text>
              </Callout.Root>
            )}

            <Flex justify="end">
              <Button 
                size="3" 
                disabled={!file || uploadMutation.isPending}
                onClick={handleUpload}
              >
                {uploadMutation.isPending ? (
                  <Flex align="center" gap="2">
                    <Spinner /> Uploading...
                  </Flex>
                ) : (
                  'Start Import'
                )}
              </Button>
            </Flex>
          </Flex>
        </Card>
      </Flex>
    </Container>
  );
}
