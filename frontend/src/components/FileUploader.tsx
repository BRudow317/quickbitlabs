import { useState } from 'react';
import { 
  Flex, Heading, Text, Card, Button, Spinner, Box 
} from '@radix-ui/themes';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { FileDropzone } from '@/components/FileDropzone';
import { uploadFile } from '@/api/openapi';
import type { CatalogOutput } from '@/api/openapi';
import { useToast } from '@/context/ToastContext';

const ACCEPTED_EXTENSIONS = '.csv,.xlsx,.xls,.parquet,.feather,.arrow,.ipc';
const ACCEPTED_LABEL = 'CSV, Excel, Parquet, Feather, or Arrow file';

type UploadResult = {
  registry_key: string;
  catalog: CatalogOutput;
  registry?: Array<Record<string, unknown>>;
};

export function FileUploader() {
  const [file, setFile] = useState<File | null>(null);
  const { toast } = useToast();

  const queryClient = useQueryClient();

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
      toast.success(`Imported ${result.registry_key} successfully.`);
      setFile(null);
      if (result.registry) {
        queryClient.setQueryData(['registry'], result.registry);
      } else {
        queryClient.invalidateQueries({ queryKey: ['registry'] });
      }
    },
    onError: (err: unknown) => {
      // Local validation errors (like "Please select a file first") won't be caught by the API interceptor
      if (err instanceof Error && !('isAxiosError' in err)) {
        toast.error(err.message);
      }
      // API errors are handled by ApiErrorInterceptor
    },
  });

  return (
    <Card size="3" style={{ flex: 1 }}>
      <Flex direction="column" gap="4">
        <Box>
          <Heading size="3" mb="1">Upload New File</Heading>
          <Text size="2" color="gray">Select a file to register it in the global catalog.</Text>
        </Box>

        <FileDropzone
          onFileSelect={setFile}
          accept={ACCEPTED_EXTENSIONS}
          label="Drop file here"
          description={ACCEPTED_LABEL}
        />

        <Flex justify="end">
          <Button
            size="2"
            disabled={!file || uploadMutation.isPending}
            onClick={() => uploadMutation.mutate()}
          >
            {uploadMutation.isPending ? (
              <Flex align="center" gap="2"><Spinner /> Processing…</Flex>
            ) : (
              'Start Import'
            )}
          </Button>
        </Flex>
      </Flex>
    </Card>
  );
}
