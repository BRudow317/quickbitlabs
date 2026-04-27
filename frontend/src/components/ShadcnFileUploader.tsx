import * as React from "react"
import { Upload, Loader2 } from "lucide-react"
import { useMutation, useQueryClient } from "@tanstack/react-query"
import { FileDropzone } from "@/components/radix/FileDropzone"
import { uploadFile } from "@/api/openapi"
import type { CatalogOutput } from "@/api/openapi"
import { useToast } from "@/context/ToastContext"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"

const ACCEPTED_EXTENSIONS = '.csv,.xlsx,.xls,.parquet,.feather,.arrow,.ipc'
const ACCEPTED_LABEL = 'CSV, Excel, Parquet, Feather, or Arrow file'

type UploadResult = {
  registry_key: string;
  catalog: CatalogOutput;
  registry?: Array<Record<string, unknown>>;
}

export function ShadcnFileUploader() {
  const [file, setFile] = React.useState<File | null>(null)
  const { toast } = useToast()
  const queryClient = useQueryClient()

  const uploadMutation = useMutation({
    mutationFn: async () => {
      if (!file) throw new Error('Please select a file first')
      const { data, error: err } = await uploadFile({
        body: { file },
      })
      if (err) throw err
      return data as UploadResult
    },
    onSuccess: (result) => {
      toast.success(`Imported ${result.registry_key} successfully.`)
      setFile(null)
      if (result.registry) {
        queryClient.setQueryData(['registry'], result.registry)
      } else {
        queryClient.invalidateQueries({ queryKey: ['registry'] })
      }
    },
    onError: (err: unknown) => {
      if (err instanceof Error && !('isAxiosError' in err)) {
        toast.error(err.message)
      }
    },
  })

  return (
    <Card className="flex-1">
      <CardHeader>
        <CardTitle className="text-lg">Upload New File</CardTitle>
        <CardDescription>Select a file to register it in the global catalog.</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <FileDropzone
          onFileSelect={setFile}
          accept={ACCEPTED_EXTENSIONS}
          label="Drop file here"
          description={ACCEPTED_LABEL}
        />

        <div className="flex justify-end">
          <Button
            size="sm"
            disabled={!file || uploadMutation.isPending}
            onClick={() => uploadMutation.mutate()}
          >
            {uploadMutation.isPending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Processing
              </>
            ) : (
              <>
                <Upload className="mr-2 h-4 w-4" />
                Start Import
              </>
            )}
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
