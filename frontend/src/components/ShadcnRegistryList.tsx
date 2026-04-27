import { File, Trash2, Loader2, Database } from "lucide-react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { listRegistry, deleteRegistryEntry } from "@/api/openapi"
import { useToast } from "@/context/ToastContext"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Button } from "@/components/ui/button"
import { Separator } from "@/components/ui/separator"

export function ShadcnRegistryList() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const { data: registryList = [], isLoading } = useQuery({
    queryKey: ['registry'],
    queryFn: async () => {
      const { data, error: err } = await listRegistry()
      if (err) throw err
      return (data ?? []) as Array<{ registry_key: string; name?: string; [k: string]: unknown }>
    },
  })

  const deleteMutation = useMutation({
    mutationFn: async (key: string) => {
      const { error: err } = await deleteRegistryEntry({ path: { registry_key: key } })
      if (err) throw err
    },
    onSuccess: (_, key) => {
      toast.success(`Removed ${key} from registry.`)
      queryClient.invalidateQueries({ queryKey: ['registry'] })
    },
  })

  return (
    <Card className="flex-1">
      <CardHeader className="pb-3">
        <div className="flex justify-between items-center">
          <CardTitle className="text-lg flex items-center gap-2">
            <Database className="h-4 w-4" /> Catalog Registry
          </CardTitle>
          <Badge variant="secondary" className="rounded-full">
            {isLoading ? "..." : registryList.length}
          </Badge>
        </div>
      </CardHeader>
      <Separator />
      <CardContent className="pt-4">
        {registryList.length === 0 && !isLoading ? (
          <div className="flex flex-col items-center justify-center py-10 text-center">
            <File className="h-10 w-10 text-muted-foreground/20 mb-2" />
            <p className="text-sm text-muted-foreground">No files imported yet.</p>
          </div>
        ) : (
          <ScrollArea className="h-[300px] pr-4">
            <div className="space-y-1">
              {registryList.map((entry) => (
                <div 
                  key={entry.registry_key} 
                  className="flex items-center gap-3 p-2 rounded-md hover:bg-muted/50 transition-colors group"
                >
                  <div className="bg-primary/10 p-2 rounded-sm group-hover:bg-primary/20 transition-colors">
                    <File className="h-4 w-4 text-primary" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium truncate leading-none mb-1">
                      {entry.registry_key}
                    </p>
                    {entry.name && (
                      <p className="text-[10px] text-muted-foreground truncate">
                        {String(entry.name)}
                      </p>
                    )}
                  </div>
                  <Button
                    size="icon"
                    variant="ghost"
                    className="h-8 w-8 text-destructive hover:text-destructive hover:bg-destructive/10 opacity-0 group-hover:opacity-100 transition-opacity"
                    onClick={() => deleteMutation.mutate(entry.registry_key)}
                    disabled={deleteMutation.isPending && deleteMutation.variables === entry.registry_key}
                  >
                    {deleteMutation.isPending && deleteMutation.variables === entry.registry_key ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : (
                      <Trash2 className="h-3.5 w-3.5" />
                    )}
                  </Button>
                </div>
              ))}
            </div>
          </ScrollArea>
        )}
      </CardContent>
    </Card>
  )
}
