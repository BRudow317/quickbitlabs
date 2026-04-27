import * as React from "react"
import { Database } from "lucide-react"
import { useMutation, useQuery } from "@tanstack/react-query"
import { getData, getSession, type Catalog, type Entity, type QueryResult } from "@/api/sessionApi"
import { useToast } from "@/context/ToastContext"
import { useData } from "@/context/DataContext"
import { useBreakpoint } from "@/context/BreakpointContext"
import { ShadcnEntityBrowser } from "@/components/ShadcnEntityBrowser"
import { ShadcnQueryBuilder } from "@/components/ShadcnQueryBuilder"
import { ShadcnDataTable } from "@/components/ShadcnDataTable"
import { Section } from "@radix-ui/themes"
import { Separator } from "@/components/ui/separator"

export function ShadcnDataMartSection() {
  const { 
    selectedEntities, setSelectedEntities, 
    queryResults, setQueryResults,
    queryLimit, setQueryLimit 
  } = useData()
  
  const { toast } = useToast()
  const screenSize = useBreakpoint()

  const { data: session, isLoading: sessionLoading } = useQuery({
    queryKey: ['session'],
    queryFn: getSession,
  })

  const allEntities = React.useMemo(() => session?.entities ?? [], [session])
  const selectedNames = React.useMemo(
    () => new Set(selectedEntities.map((e) => e.name)),
    [selectedEntities]
  )

  const toggleEntity = (entity: Entity) => {
    setSelectedEntities((prev) =>
      selectedNames.has(entity.name)
        ? prev.filter((e) => e.name !== entity.name)
        : [...prev, entity]
    )
    setQueryResults(null)
  }

  const queryMutation = useMutation({
    mutationFn: (): Promise<QueryResult> => {
      const catalog: Catalog = {
        entities: selectedEntities,
        limit: parseInt(queryLimit) || 500,
      }
      return getData(catalog)
    },
    onSuccess: (data) => {
      setQueryResults(data)
    },
    onError: (err: unknown) => {
      if (err instanceof Error && !('isAxiosError' in err)) {
        toast.error(err.message)
      }
      setQueryResults(null)
    }
  })

  const isStacked = ["xsm", "sm"].includes(screenSize)
  const layoutDirection = isStacked ? "flex-col" : "flex-row"

  return (
    <Section size="2" className="w-full">
      <div className="flex flex-col gap-6 w-full">
        {/* Section Header */}
        <div className="flex items-center gap-4">
          <div className="bg-primary p-2.5 rounded-lg shadow-sm">
            <Database className="h-5 w-5 text-primary-foreground" />
          </div>
          <div>
            <h2 className="text-xl font-semibold tracking-tight">Shadcn DataMart</h2>
            <p className="text-sm text-muted-foreground">Modern interface for federated metadata discovery</p>
          </div>
        </div>

        <Separator />

        {/* FLEX-STANDARD: Row on Desktop, Column on Mobile */}
        <div className={`flex ${layoutDirection} gap-6 items-start w-full`}>
          {/* Col 1: Browser */}
          <ShadcnEntityBrowser 
            entities={allEntities}
            selectedNames={selectedNames}
            onToggleEntity={toggleEntity}
            isLoading={sessionLoading}
          />

          {/* Col 2: Query & Results */}
          <div className="flex flex-col gap-4 flex-1 min-w-0 w-full">
            <ShadcnQueryBuilder 
              selectedEntities={selectedEntities}
              onToggleEntity={toggleEntity}
              onClear={() => {
                setSelectedEntities([])
                setQueryResults(null)
              }}
              limit={queryLimit}
              onLimitChange={setQueryLimit}
              onRunQuery={() => queryMutation.mutate()}
              isPending={queryMutation.isPending}
            />

            {queryResults && (
              <div className="mt-2 w-full space-y-2">
                <div className="flex justify-between items-center px-1">
                  <span className="text-xs font-medium text-muted-foreground">
                    {queryResults.total} row{queryResults.total !== 1 ? 's' : ''} returned
                  </span>
                </div>
                <ShadcnDataTable 
                  columns={queryResults.columns.map(col => ({
                    accessorKey: col,
                    header: col,
                  }))}
                  data={queryResults.rows} 
                />
              </div>
            )}
          </div>
        </div>
      </div>
    </Section>
  )
}
