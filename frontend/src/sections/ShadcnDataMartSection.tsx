import * as React from "react"
import { Database } from "lucide-react"
import { useMutation, useQuery } from "@tanstack/react-query"
import {
  getData, getSession,
  type Catalog, type Entity, type Join, type OperatorGroup, type QueryResult, type Sort,
} from "@/api/sessionApi"
import { useToast } from "@/context/ToastContext"
import { useData } from "@/context/DataContext"
import { ShadcnQueryBuilder } from "@/components/ShadcnQueryBuilder"
import { CrudDataTable } from "@/components/CrudDataTable"
import { Section } from "@radix-ui/themes"
import { Separator } from "@/components/ui/separator"

export function ShadcnDataMartSection() {
  const {
    selectedEntities, setSelectedEntities,
    queryResults, setQueryResults,
    queryLimit, setQueryLimit,
  } = useData()

  const { toast } = useToast()
  const [joins, setJoins]               = React.useState<Join[]>([])
  const [filterGroups, setFilterGroups] = React.useState<OperatorGroup[]>([])
  const [sortColumns, setSortColumns]   = React.useState<Sort[]>([])
  const [queryVersion, setQueryVersion] = React.useState(0)

  const { data: session, isLoading: sessionLoading } = useQuery({
    queryKey: ['session'],
    queryFn: getSession,
  })

  const allEntities = React.useMemo(() => session?.entities ?? [], [session])
  const selectedNames = React.useMemo(
    () => new Set(selectedEntities.map(e => e.name)),
    [selectedEntities],
  )

  const toggleEntity = (entity: Entity) => {
    setSelectedEntities(prev =>
      selectedNames.has(entity.name)
        ? prev.filter(e => e.name !== entity.name)
        : [...prev, entity],
    )
    setQueryResults(null)
  }

  const clearAll = () => {
    setSelectedEntities([])
    setQueryResults(null)
    setJoins([])
    setFilterGroups([])
    setSortColumns([])
  }

  const queryMutation = useMutation({
    mutationFn: (): Promise<QueryResult> => {
      const catalog: Catalog = {
        entities: selectedEntities,
        joins,
        operator_groups: filterGroups,
        sort_columns: sortColumns,
        limit: parseInt(queryLimit) || 500,
      }
      return getData(catalog)
    },
    onSuccess: data => {
      setQueryResults(data)
      setQueryVersion(v => v + 1)
    },
    onError: (err: unknown) => {
      if (err instanceof Error && !('isAxiosError' in err)) toast.error(err.message)
      setQueryResults(null)
    },
  })

  return (
    <Section size="2" className="w-full">
      <div className="flex flex-col gap-6 w-full">
        {/* Header */}
        <div className="flex items-center gap-4">
          <div className="bg-primary p-2.5 rounded-lg shadow-sm">
            <Database className="h-5 w-5 text-primary-foreground" />
          </div>
          <div>
            <h2 className="text-xl font-semibold tracking-tight">DataMart</h2>
            <p className="text-sm text-muted-foreground">Federated metadata discovery &amp; query</p>
          </div>
        </div>

        <Separator />

        <div className="flex flex-col gap-4 w-full">
          <ShadcnQueryBuilder
            allEntities={allEntities}
            selectedEntities={selectedEntities}
            onToggleEntity={toggleEntity}
            onClear={clearAll}
            limit={queryLimit}
            onLimitChange={setQueryLimit}
            onRunQuery={() => queryMutation.mutate()}
            isPending={queryMutation.isPending}
            isLoadingEntities={sessionLoading}
            onJoinsChange={setJoins}
            onFilterGroupsChange={setFilterGroups}
            onSortColumnsChange={setSortColumns}
          />

          {queryResults && (
            <div className="w-full space-y-2">
              <span className="px-1 text-xs font-medium text-muted-foreground">
                {queryResults.total} row{queryResults.total !== 1 ? 's' : ''} returned
              </span>
              <CrudDataTable
                key={queryVersion}
                columns={queryResults.columns}
                data={queryResults.rows}
                availableEntities={selectedEntities}
              />
            </div>
          )}
        </div>
      </div>
    </Section>
  )
}
