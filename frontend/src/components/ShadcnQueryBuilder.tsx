import { Eraser, Loader2, Play } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { X } from "lucide-react"
import { EntityTreeNav } from "@/components/EntityTreeNav"
import { JoinBuilder } from "@/components/JoinBuilder"
import { FilterBuilder } from "@/components/FilterBuilder"
import { SortBuilder } from "@/components/SortBuilder"
import type { Entity, Join, OperatorGroup, Sort } from "@/api/sessionApi"
import { cn } from "@/lib/utils"

interface ShadcnQueryBuilderProps {
  allEntities: Entity[]
  selectedEntities: Entity[]
  onToggleEntity: (entity: Entity) => void
  onClear: () => void
  limit: string
  onLimitChange: (value: string) => void
  onRunQuery: () => void
  isPending: boolean
  isLoadingEntities?: boolean

  onJoinsChange: (joins: Join[]) => void
  onFilterGroupsChange: (groups: OperatorGroup[]) => void
  onSortColumnsChange: (sorts: Sort[]) => void

  className?: string
}

export function ShadcnQueryBuilder({
  allEntities,
  selectedEntities,
  onToggleEntity,
  onClear,
  limit,
  onLimitChange,
  onRunQuery,
  isPending,
  isLoadingEntities,
  onJoinsChange,
  onFilterGroupsChange,
  onSortColumnsChange,
  className,
}: ShadcnQueryBuilderProps) {
  const selectedNames = new Set(selectedEntities.map(e => e.name))
  const hasEntities   = selectedEntities.length > 0
  const hasMultiple   = selectedEntities.length >= 2

  return (
    <Card className={cn("w-full", className)}>
      <CardHeader className="pb-0 pt-4 px-4">
        <CardTitle className="text-sm font-medium">Query Builder</CardTitle>
      </CardHeader>

      <CardContent className="p-0">
        {/* -- Top: entity nav (left) + selections (right) -- */}
        <div className="flex min-h-50 border-b">
          {/* Left: source → entity tree */}
          <div className="w-52 shrink-0 overflow-y-auto border-r p-3" style={{ maxHeight: 300 }}>
            <EntityTreeNav
              entities={allEntities}
              selectedNames={selectedNames}
              onToggle={onToggleEntity}
              isLoading={isLoadingEntities}
            />
          </div>

          {/* Right: selected badges + limit + actions */}
          <div className="flex flex-1 flex-col gap-3 p-4">
            {/* Selected entity badges */}
            <div className="flex flex-wrap gap-1.5 min-h-8 items-start content-start">
              {selectedEntities.length === 0 ? (
                <p className="text-xs text-muted-foreground pt-1">
                  Select entities from the panel.
                </p>
              ) : (
                selectedEntities.map(e => (
                  <Badge key={e.name} variant="secondary" className="flex items-center gap-1 pr-1 py-0.5">
                    {e.alias ?? e.name}
                    <button
                      onClick={evt => { evt.stopPropagation(); onToggleEntity(e) }}
                      className="rounded-full p-0.5 hover:bg-muted"
                    >
                      <X className="h-3 w-3" />
                    </button>
                  </Badge>
                ))
              )}
            </div>

            {/* Limit + run + clear */}
            <div className="mt-auto flex items-end gap-3 flex-wrap">
              <div className="grid w-20 items-center gap-1">
                <Label htmlFor="ql-limit" className="text-xs">Limit</Label>
                <Input
                  id="ql-limit"
                  type="number"
                  value={limit}
                  onChange={e => onLimitChange(e.target.value)}
                  className="h-9"
                />
              </div>

              <div className="flex gap-2">
                <Button disabled={!hasEntities || isPending} onClick={onRunQuery} className="h-9">
                  {isPending
                    ? <><Loader2 className="mr-2 h-4 w-4 animate-spin" />Querying</>
                    : <><Play    className="mr-2 h-4 w-4" />Run Query</>
                  }
                </Button>
                {hasEntities && (
                  <Button variant="outline" onClick={onClear} className="h-9">
                    <Eraser className="mr-2 h-4 w-4" />Clear
                  </Button>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* -- Joins (2+ entities) -- */}
        {hasMultiple && (
          <div className="border-b px-4 py-3">
            <JoinBuilder selectedEntities={selectedEntities} onChange={onJoinsChange} />
          </div>
        )}

        {/* -- Filters -- */}
        {hasEntities && (
          <div className="border-b px-4 py-3">
            <FilterBuilder selectedEntities={selectedEntities} onChange={onFilterGroupsChange} />
          </div>
        )}

        {/* -- Sort -- */}
        {hasEntities && (
          <div className="px-4 py-3">
            <SortBuilder selectedEntities={selectedEntities} onChange={onSortColumnsChange} />
          </div>
        )}
      </CardContent>
    </Card>
  )
}
