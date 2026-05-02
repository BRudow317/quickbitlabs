import * as React from "react"
import { Plus, X } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { cn } from "@/lib/utils"
import type { Column, Entity, Sort } from "@/api/sessionApi"

type Direction = "ASC" | "DESC"

interface UISort {
  id: string
  colName: string
  direction: Direction
  nullsFirst: boolean | null
}

function uid() { return Math.random().toString(36).slice(2) }

interface SortBuilderProps {
  selectedEntities: Entity[]
  onChange: (sorts: Sort[]) => void
  className?: string
}

export function SortBuilder({ selectedEntities, onChange, className }: SortBuilderProps) {
  const [sorts, setSorts] = React.useState<UISort[]>([])

  const availableColumns = React.useMemo<Column[]>(() => {
    const seen = new Set<string>()
    const cols: Column[] = []
    for (const entity of selectedEntities) {
      for (const col of entity.columns ?? []) {
        if (!seen.has(col.name)) { seen.add(col.name); cols.push(col) }
      }
    }
    return cols.sort((a, b) => (a.alias ?? a.name).localeCompare(b.alias ?? b.name))
  }, [selectedEntities])

  const colMap = React.useMemo(() => new Map(availableColumns.map(c => [c.name, c])), [availableColumns])

  const serialize = React.useCallback(
    (uiSorts: UISort[]): Sort[] =>
      uiSorts.flatMap(s => {
        const col = colMap.get(s.colName)
        return col ? [{ column: col, direction: s.direction, nulls_first: s.nullsFirst }] : []
      }),
    [colMap],
  )

  const commit = (next: UISort[]) => { setSorts(next); onChange(serialize(next)) }
  const addSort    = ()                          => commit([...sorts, { id: uid(), colName: "", direction: "ASC", nullsFirst: null }])
  const removeSort = (id: string)               => commit(sorts.filter(s => s.id !== id))
  const patch      = (id: string, d: Partial<UISort>) => commit(sorts.map(s => s.id === id ? { ...s, ...d } : s))

  return (
    <div className={cn("space-y-2", className)}>
      <div className="flex items-center justify-between">
        <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Sort</span>
        <Button variant="outline" size="sm" className="h-7 text-xs" onClick={addSort}>
          <Plus className="h-3 w-3 mr-1" />Add Sort
        </Button>
      </div>

      {sorts.map((s, idx) => (
        <div key={s.id} className="flex flex-wrap items-center gap-2 rounded-md border bg-muted/20 p-2">
          <span className="w-5 text-center text-[10px] tabular-nums text-muted-foreground">{idx + 1}</span>

          <Select value={s.colName || undefined} onValueChange={val => patch(s.id, { colName: val })}>
            <SelectTrigger className="h-8 w-44 text-xs"><SelectValue placeholder="column" /></SelectTrigger>
            <SelectContent>
              {availableColumns.map(col => (
                <SelectItem key={col.name} value={col.name} className="text-xs">{col.alias ?? col.name}</SelectItem>
              ))}
            </SelectContent>
          </Select>

          {/* Direction toggle */}
          <div className="flex overflow-hidden rounded-md border">
            {(["ASC", "DESC"] as Direction[]).map(dir => (
              <Button
                key={dir}
                variant="ghost"
                size="sm"
                className={cn(
                  "h-8 rounded-none px-3 text-xs",
                  s.direction === dir
                    ? "bg-primary text-primary-foreground hover:bg-primary/90 hover:text-primary-foreground"
                    : "hover:bg-muted",
                )}
                onClick={() => patch(s.id, { direction: dir })}
              >
                {dir}
              </Button>
            ))}
          </div>

          <Select
            value={s.nullsFirst === null ? "default" : s.nullsFirst ? "first" : "last"}
            onValueChange={val => patch(s.id, { nullsFirst: val === "default" ? null : val === "first" })}
          >
            <SelectTrigger className="h-8 w-32 text-xs"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="default" className="text-xs">Nulls default</SelectItem>
              <SelectItem value="first"   className="text-xs">Nulls first</SelectItem>
              <SelectItem value="last"    className="text-xs">Nulls last</SelectItem>
            </SelectContent>
          </Select>

          <Button variant="ghost" size="icon"
            className="ml-auto h-8 w-8 shrink-0 text-muted-foreground hover:text-destructive"
            onClick={() => removeSort(s.id)}
          >
            <X className="h-3.5 w-3.5" />
          </Button>
        </div>
      ))}
    </div>
  )
}
