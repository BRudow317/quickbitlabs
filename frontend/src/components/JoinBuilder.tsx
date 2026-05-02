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
import type { Entity, Join } from "@/api/sessionApi"

// ── Types ──────────────────────────────────────────────────────────────────────

type JoinType = "INNER" | "LEFT" | "OUTER"

interface UIJoin {
  id: string
  leftEntityName: string
  leftColName: string
  joinType: JoinType
  rightEntityName: string
  rightColName: string
}

function uid() { return Math.random().toString(36).slice(2) }

function makeJoin(entities: Entity[]): UIJoin {
  return {
    id: uid(),
    leftEntityName: entities[0]?.name ?? "",
    leftColName: "",
    joinType: "INNER",
    rightEntityName: entities[1]?.name ?? "",
    rightColName: "",
  }
}

const JOIN_LABELS: Record<JoinType, string> = {
  INNER: "INNER JOIN",
  LEFT:  "LEFT JOIN",
  OUTER: "OUTER JOIN",
}

// ── Component ──────────────────────────────────────────────────────────────────

interface JoinBuilderProps {
  selectedEntities: Entity[]
  onChange: (joins: Join[]) => void
  className?: string
}

export function JoinBuilder({ selectedEntities, onChange, className }: JoinBuilderProps) {
  const [joins, setJoins] = React.useState<UIJoin[]>([])

  const entityMap = React.useMemo(
    () => new Map(selectedEntities.map(e => [e.name, e])),
    [selectedEntities],
  )

  const serialize = React.useCallback(
    (uiJoins: UIJoin[]): Join[] =>
      uiJoins.flatMap(j => {
        const le = entityMap.get(j.leftEntityName)
        const re = entityMap.get(j.rightEntityName)
        if (!le || !re) return []
        const lc = le.columns?.find(c => c.name === j.leftColName)
        const rc = re.columns?.find(c => c.name === j.rightColName)
        if (!lc || !rc) return []
        return [{
          left_entity: le, left_column: lc,
          right_entity: re, right_column: rc,
          join_type: j.joinType,
        }]
      }),
    [entityMap],
  )

  const commit = (next: UIJoin[]) => { setJoins(next); onChange(serialize(next)) }

  const addJoin  = ()    => commit([...joins, makeJoin(selectedEntities)])
  const removeJoin = (id: string) => commit(joins.filter(j => j.id !== id))

  const patch = (id: string, delta: Partial<UIJoin>) =>
    commit(joins.map(j => {
      if (j.id !== id) return j
      const next = { ...j, ...delta }
      if (delta.leftEntityName  !== undefined && delta.leftEntityName  !== j.leftEntityName)  next.leftColName  = ""
      if (delta.rightEntityName !== undefined && delta.rightEntityName !== j.rightEntityName) next.rightColName = ""
      return next
    }))

  return (
    <div className={cn("space-y-2", className)}>
      <div className="flex items-center justify-between">
        <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Joins</span>
        <Button variant="outline" size="sm" className="h-7 text-xs" onClick={addJoin}>
          <Plus className="h-3 w-3 mr-1" />Add Join
        </Button>
      </div>

      {joins.map(j => {
        const leftEntity  = entityMap.get(j.leftEntityName)
        const rightEntity = entityMap.get(j.rightEntityName)
        return (
          <div key={j.id} className="flex flex-wrap items-center gap-2 rounded-md border bg-muted/20 p-2">
            <Select value={j.leftEntityName || undefined} onValueChange={val => patch(j.id, { leftEntityName: val })}>
              <SelectTrigger className="h-8 w-36 text-xs"><SelectValue placeholder="entity" /></SelectTrigger>
              <SelectContent>
                {selectedEntities.map(e => (
                  <SelectItem key={e.name} value={e.name} className="text-xs">{e.alias ?? e.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={j.leftColName || undefined} onValueChange={val => patch(j.id, { leftColName: val })} disabled={!leftEntity}>
              <SelectTrigger className="h-8 w-36 text-xs"><SelectValue placeholder="column" /></SelectTrigger>
              <SelectContent>
                {(leftEntity?.columns ?? []).map(c => (
                  <SelectItem key={c.name} value={c.name} className="text-xs">{c.alias ?? c.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={j.joinType} onValueChange={val => patch(j.id, { joinType: val as JoinType })}>
              <SelectTrigger className="h-8 w-32 text-xs"><SelectValue /></SelectTrigger>
              <SelectContent>
                {(Object.entries(JOIN_LABELS) as [JoinType, string][]).map(([val, label]) => (
                  <SelectItem key={val} value={val} className="text-xs">{label}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={j.rightEntityName || undefined} onValueChange={val => patch(j.id, { rightEntityName: val })}>
              <SelectTrigger className="h-8 w-36 text-xs"><SelectValue placeholder="entity" /></SelectTrigger>
              <SelectContent>
                {selectedEntities.map(e => (
                  <SelectItem key={e.name} value={e.name} className="text-xs">{e.alias ?? e.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={j.rightColName || undefined} onValueChange={val => patch(j.id, { rightColName: val })} disabled={!rightEntity}>
              <SelectTrigger className="h-8 w-36 text-xs"><SelectValue placeholder="column" /></SelectTrigger>
              <SelectContent>
                {(rightEntity?.columns ?? []).map(c => (
                  <SelectItem key={c.name} value={c.name} className="text-xs">{c.alias ?? c.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Button variant="ghost" size="icon"
              className="ml-auto h-8 w-8 shrink-0 text-muted-foreground hover:text-destructive"
              onClick={() => removeJoin(j.id)}
            >
              <X className="h-3.5 w-3.5" />
            </Button>
          </div>
        )
      })}
    </div>
  )
}
