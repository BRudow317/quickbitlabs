import * as React from "react"
import { Plus, X } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { cn } from "@/lib/utils"
import type { Column, Entity, Operation, OperatorGroup } from "@/api/sessionApi"

// ── UI-layer types (carry React keys, raw string values) ───────────────────────

type Combinator = "AND" | "OR" | "NOT"

type FilterOp =
  | "=" | "!=" | ">" | "<" | ">=" | "<="
  | "IN" | "NOT IN"
  | "LIKE" | "NOT LIKE"
  | "BETWEEN" | "NOT BETWEEN"
  | "IS NULL" | "IS NOT NULL"

interface UICondition {
  id: string
  column: Column | null   // full Column from session catalog
  operator: FilterOp
  value: string
  value2: string  // only used for BETWEEN / NOT BETWEEN
}

interface UIGroup {
  id: string
  combinator: Combinator
  conditions: UICondition[]
  groups: UIGroup[]
}

// ── Operator metadata ──────────────────────────────────────────────────────────

const OPERATORS: { value: FilterOp; label: string }[] = [
  { value: "=",           label: "equals" },
  { value: "!=",          label: "not equals" },
  { value: ">",           label: "greater than" },
  { value: ">=",          label: "≥" },
  { value: "<",           label: "less than" },
  { value: "<=",          label: "≤" },
  { value: "LIKE",        label: "contains" },
  { value: "NOT LIKE",    label: "not contains" },
  { value: "IN",          label: "in list" },
  { value: "NOT IN",      label: "not in list" },
  { value: "BETWEEN",     label: "between" },
  { value: "NOT BETWEEN", label: "not between" },
  { value: "IS NULL",     label: "is null" },
  { value: "IS NOT NULL", label: "is not null" },
]

const NO_VALUE_OPS   = new Set<FilterOp>(["IS NULL", "IS NOT NULL"])
const DUAL_VALUE_OPS = new Set<FilterOp>(["BETWEEN", "NOT BETWEEN"])
const LIST_VALUE_OPS = new Set<FilterOp>(["IN", "NOT IN"])

const COMBINATORS: Combinator[] = ["AND", "OR", "NOT"]

// ── Factory helpers ────────────────────────────────────────────────────────────

function uid() { return Math.random().toString(36).slice(2) }

function makeCondition(): UICondition {
  return { id: uid(), column: null, operator: "=", value: "", value2: "" }
}

function makeGroup(combinator: Combinator = "AND"): UIGroup {
  return { id: uid(), combinator, conditions: [makeCondition()], groups: [] }
}

// ── Serialization to backend OperatorGroup ─────────────────────────────────────
// Operation.independent is a full Column object (the backend contract).
// Operation.dependent is the comparison value.
// OperatorGroup.condition is "AND" | "OR" | "NOT".
// OperatorGroup.operation_group is a flat list of Operations and nested OperatorGroups.

function serializeCondition(c: UICondition): Operation | null {
  if (!c.column) return null
  if (NO_VALUE_OPS.has(c.operator)) {
    return { independent: c.column, operator: c.operator, dependent: null }
  }
  if (DUAL_VALUE_OPS.has(c.operator)) {
    return { independent: c.column, operator: c.operator, dependent: [c.value, c.value2] }
  }
  if (LIST_VALUE_OPS.has(c.operator)) {
    return {
      independent: c.column,
      operator: c.operator,
      dependent: c.value.split(",").map(v => v.trim()).filter(Boolean),
    }
  }
  return { independent: c.column, operator: c.operator, dependent: c.value }
}

function serializeGroup(group: UIGroup): OperatorGroup {
  const ops: Array<Operation | OperatorGroup> = [
    ...group.conditions.map(serializeCondition).filter((op): op is Operation => op !== null),
    ...group.groups.map(serializeGroup),
  ]
  return {
    condition: group.combinator,
    operation_group: ops,
  }
}

function hasContent(group: UIGroup): boolean {
  return (
    group.conditions.some(c => c.column !== null) ||
    group.groups.some(hasContent)
  )
}

// ── FilterConditionRow ─────────────────────────────────────────────────────────

interface ConditionRowProps {
  condition: UICondition
  availableColumns: Column[]
  onChange: (updates: Partial<UICondition>) => void
  onRemove: () => void
}

function FilterConditionRow({ condition, availableColumns, onChange, onRemove }: ConditionRowProps) {
  const showValue = !NO_VALUE_OPS.has(condition.operator)
  const isDual    = DUAL_VALUE_OPS.has(condition.operator)
  const isList    = LIST_VALUE_OPS.has(condition.operator)

  return (
    <div className="flex flex-wrap items-center gap-2">
      {/* Column selector — always a Select backed by real Column objects */}
      <Select
        value={condition.column?.name ?? undefined}
        onValueChange={val => {
          const col = availableColumns.find(c => c.name === val) ?? null
          onChange({ column: col })
        }}
      >
        <SelectTrigger className="h-8 w-40 text-xs">
          <SelectValue placeholder="column" />
        </SelectTrigger>
        <SelectContent>
          <SelectGroup>
            {availableColumns.map(col => (
              <SelectItem key={col.name} value={col.name} className="text-xs">
                {col.alias ?? col.name}
              </SelectItem>
            ))}
          </SelectGroup>
        </SelectContent>
      </Select>

      {/* Operator */}
      <Select
        value={condition.operator}
        onValueChange={val => onChange({ operator: val as FilterOp, value: "", value2: "" })}
      >
        <SelectTrigger className="h-8 w-32 text-xs">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {OPERATORS.map(op => (
            <SelectItem key={op.value} value={op.value} className="text-xs">
              {op.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {/* Value input(s) */}
      {showValue && (
        isDual ? (
          <>
            <Input
              placeholder="from"
              value={condition.value}
              onChange={e => onChange({ value: e.target.value })}
              className="h-8 w-28 text-xs"
            />
            <span className="text-xs text-muted-foreground">and</span>
            <Input
              placeholder="to"
              value={condition.value2}
              onChange={e => onChange({ value2: e.target.value })}
              className="h-8 w-28 text-xs"
            />
          </>
        ) : (
          <Input
            placeholder={isList ? "a, b, c" : "value"}
            value={condition.value}
            onChange={e => onChange({ value: e.target.value })}
            className="h-8 w-44 text-xs"
          />
        )
      )}

      <Button
        variant="ghost"
        size="icon"
        className="h-8 w-8 shrink-0 text-muted-foreground hover:text-destructive"
        onClick={onRemove}
      >
        <X className="h-3.5 w-3.5" />
      </Button>
    </div>
  )
}

// ── FilterGroupNode (recursive) ────────────────────────────────────────────────

interface GroupNodeProps {
  group: UIGroup
  availableColumns: Column[]
  isRoot?: boolean
  onUpdate: (updater: (g: UIGroup) => UIGroup) => void
  onRemove?: () => void
}

function FilterGroupNode({ group, availableColumns, isRoot, onUpdate, onRemove }: GroupNodeProps) {
  const update = (updater: (g: UIGroup) => UIGroup) => onUpdate(updater)

  const addCondition = () =>
    update(g => ({ ...g, conditions: [...g.conditions, makeCondition()] }))

  const removeCondition = (id: string) =>
    update(g => ({ ...g, conditions: g.conditions.filter(c => c.id !== id) }))

  const patchCondition = (id: string, patch: Partial<UICondition>) =>
    update(g => ({
      ...g,
      conditions: g.conditions.map(c => c.id === id ? { ...c, ...patch } : c),
    }))

  const addSubGroup = () =>
    update(g => ({ ...g, groups: [...g.groups, makeGroup()] }))

  const removeSubGroup = (id: string) =>
    update(g => ({ ...g, groups: g.groups.filter(sg => sg.id !== id) }))

  const patchSubGroup = (id: string, updater: (sg: UIGroup) => UIGroup) =>
    update(g => ({
      ...g,
      groups: g.groups.map(sg => sg.id === id ? updater(sg) : sg),
    }))

  return (
    <div className={cn(
      "rounded-md border p-3 space-y-2.5",
      isRoot
        ? "border-border bg-card"
        : "border-border/50 bg-muted/20 ml-4"
    )}>
      {/* Combinator toggle + remove */}
      <div className="flex items-center gap-1">
        <span className="text-xs text-muted-foreground mr-1">Match</span>
        {COMBINATORS.map(c => (
          <Button
            key={c}
            variant={group.combinator === c ? "default" : "outline"}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => update(g => ({ ...g, combinator: c }))}
          >
            {c}
          </Button>
        ))}
        {!isRoot && onRemove && (
          <Button
            variant="ghost"
            size="sm"
            className="ml-auto h-6 px-2 text-xs text-muted-foreground hover:text-destructive"
            onClick={onRemove}
          >
            <X className="h-3 w-3 mr-1" />
            Remove group
          </Button>
        )}
      </div>

      {/* Conditions */}
      {group.conditions.length > 0 && (
        <div className="space-y-2 pl-1">
          {group.conditions.map(condition => (
            <FilterConditionRow
              key={condition.id}
              condition={condition}
              availableColumns={availableColumns}
              onChange={patch => patchCondition(condition.id, patch)}
              onRemove={() => removeCondition(condition.id)}
            />
          ))}
        </div>
      )}

      {/* Nested groups */}
      {group.groups.map(sg => (
        <FilterGroupNode
          key={sg.id}
          group={sg}
          availableColumns={availableColumns}
          onUpdate={updater => patchSubGroup(sg.id, updater)}
          onRemove={() => removeSubGroup(sg.id)}
        />
      ))}

      {/* Add actions */}
      <div className="flex gap-2 pt-0.5">
        <Button variant="outline" size="sm" className="h-7 text-xs" onClick={addCondition}>
          <Plus className="h-3 w-3 mr-1" />
          Condition
        </Button>
        <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={addSubGroup}>
          <Plus className="h-3 w-3 mr-1" />
          Group
        </Button>
      </div>
    </div>
  )
}

// ── FilterBuilder (public export) ──────────────────────────────────────────────

interface FilterBuilderProps {
  selectedEntities: Entity[]
  onChange: (groups: OperatorGroup[]) => void
  className?: string
}

export function FilterBuilder({ selectedEntities, onChange, className }: FilterBuilderProps) {
  const [root, setRoot] = React.useState<UIGroup>(() => makeGroup("AND"))

  const availableColumns = React.useMemo(() => {
    const seen = new Set<string>()
    const cols: Column[] = []
    for (const entity of selectedEntities) {
      for (const col of entity.columns ?? []) {
        if (!seen.has(col.name)) { seen.add(col.name); cols.push(col) }
      }
    }
    return cols.sort((a, b) => (a.alias ?? a.name).localeCompare(b.alias ?? b.name))
  }, [selectedEntities])

  const handleRootUpdate = React.useCallback(
    (updater: (g: UIGroup) => UIGroup) => {
      setRoot(prev => {
        const next = updater(prev)
        onChange(hasContent(next) ? [serializeGroup(next)] : [])
        return next
      })
    },
    [onChange],
  )

  const handleReset = () => { setRoot(makeGroup("AND")); onChange([]) }

  return (
    <div className={cn("space-y-3", className)}>
      <div className="flex items-center justify-between">
        <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Filters</span>
        <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={handleReset}>
          Reset
        </Button>
      </div>
      <FilterGroupNode
        group={root}
        availableColumns={availableColumns}
        isRoot
        onUpdate={handleRootUpdate}
      />
    </div>
  )
}
