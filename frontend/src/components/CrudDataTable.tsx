import * as React from "react"
import { Plus, Save, Trash2, Loader2, RotateCcw } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { useToast } from "@/context/ToastContext"
import { upsertData, deleteData, type Entity, type Catalog } from "@/api/sessionApi"
import { cn } from "@/lib/utils"

// ── Row state ──────────────────────────────────────────────────────────────────

type RowState = "pristine" | "dirty" | "new"

interface ManagedRow {
  _id: string
  _state: RowState
  [col: string]: unknown
}

function uid() { return Math.random().toString(36).slice(2) }

function toManaged(rows: Record<string, unknown>[]): ManagedRow[] {
  return rows.map(r => ({ ...r, _id: uid(), _state: "pristine" as RowState }))
}

// ── Component ──────────────────────────────────────────────────────────────────

interface CrudDataTableProps {
  columns: string[]
  data: Record<string, unknown>[]
  availableEntities: Entity[]
  maxHeight?: string
}

export function CrudDataTable({
  columns: initCols,
  data: initData,
  availableEntities,
  maxHeight = "460px",
}: CrudDataTableProps) {
  const [cols, setCols] = React.useState<string[]>(initCols)
  const [rows, setRows] = React.useState<ManagedRow[]>(() => toManaged(initData))

  // Inline cell editing
  const [editing, setEditing] = React.useState<{ rowId: string; col: string } | null>(null)
  const [editVal, setEditVal] = React.useState("")

  // Row selection (checkboxes)
  const [selected, setSelected] = React.useState<Set<string>>(new Set())

  // Write target
  const [target, setTarget] = React.useState<string>(availableEntities[0]?.name ?? "")

  // Add column inline form
  const [addingCol, setAddingCol] = React.useState(false)
  const [newColName, setNewColName] = React.useState("")

  const [saving, setSaving] = React.useState(false)
  const { toast } = useToast()

  // ── Derived ────────────────────────────────────────────────────────────────

  const dirtyRows = rows.filter(r => r._state !== "pristine")
  const newCount = dirtyRows.filter(r => r._state === "new").length
  const modCount = dirtyRows.filter(r => r._state === "dirty").length
  const allChecked = rows.length > 0 && selected.size === rows.length
  const someChecked = selected.size > 0 && !allChecked
  const targetEntity = availableEntities.find(e => e.name === target)

  // ── Editing ────────────────────────────────────────────────────────────────

  function startEdit(rowId: string, col: string, val: unknown) {
    setEditing({ rowId, col })
    setEditVal(val == null ? "" : String(val))
  }

  function commitEdit() {
    if (!editing) return
    setRows(prev =>
      prev.map(r =>
        r._id !== editing.rowId
          ? r
          : { ...r, [editing.col]: editVal, _state: r._state === "pristine" ? "dirty" : r._state }
      )
    )
    setEditing(null)
  }

  function cancelEdit() { setEditing(null) }

  function onCellKeyDown(e: React.KeyboardEvent) {
    if (e.key === "Enter") { e.preventDefault(); commitEdit() }
    if (e.key === "Escape") { e.preventDefault(); cancelEdit() }
  }

  // ── Row management ─────────────────────────────────────────────────────────

  function addRow() {
    const row: ManagedRow = { _id: uid(), _state: "new" }
    for (const col of cols) row[col] = ""
    setRows(prev => [...prev, row])
  }

  // ── Column management ──────────────────────────────────────────────────────

  function confirmAddCol() {
    const name = newColName.trim()
    if (!name || cols.includes(name)) return
    setCols(prev => [...prev, name])
    setRows(prev => prev.map(r => ({ ...r, [name]: "" })))
    setNewColName("")
    setAddingCol(false)
  }

  function cancelAddCol() { setAddingCol(false); setNewColName("") }

  // ── Selection ──────────────────────────────────────────────────────────────

  function toggleRow(id: string) {
    setSelected(prev => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }

  function toggleAll() {
    setSelected(prev =>
      prev.size === rows.length ? new Set() : new Set(rows.map(r => r._id))
    )
  }

  // ── Write operations ───────────────────────────────────────────────────────

  function makeCatalog(): Catalog | null {
    if (!targetEntity) return null
    return { entities: [targetEntity] }
  }

  function stripMeta(row: ManagedRow): Record<string, unknown> {
    const { _id, _state, ...rest } = row
    void _id; void _state
    return rest
  }

  function handleDiscard() {
    setRows(toManaged(initData))
    setCols(initCols)
    setSelected(new Set())
    setEditing(null)
  }

  async function handleSave() {
    const catalog = makeCatalog()
    if (!catalog || dirtyRows.length === 0) return
    setSaving(true)
    try {
      await upsertData(catalog, dirtyRows.map(stripMeta), cols)
      setRows(prev => prev.map(r => ({ ...r, _state: "pristine" as RowState })))
      toast.success("Saved successfully")
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Save failed")
    } finally {
      setSaving(false)
    }
  }

  async function handleDelete() {
    const catalog = makeCatalog()
    if (!catalog || selected.size === 0) return
    const toDelete = rows.filter(r => selected.has(r._id))
    setSaving(true)
    try {
      await deleteData(catalog, toDelete.map(stripMeta), cols)
      setRows(prev => prev.filter(r => !selected.has(r._id)))
      setSelected(new Set())
      toast.success(`${toDelete.length} row(s) deleted`)
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Delete failed")
    } finally {
      setSaving(false)
    }
  }

  // ── Render ─────────────────────────────────────────────────────────────────

  return (
    <div className="flex flex-col rounded-md border bg-background">
      {/* ── Toolbar ── */}
      <div className="flex flex-wrap items-center gap-2 px-3 py-2 border-b">
        {/* Left: row / column actions */}
        <Button size="sm" variant="outline" className="h-7 text-xs" onClick={addRow} disabled={saving}>
          <Plus className="h-3 w-3 mr-1" />
          Add Row
        </Button>

        {addingCol ? (
          <div className="flex items-center gap-1">
            <Input
              autoFocus
              placeholder="Column name"
              value={newColName}
              onChange={e => setNewColName(e.target.value)}
              onKeyDown={e => {
                if (e.key === "Enter") confirmAddCol()
                if (e.key === "Escape") cancelAddCol()
              }}
              className="h-7 w-32 text-xs"
            />
            <Button size="sm" className="h-7 text-xs" onClick={confirmAddCol}>Add</Button>
            <Button size="sm" variant="ghost" className="h-7 text-xs" onClick={cancelAddCol}>Cancel</Button>
          </div>
        ) : (
          <Button size="sm" variant="outline" className="h-7 text-xs" onClick={() => setAddingCol(true)} disabled={saving}>
            <Plus className="h-3 w-3 mr-1" />
            Add Column
          </Button>
        )}

        {/* Right: save / delete / target */}
        <div className="ml-auto flex items-center gap-2">
          {selected.size > 0 && (
            <Button
              size="sm"
              variant="destructive"
              className="h-7 text-xs"
              onClick={handleDelete}
              disabled={saving}
            >
              {saving ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Trash2 className="h-3 w-3 mr-1" />}
              Delete ({selected.size})
            </Button>
          )}

          {availableEntities.length > 0 && (
            <Select value={target} onValueChange={setTarget}>
              <SelectTrigger className="h-7 w-44 text-xs">
                <SelectValue placeholder="Save target…" />
              </SelectTrigger>
              <SelectContent>
                {availableEntities.map(e => (
                  <SelectItem key={e.name} value={e.name} className="text-xs">
                    <span className="truncate">{e.name}</span>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          )}

          {dirtyRows.length > 0 && (
            <Button
              size="sm"
              variant="outline"
              className="h-7 text-xs"
              onClick={handleDiscard}
              disabled={saving}
            >
              <RotateCcw className="h-3 w-3 mr-1" />
              Discard ({dirtyRows.length})
            </Button>
          )}

          <Button
            size="sm"
            className="h-7 text-xs"
            onClick={handleSave}
            disabled={saving || dirtyRows.length === 0 || !targetEntity}
          >
            {saving
              ? <Loader2 className="h-3 w-3 mr-1 animate-spin" />
              : <Save className="h-3 w-3 mr-1" />}
            Save{dirtyRows.length > 0 ? ` (${dirtyRows.length})` : ""}
          </Button>
        </div>
      </div>

      {/* ── Table ── */}
      <div className="w-full overflow-auto" style={{ maxHeight }}>
        <Table>
          <TableHeader className="sticky top-0 z-10 bg-muted/50 backdrop-blur-sm">
            <TableRow>
              <TableHead className="w-10 border-r px-3">
                <Checkbox
                  checked={allChecked ? true : someChecked ? "indeterminate" : false}
                  onCheckedChange={toggleAll}
                />
              </TableHead>
              {cols.map(col => (
                <TableHead key={col} className="border-r last:border-r-0 text-xs whitespace-nowrap">
                  {col}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>

          <TableBody>
            {rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={cols.length + 1} className="h-24 text-center text-muted-foreground text-sm">
                  No results.
                </TableCell>
              </TableRow>
            ) : (
              rows.map(row => (
                <TableRow
                  key={row._id}
                  data-state={selected.has(row._id) ? "selected" : undefined}
                  className={cn(
                    row._state === "new" && "bg-green-500/5",
                    row._state === "dirty" && "bg-amber-500/5",
                  )}
                >
                  <TableCell className="w-10 border-r px-3">
                    <Checkbox
                      checked={selected.has(row._id)}
                      onCheckedChange={() => toggleRow(row._id)}
                    />
                  </TableCell>

                  {cols.map(col => {
                    const isEditing = editing?.rowId === row._id && editing?.col === col
                    return (
                      <TableCell
                        key={col}
                        className="border-r last:border-r-0 p-0"
                        onDoubleClick={() => !isEditing && startEdit(row._id, col, row[col])}
                      >
                        {isEditing ? (
                          <input
                            autoFocus
                            value={editVal}
                            onChange={e => setEditVal(e.target.value)}
                            onBlur={commitEdit}
                            onKeyDown={onCellKeyDown}
                            className="w-full h-full min-h-[2.25rem] px-3 py-2 text-sm bg-transparent outline-none ring-2 ring-inset ring-primary"
                          />
                        ) : (
                          <div className="px-3 py-2 text-sm min-h-[2.25rem] cursor-default select-none truncate max-w-[240px]">
                            {row[col] == null ? (
                              <span className="text-muted-foreground/40 italic text-xs">null</span>
                            ) : (
                              String(row[col])
                            )}
                          </div>
                        )}
                      </TableCell>
                    )
                  })}
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {/* ── Status bar ── */}
      <div className="flex items-center gap-4 px-3 py-1 border-t text-[10px] text-muted-foreground">
        <span>{rows.length} rows</span>
        {selected.size > 0 && <span>{selected.size} selected</span>}
        {newCount > 0 && (
          <span className="text-green-600 dark:text-green-400">+{newCount} new</span>
        )}
        {modCount > 0 && (
          <span className="text-amber-600 dark:text-amber-400">~{modCount} modified</span>
        )}
      </div>
    </div>
  )
}
