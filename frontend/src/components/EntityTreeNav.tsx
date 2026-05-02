import * as React from "react"
import { Check, ChevronDown, ChevronRight } from "lucide-react"
import { Skeleton } from "@/components/ui/skeleton"
import { cn } from "@/lib/utils"
import type { Entity } from "@/api/sessionApi"

// ── Helpers ────────────────────────────────────────────────────────────────────

function getSource(entity: Entity): string {
  return entity.plugin ?? entity.columns?.[0]?.locator?.plugin ?? "unknown"
}

function titleCase(s: string) {
  return s.charAt(0).toUpperCase() + s.slice(1)
}

// ── Component ──────────────────────────────────────────────────────────────────

interface EntityTreeNavProps {
  entities: Entity[]
  selectedNames: Set<string>
  onToggle: (entity: Entity) => void
  isLoading?: boolean
}

export function EntityTreeNav({ entities, selectedNames, onToggle, isLoading }: EntityTreeNavProps) {
  // Group by source plugin, both groups and entities sorted alphabetically
  const groups = React.useMemo(() => {
    const map = new Map<string, Entity[]>()
    for (const entity of entities) {
      const src = getSource(entity)
      if (!map.has(src)) map.set(src, [])
      map.get(src)!.push(entity)
    }
    return Array.from(map.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([source, ents]) => ({
        source,
        entities: [...ents].sort((a, b) =>
          (a.alias ?? a.name).localeCompare(b.alias ?? b.name),
        ),
      }))
  }, [entities])

  // All sources open by default; sync when new sources arrive
  const [openSources, setOpenSources] = React.useState<Set<string>>(
    () => new Set(groups.map(g => g.source)),
  )
  React.useEffect(() => {
    setOpenSources(prev => {
      const next = new Set(prev)
      groups.forEach(g => next.add(g.source))
      return next
    })
  }, [groups])

  const toggleSource = (src: string) =>
    setOpenSources(prev => {
      const next = new Set(prev)
      next.has(src) ? next.delete(src) : next.add(src)
      return next
    })

  if (isLoading) {
    return (
      <div className="space-y-1.5 p-1">
        {[80, 60, 70, 55, 65].map((w, i) => (
          <Skeleton key={i} className="h-6" style={{ width: `${w}%` }} />
        ))}
      </div>
    )
  }

  if (groups.length === 0) {
    return (
      <p className="px-2 py-3 text-xs text-muted-foreground">No entities available.</p>
    )
  }

  return (
    <div className="space-y-0.5">
      {groups.map(({ source, entities }) => {
        const isOpen = openSources.has(source)
        const selectedCount = entities.filter(e => selectedNames.has(e.name)).length

        return (
          <div key={source}>
            {/* ── Source header ── */}
            <button
              className="flex w-full items-center gap-1.5 rounded-md px-2 py-1.5 text-xs font-semibold
                         hover:bg-accent hover:text-accent-foreground transition-colors"
              onClick={() => toggleSource(source)}
            >
              {isOpen
                ? <ChevronDown  className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                : <ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
              }
              <span className="truncate">{titleCase(source)}</span>
              <span className="ml-auto shrink-0 tabular-nums text-[10px] text-muted-foreground">
                {selectedCount > 0 ? `${selectedCount}/` : ""}{entities.length}
              </span>
            </button>

            {/* ── Entity rows ── */}
            {isOpen && (
              <div className="ml-3 space-y-0.5 border-l border-border/50 pl-2">
                {entities.map(entity => {
                  const isSelected = selectedNames.has(entity.name)
                  return (
                    <button
                      key={entity.name}
                      className={cn(
                        "flex w-full items-center gap-1.5 rounded-md px-2 py-1 text-xs transition-colors",
                        isSelected
                          ? "bg-primary/10 text-primary font-medium"
                          : "text-foreground/80 hover:bg-accent hover:text-accent-foreground",
                      )}
                      onClick={() => onToggle(entity)}
                    >
                      <span className="flex h-3.5 w-3.5 shrink-0 items-center justify-center">
                        {isSelected && <Check className="h-3 w-3" />}
                      </span>
                      <span className="truncate">{entity.alias ?? entity.name}</span>
                    </button>
                  )
                })}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}
