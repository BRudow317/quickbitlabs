import * as React from "react"
import { ChevronRight, Database, Search, Check } from "lucide-react"
import { cn } from "@/lib/utils"
import type { Entity } from "@/api/sessionApi"

import { Button } from "@/components/ui/button"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"

interface SourceEntitySelectorProps {
  entities: Entity[]
  selectedNames: Set<string>
  onToggleEntity: (entity: Entity) => void
  isLoading?: boolean
  className?: string
}

export function SourceEntitySelector({
  entities,
  selectedNames,
  onToggleEntity,
  isLoading,
  className,
}: SourceEntitySelectorProps) {
  const [open, setOpen] = React.useState(false)
  const [activeSource, setActiveSource] = React.useState<string | null>(null)
  const [search, setSearch] = React.useState("")

  // Grouping logic: Source -> Entities
  const grouped = React.useMemo(() => {
    const map = new Map<string, Entity[]>()
    for (const entity of entities) {
      const src = entity.plugin || entity.columns?.[0]?.locator?.plugin || "unknown"
      if (!map.has(src)) map.set(src, [])
      map.get(src)!.push(entity)
    }
    return Array.from(map.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([source, ents]) => ({
        source,
        entities: [...ents].sort((a, b) => a.name.localeCompare(b.name)),
      }))
  }, [entities])

  // Filtered entities within the active source based on search
  const filteredEntities = React.useMemo(() => {
    if (!activeSource) return []
    const sourceGroup = grouped.find(g => g.source === activeSource)
    if (!sourceGroup) return []
    if (!search) return sourceGroup.entities
    const term = search.toLowerCase()
    return sourceGroup.entities.filter(e => 
      e.name.toLowerCase().includes(term) || 
      (e.alias && e.alias.toLowerCase().includes(term))
    )
  }, [activeSource, grouped, search])

  const selectedCount = selectedNames.size
  const triggerLabel = selectedCount === 0
    ? "Select sources & entities..."
    : `${selectedCount} selected`

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          disabled={isLoading || entities.length === 0}
          className={cn("justify-between font-normal min-w-[240px] h-9 px-3 py-2", className)}
        >
          <div className="flex items-center gap-2 truncate w-full">
            <Database size={14} className="text-muted-foreground shrink-0" />
            <span className="truncate flex-1 text-left text-sm">{triggerLabel}</span>
          </div>
        </Button>
      </PopoverTrigger>
      
      <PopoverContent 
        side="bottom" 
        align="start" 
        className="p-0 border shadow-xl w-[500px] max-w-[95vw] overflow-hidden"
      >
        <div className="flex h-[450px] max-h-[70vh]">
          {/* Left: Source List */}
          <div className="w-[180px] shrink-0 border-r bg-muted/20 flex flex-col min-w-0">
            <div className="p-3 border-b shrink-0">
              <span className="text-[10px] font-bold text-muted-foreground uppercase tracking-wider">Sources</span>
            </div>
            <ScrollArea className="flex-1 overflow-hidden">
              <div className="flex flex-col p-1">
                {grouped.map(group => {
                  const isActive = activeSource === group.source
                  const countInSource = group.entities.filter(e => selectedNames.has(e.name)).length
                  return (
                    <button
                      key={group.source}
                      onClick={() => { setActiveSource(group.source); setSearch(""); }}
                      className={cn(
                        "text-left px-3 py-2.5 text-xs rounded-md flex items-center justify-between group transition-colors min-w-0",
                        isActive 
                          ? "bg-accent text-accent-foreground shadow-sm" 
                          : "text-foreground hover:bg-muted"
                      )}
                    >
                      <span className="truncate capitalize mr-2">{group.source}</span>
                      <div className="flex items-center gap-1.5 shrink-0">
                        {countInSource > 0 && (
                          <span className="text-[10px] font-bold text-primary">({countInSource})</span>
                        )}
                        <ChevronRight className={cn("size-3 opacity-20 group-hover:opacity-100 transition-opacity", isActive && "opacity-100")} />
                      </div>
                    </button>
                  )
                })}
              </div>
            </ScrollArea>
          </div>

          {/* Right: Entity List */}
          <div className="flex-1 bg-background flex flex-col min-w-0">
            {!activeSource ? (
              <div className="flex-1 flex flex-col items-center justify-center p-6 text-center gap-2">
                <Database size={24} className="text-muted-foreground/20" />
                <span className="text-xs text-muted-foreground">Select a data source<br/>to browse available entities</span>
              </div>
            ) : (
              <>
                <div className="p-2 border-b flex items-center gap-2 bg-muted/5 shrink-0">
                  <div className="relative flex-1">
                    <Search className="absolute left-2.5 top-2.5 size-3.5 text-muted-foreground/50" />
                    <Input
                      placeholder={`Search in ${activeSource}...`}
                      value={search}
                      onChange={e => setSearch(e.target.value)}
                      className="h-9 pl-8 text-xs border-none bg-transparent shadow-none focus-visible:ring-0"
                    />
                  </div>
                </div>
                <ScrollArea className="flex-1 overflow-hidden">
                  <div className="p-2 flex flex-col gap-1">
                    {filteredEntities.map(entity => {
                      const isSelected = selectedNames.has(entity.name)
                      return (
                        <button
                          key={entity.name}
                          onClick={() => onToggleEntity(entity)}
                          className={cn(
                            "text-left px-2.5 py-2 text-xs rounded-md flex items-center gap-3 transition-colors min-w-0 group",
                            isSelected 
                              ? "bg-accent/50 text-accent-foreground font-medium" 
                              : "text-foreground hover:bg-muted"
                          )}
                        >
                          <div className={cn(
                            "size-4 border rounded flex items-center justify-center shrink-0 transition-all",
                            isSelected ? "bg-primary border-primary scale-110" : "border-muted-foreground/30 bg-background group-hover:border-primary/50"
                          )}>
                            {isSelected && <Check className="size-3 text-primary-foreground stroke-[3]" />}
                          </div>
                          <span className="truncate flex-1" title={entity.alias || entity.name}>
                            {entity.alias || entity.name}
                          </span>
                        </button>
                      )
                    })}
                    {filteredEntities.length === 0 && (
                      <div className="p-12 text-center flex flex-col items-center gap-2">
                        <Search size={20} className="text-muted-foreground/20" />
                        <span className="text-xs text-muted-foreground">No entities match "{search}"</span>
                      </div>
                    )}
                  </div>
                </ScrollArea>
              </>
            )}
          </div>
        </div>
      </PopoverContent>
    </Popover>
  )
}
