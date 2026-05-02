import * as React from "react"
import { ChevronsUpDown } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import type { Entity } from "@/api/sessionApi"
import { cn } from "@/lib/utils"

const UNGROUPED = "(No Namespace)"

interface GroupedEntityDropdownProps {
  entities: Entity[]
  selectedNames: Set<string>
  onToggleEntity: (entity: Entity) => void
  isLoading?: boolean
  className?: string
}

export function GroupedEntityDropdown({
  entities,
  selectedNames,
  onToggleEntity,
  isLoading,
  className,
}: GroupedEntityDropdownProps) {
  const [open, setOpen] = React.useState(false)

  const grouped = React.useMemo(() => {
    const map = new Map<string, Entity[]>()
    for (const entity of entities) {
      const key = entity.plugin || entity.namespace || UNGROUPED
      if (!map.has(key)) map.set(key, [])
      map.get(key)!.push(entity)
    }
    // Sort groups alphabetically by key
    const sortedGroups = Array.from(map.entries()).sort((a, b) => a[0].localeCompare(b[0]))
    // Sort entities within each group alphabetically by name
    for (const [_, nsEntities] of sortedGroups) {
      nsEntities.sort((a, b) => a.name.localeCompare(b.name))
    }
    return sortedGroups
  }, [entities])

  const selectedCount = selectedNames.size
  const triggerLabel =
    selectedCount === 0
      ? "Select entities..."
      : `${selectedCount} entity${selectedCount !== 1 ? "s" : ""} selected`

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          disabled={isLoading || entities.length === 0}
          className={cn("w-full justify-between font-normal", className)}
        >
          <span className="truncate text-left">{triggerLabel}</span>
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent
        className="p-0"
        style={{ width: "var(--radix-popover-trigger-width)" }}
        align="start"
      >
        <Command>
          <CommandInput placeholder="Search entities..." />
          <CommandList>
            <CommandEmpty>No entities found.</CommandEmpty>
            {grouped.map(([namespace, nsEntities], idx) => (
              <React.Fragment key={namespace}>
                {idx > 0 && <CommandSeparator />}
                <CommandGroup heading={namespace}>
                  {nsEntities.map((entity) => {
                    const isSelected = selectedNames.has(entity.name)
                    return (
                      <CommandItem
                        key={entity.name}
                        value={`${namespace}/${entity.name}`}
                        onSelect={() => onToggleEntity(entity)}
                        className="cursor-pointer"
                      >
                        <div className="flex items-center gap-2 flex-1 min-w-0">
                          <div 
                            style={{ 
                              width: '8px', 
                              height: '8px', 
                              borderRadius: '50%', 
                              flexShrink: 0,
                              background: isSelected ? 'var(--primary)' : 'transparent',
                              border: isSelected ? 'none' : '1px solid var(--muted-foreground)'
                            }} 
                          />
                          <span 
                            className="truncate" 
                            style={{ 
                              color: isSelected ? 'var(--foreground)' : 'var(--muted-foreground)',
                              fontWeight: isSelected ? 600 : 400
                            }}
                          >
                            {entity.alias || entity.name}
                          </span>
                        </div>
                      </CommandItem>
                    )
                  })}
                </CommandGroup>
              </React.Fragment>
            ))}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}
