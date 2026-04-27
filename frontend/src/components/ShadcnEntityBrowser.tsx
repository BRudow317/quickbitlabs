import * as React from "react"
import { Search } from "lucide-react"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import type { Entity } from "@/api/sessionApi"
import { cn } from "@/lib/utils"

interface ShadcnEntityBrowserProps {
  entities: Entity[]
  selectedNames: Set<string>
  onToggleEntity: (entity: Entity) => void
  isLoading?: boolean
  className?: string
}

export function ShadcnEntityBrowser({
  entities,
  selectedNames,
  onToggleEntity,
  isLoading,
  className,
}: ShadcnEntityBrowserProps) {
  const [search, setSearch] = React.useState("")

  const filteredEntities = React.useMemo(
    () =>
      entities.filter(
        (e) =>
          e.name.toLowerCase().includes(search.toLowerCase()) ||
          (e.namespace ?? "").toLowerCase().includes(search.toLowerCase())
      ),
    [entities, search]
  )

  return (
    <Card className={cn("w-full md:w-[280px] shrink-0", className)}>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm font-medium">Available Entities</CardTitle>
        <div className="relative mt-2">
          <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search..."
            className="pl-8"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="space-y-2">
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
          </div>
        ) : entities.length === 0 ? (
          <p className="text-xs text-muted-foreground text-center py-4">
            No metadata cached.
          </p>
        ) : (
          <ScrollArea className="h-[400px] pr-3">
            <div className="space-y-1">
              {filteredEntities.map((entity) => {
                const isSelected = selectedNames.has(entity.name)
                return (
                  <button
                    key={entity.name}
                    onClick={() => onToggleEntity(entity)}
                    className={cn(
                      "w-full text-left px-2 py-1.5 rounded-sm text-sm transition-colors",
                      isSelected
                        ? "bg-primary/10 text-primary font-medium"
                        : "hover:bg-muted text-foreground"
                    )}
                  >
                    <div className="truncate">{entity.name}</div>
                    {entity.namespace && (
                      <div className="text-[10px] text-muted-foreground truncate">
                        {entity.namespace}
                      </div>
                    )}
                  </button>
                )
              })}
            </div>
          </ScrollArea>
        )}
        {entities.length > 0 && (
          <div className="mt-3 text-[10px] text-muted-foreground">
            {filteredEntities.length} of {entities.length} entities
          </div>
        )}
      </CardContent>
    </Card>
  )
}
