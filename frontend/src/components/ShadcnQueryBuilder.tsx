import { X, Play, Eraser, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import type { Entity } from "@/api/sessionApi"
import { cn } from "@/lib/utils"

interface ShadcnQueryBuilderProps {
  selectedEntities: Entity[]
  onToggleEntity: (entity: Entity) => void
  onClear: () => void
  limit: string
  onLimitChange: (value: string) => void
  onRunQuery: () => void
  isPending: boolean
  className?: string
}

export function ShadcnQueryBuilder({
  selectedEntities,
  onToggleEntity,
  onClear,
  limit,
  onLimitChange,
  onRunQuery,
  isPending,
  className,
}: ShadcnQueryBuilderProps) {
  return (
    <Card className={cn("w-full", className)}>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm font-medium">Query Selection</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap gap-2 min-h-10 items-center p-2 rounded-md border bg-muted/20">
          {selectedEntities.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              Select entities from the browser to begin.
            </p>
          ) : (
            selectedEntities.map((e) => (
              <Badge
                key={e.name}
                variant="secondary"
                className="flex items-center gap-1 pr-1 py-1"
              >
                {e.name}
                <button
                  onClick={(e_event) => {
                    e_event.stopPropagation()
                    onToggleEntity(e)
                  }}
                  className="rounded-full hover:bg-muted p-0.5"
                >
                  <X className="h-3 w-3" />
                </button>
              </Badge>
            ))
          )}
        </div>

        <div className="flex items-end gap-4">
          <div className="grid w-24 items-center gap-1.5">
            <Label htmlFor="limit" className="text-xs">Limit</Label>
            <Input
              id="limit"
              type="number"
              value={limit}
              onChange={(e) => onLimitChange(e.target.value)}
              className="h-9"
            />
          </div>

          <div className="flex gap-2">
            <Button
              disabled={selectedEntities.length === 0 || isPending}
              onClick={onRunQuery}
              className="h-9"
            >
              {isPending ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Querying
                </>
              ) : (
                <>
                  <Play className="mr-2 h-4 w-4" />
                  Run Query
                </>
              )}
            </Button>

            {selectedEntities.length > 0 && (
              <Button
                variant="outline"
                onClick={onClear}
                className="h-9"
              >
                <Eraser className="mr-2 h-4 w-4" />
                Clear
              </Button>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
