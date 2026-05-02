import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { RQBQueryBuilder } from '@/components/RQBQueryBuilder';
import { CrudDataTable } from '@/components/CrudDataTable';
import type { Catalog, QueryResult } from '@/api/sessionApi';
import { getData, getSession } from '@/api/sessionApi';
import { useToast } from '@/context/ToastContext';
import { Separator } from '@/components/ui/separator';
import { Button } from '@/components/ui/button';
import { Loader2, Trash2 } from 'lucide-react';

export function QueryBuilderPage() {
  const { toast } = useToast();
  const [queryCatalog, setQueryCatalog] = useState<Catalog | null>(null);
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [isQuerying, setIsQuerying] = useState(false);

  const { data: catalog, isLoading } = useQuery({
    queryKey: ['session'],
    queryFn: getSession,
  });

  const handleRunQuery = async (finalCatalog: Catalog) => {
    setIsQuerying(true);
    try {
      const result = await getData(finalCatalog);
      setQueryResult(result);
      setQueryCatalog(finalCatalog);
    } catch (err: any) {
      toast.error(err.message || 'Failed to execute query');
    } finally {
      setIsQuerying(false);
    }
  };

  if (isLoading) {
    return (
      <div className="flex h-[50vh] w-full items-center justify-center">
        <div className="flex flex-col items-center gap-2">
          <Loader2 className="size-8 animate-spin text-primary/50" />
          <span className="text-sm text-muted-foreground animate-pulse">Loading Catalog...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto max-w-7xl space-y-8 py-8">
      <div className="space-y-2">
        <h1 className="text-4xl font-bold tracking-tight text-foreground">Query Builder</h1>
        <p className="text-lg text-muted-foreground">
          Build complex cross-system queries using the Universal Pydantic Contract and React Query Builder.
        </p>
      </div>

      <Separator className="bg-border/60" />

      <div className="grid gap-8">
        <RQBQueryBuilder 
          allEntities={catalog?.entities || []}
          onRunQuery={handleRunQuery}
          isPending={isQuerying}
        />

        {queryResult && (
          <div className="flex flex-col gap-4 animate-in fade-in slide-in-from-bottom-4 duration-500">
            <div className="flex items-center justify-between px-2">
              <div className="flex items-center gap-3">
                <span className="text-sm font-semibold text-foreground">
                  Query Results
                </span>
                <span className="text-xs text-muted-foreground bg-muted px-2 py-0.5 rounded-full border">
                  {queryResult.total.toLocaleString()} row{queryResult.total !== 1 ? 's' : ''}
                </span>
              </div>
              <Button 
                variant="ghost" 
                size="sm" 
                onClick={() => setQueryResult(null)}
                className="h-8 text-muted-foreground hover:text-destructive"
              >
                <Trash2 size={14} className="mr-1.5" />
                Clear Results
              </Button>
            </div>
            
            <div className="rounded-xl border bg-card shadow-sm overflow-hidden">
              <CrudDataTable 
                columns={queryResult.columns}
                data={queryResult.rows}
                availableEntities={queryCatalog?.entities || []}
              />
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
