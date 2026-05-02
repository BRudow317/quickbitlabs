import { useState, useMemo } from 'react';
import { QueryBuilder } from 'react-querybuilder';
import type { RuleGroupType, Field } from 'react-querybuilder';
import { Play, Eraser, Database, X } from 'lucide-react';
import type { Entity, Catalog, OperatorGroup, Operation, OperatorLiteral } from '@/api/sessionApi';
import { SourceEntitySelector } from './SourceEntitySelector';

import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

// Note: You must run 'npm install react-querybuilder' for this to work.
import 'react-querybuilder/dist/query-builder.css';

interface RQBQueryBuilderProps {
  allEntities: Entity[];
  onRunQuery: (catalog: Catalog) => void;
  isPending: boolean;
}

export function RQBQueryBuilder({ allEntities, onRunQuery, isPending }: RQBQueryBuilderProps) {
  const [selectedEntityNames, setSelectedEntityNames] = useState<string[]>([]);
  const [query, setQuery] = useState<RuleGroupType>({
    combinator: 'and',
    rules: [],
  });

  const selectedEntities = useMemo(() => 
    allEntities.filter(e => selectedEntityNames.includes(e.name)),
  [allEntities, selectedEntityNames]);

  const selectedNamesSet = useMemo(() => new Set(selectedEntityNames), [selectedEntityNames]);

  const fields = useMemo((): Field[] => {
    const f: Field[] = [];
    selectedEntities.forEach(entity => {
      entity.columns?.forEach(col => {
        f.push({
          name: `${entity.name}.${col.name}`,
          label: `${entity.alias || entity.name}.${col.alias || col.name}`,
          placeholder: `Enter ${col.name}`,
          type: 'text',
        });
      });
    });
    return f.sort((a, b) => a.label.localeCompare(b.label));
  }, [selectedEntities]);

  const handleToggleEntity = (entity: Entity) => {
    setSelectedEntityNames(prev => 
      prev.includes(entity.name) ? prev.filter(n => n !== entity.name) : [...prev, entity.name]
    );
  };

  const transformToOperatorGroup = (rqbQuery: RuleGroupType): OperatorGroup | null => {
    if (rqbQuery.rules.length === 0) return null;

    const transformRule = (rule: any): Operation | OperatorGroup | null => {
      if ('rules' in rule) {
        return transformToOperatorGroup(rule as RuleGroupType);
      }
      
      const [eName, cName] = rule.field.split('.');
      const entity = selectedEntities.find(e => e.name === eName);
      const col = entity?.columns?.find(c => c.name === cName);

      if (!col) return null;

      return {
        independent: col,
        operator: rule.operator as OperatorLiteral,
        dependent: rule.value,
      } as Operation;
    };

    return {
      condition: rqbQuery.combinator.toUpperCase() as "AND" | "OR",
      operation_group: rqbQuery.rules.map(transformRule).filter(Boolean) as (Operation | OperatorGroup)[],
    };
  };

  const handleRun = () => {
    const opGroup = transformToOperatorGroup(query);
    const catalog: Catalog = {
      entities: selectedEntities,
      filters: opGroup ? [opGroup] : [],
      limit: 100,
    };
    onRunQuery(catalog);
  };

  return (
    <Card className="w-full">
      <CardHeader className="flex flex-row items-center justify-between pb-4 pt-4 px-6 border-b bg-muted/20">
        <div className="flex items-center gap-2">
          <Database size={18} className="text-primary" />
          <CardTitle className="text-base font-semibold">Advanced Query Builder</CardTitle>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => setQuery({ combinator: 'and', rules: [] })} className="h-8">
            <Eraser size={14} className="mr-1.5" /> Clear Query
          </Button>
          <Button size="sm" onClick={handleRun} disabled={isPending || selectedEntities.length === 0} className="h-8">
            <Play size={14} className="mr-1.5" /> {isPending ? 'Running...' : 'Run Query'}
          </Button>
        </div>
      </CardHeader>

      <CardContent className="p-6 flex flex-col gap-6">
        <div className="p-4 rounded-xl border bg-muted/30 flex flex-col gap-4">
          <div className="flex items-center justify-between">
            <span className="text-xs font-bold text-muted-foreground uppercase tracking-wider">Source Entities</span>
            <span className="text-[10px] text-muted-foreground font-medium bg-background px-2 py-0.5 rounded-full border shadow-sm">
              {selectedEntities.length} selected
            </span>
          </div>
          
          <SourceEntitySelector 
            entities={allEntities}
            selectedNames={selectedNamesSet}
            onToggleEntity={handleToggleEntity}
            className="w-full sm:max-w-md"
          />

          {selectedEntities.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {selectedEntities.map(entity => (
                <Badge 
                  key={entity.name} 
                  variant="secondary"
                  className="pl-2.5 pr-1 py-1 flex items-center gap-1.5 text-[11px] rounded-lg border-primary/20 bg-primary/5 hover:bg-primary/10 transition-colors"
                >
                  {entity.alias || entity.name}
                  <button 
                    onClick={(e) => { e.stopPropagation(); handleToggleEntity(entity); }}
                    className="p-0.5 rounded-md hover:bg-primary/20 transition-colors"
                  >
                    <X size={12} className="text-primary/70" />
                  </button>
                </Badge>
              ))}
            </div>
          )}
        </div>

        {selectedEntities.length > 0 ? (
          <div className="rqb-container rounded-xl border p-4 bg-muted/10 shadow-inner">
            <QueryBuilder 
              fields={fields} 
              query={query} 
              onQueryChange={q => setQuery(q)} 
            />
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center p-12 rounded-xl border border-dashed bg-muted/5">
            <Database size={32} className="text-muted-foreground/30 mb-3" />
            <p className="text-sm text-muted-foreground">Select entities to build your query.</p>
          </div>
        )}
      </CardContent>
      
      <style dangerouslySetInnerHTML={{ __html: `
        .rqb-container .queryBuilder {
          padding: 0;
          font-family: inherit;
          background: transparent;
          color: var(--foreground);
        }
        .rqb-container .ruleGroup {
          background: var(--muted);
          border: 1px solid var(--border);
          border-radius: var(--radius-md);
          color: var(--foreground);
          padding: 1rem;
        }
        .rqb-container .rule {
          background: var(--background);
          border: 1px solid var(--border);
          border-radius: var(--radius-sm);
          color: var(--foreground);
          padding: 0.75rem;
          box-shadow: 0 1px 2px rgba(0,0,0,0.05);
        }
        .rqb-container select, .rqb-container input {
          background: var(--background);
          color: var(--foreground) !important;
          border: 1px solid var(--border);
          border-radius: var(--radius-sm);
          padding: 4px 8px;
          font-size: 13px;
          transition: border-color 0.1s;
        }
        .rqb-container select:focus, .rqb-container input:focus {
          border-color: var(--primary);
          outline: none;
        }
        .rqb-container .ruleGroup-header, .rqb-container .rule-header {
           margin-bottom: 0.75rem;
           display: flex;
           gap: 8px;
           align-items: center;
        }
        .rqb-container button {
          cursor: pointer;
          font-size: 12px;
          padding: 4px 10px;
          border-radius: var(--radius-sm);
          border: 1px solid var(--border);
          background: var(--secondary);
          color: var(--secondary-foreground);
          font-weight: 500;
          transition: all 0.1s;
        }
        .rqb-container button:hover {
          background: var(--accent);
          color: var(--accent-foreground);
          border-color: var(--accent);
        }
        .rqb-container .queryBuilder-dragHandle {
          color: var(--muted-foreground);
          opacity: 0.5;
        }
        .rqb-container .ruleGroup-combinators select {
          color: var(--foreground) !important;
          font-weight: bold;
          text-transform: uppercase;
          letter-spacing: 0.025em;
        }
      `}} />
    </Card>
  );
}
