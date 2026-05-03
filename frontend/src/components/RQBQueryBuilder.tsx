import { useState, useMemo } from 'react';
import { QueryBuilder, ValueEditor } from 'react-querybuilder';
import type { RuleGroupType, Field, ValueEditorProps, NameLabelPair } from 'react-querybuilder';
import { Play, Eraser, Database, X, Plus, Link2, ArrowUpDown, Pencil, Filter } from 'lucide-react';
import { cn } from '@/lib/utils';
import type {
  Entity, Catalog, OperatorGroup, Operation, OperatorLiteral,
  Join, Sort, Assignment,
} from '@/api/sessionApi';
import { SourceEntitySelector } from './SourceEntitySelector';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';

import 'react-querybuilder/dist/query-builder.css';

// -- Operators — match backend OperatorLiteral exactly -------------------------

const OPERATORS: NameLabelPair[] = [
  { name: '=',           label: 'equals' },
  { name: '!=',          label: 'not equals' },
  { name: '>',           label: '>' },
  { name: '<',           label: '<' },
  { name: '>=',          label: '>=' },
  { name: '<=',          label: '<=' },
  { name: 'LIKE',        label: 'contains' },
  { name: 'NOT LIKE',    label: 'not contains' },
  { name: 'IN',          label: 'in (a, b, c)' },
  { name: 'NOT IN',      label: 'not in' },
  { name: 'BETWEEN',     label: 'between' },
  { name: 'NOT BETWEEN', label: 'not between' },
  { name: 'IS NULL',     label: 'is null' },
  { name: 'IS NOT NULL', label: 'is not null' },
];

const NO_VALUE_OPS = new Set(['IS NULL', 'IS NOT NULL']);
const BETWEEN_OPS  = new Set(['BETWEEN', 'NOT BETWEEN']);
const LIST_OPS     = new Set(['IN', 'NOT IN']);

// -- Custom value editors -------------------------------------------------------

function BetweenValueEditor({ value, handleOnChange }: ValueEditorProps) {
  let lo = '', hi = '';
  try {
    const parsed = JSON.parse(value as string);
    if (Array.isArray(parsed)) { lo = String(parsed[0] ?? ''); hi = String(parsed[1] ?? ''); }
  } catch { lo = String(value ?? ''); }

  return (
    <span className="inline-flex items-center gap-1.5">
      <input className="rqb-between-input" value={lo} placeholder="from"
        onChange={e => handleOnChange(JSON.stringify([e.target.value, hi]))} />
      <span style={{ fontSize: '12px', color: 'var(--muted-foreground)' }}>to</span>
      <input className="rqb-between-input" value={hi} placeholder="to"
        onChange={e => handleOnChange(JSON.stringify([lo, e.target.value]))} />
    </span>
  );
}

function CustomValueEditor(props: ValueEditorProps) {
  if (NO_VALUE_OPS.has(props.operator)) return null;
  if (BETWEEN_OPS.has(props.operator)) return <BetweenValueEditor {...props} />;
  return <ValueEditor {...props} />;
}

// -- Serialization helpers ------------------------------------------------------

function toDependent(operator: string, raw: unknown): string | unknown[] | null {
  if (NO_VALUE_OPS.has(operator)) return null;
  if (BETWEEN_OPS.has(operator)) {
    try { const p = JSON.parse(raw as string); if (Array.isArray(p)) return p; } catch { /* */ }
    const parts = String(raw ?? '').split(',').map(s => s.trim());
    return [parts[0] ?? '', parts[1] ?? ''];
  }
  if (LIST_OPS.has(operator)) return String(raw ?? '').split(',').map(s => s.trim()).filter(Boolean);
  return (raw as string) ?? '';
}

function ruleGroupToOperatorGroup(group: RuleGroupType, entities: Entity[]): OperatorGroup | null {
  if (!group.rules.length) return null;

  // RQB represents NOT as { not: true, combinator: 'and', rules: [...] }
  if (group.not) {
    const inner = ruleGroupToOperatorGroup({ ...group, not: false }, entities);
    return inner ? { condition: 'NOT', operation_group: [inner] } : null;
  }

  const items: (Operation | OperatorGroup)[] = [];
  for (const rule of group.rules) {
    if ('rules' in rule) {
      const sub = ruleGroupToOperatorGroup(rule as RuleGroupType, entities);
      if (sub) items.push(sub);
    } else {
      const [eName, cName] = (rule.field as string).split('.');
      const col = entities.find(e => e.name === eName)?.columns?.find(c => c.name === cName);
      if (!col) continue;
      items.push({
        independent: col,
        operator: rule.operator as OperatorLiteral,
        dependent: toDependent(rule.operator as string, rule.value),
      });
    }
  }

  if (!items.length) return null;
  return {
    condition: (group.combinator.toUpperCase() === 'OR' ? 'OR' : 'AND') as 'AND' | 'OR',
    operation_group: items,
  };
}

// -- UI state types -------------------------------------------------------------

function uid() { return Math.random().toString(36).slice(2); }

interface UIJoin {
  id: string;
  leftEntity: string;
  leftCol: string;
  joinType: 'INNER' | 'LEFT' | 'OUTER';
  rightEntity: string;
  rightCol: string;
}

interface UISort {
  id: string;
  entity: string;
  col: string;
  direction: 'ASC' | 'DESC';
  nullsFirst: boolean | null;
}

interface UIAssignment {
  id: string;
  entity: string;
  col: string;
  value: string;
}

interface ColEntry { key: string; label: string; entity: string; col: string; }

function allColumnsFor(entities: Entity[]): ColEntry[] {
  return entities
    .flatMap(e => (e.columns ?? []).map(c => ({
      key: `${e.name}.${c.name}`,
      label: `${e.alias ?? e.name}.${c.alias ?? c.name}`,
      entity: e.name,
      col: c.name,
    })))
    .sort((a, b) => a.label.localeCompare(b.label));
}

function toJoins(uiJoins: UIJoin[], entities: Entity[]): Join[] {
  return uiJoins.flatMap(j => {
    const le = entities.find(e => e.name === j.leftEntity);
    const re = entities.find(e => e.name === j.rightEntity);
    const lc = le?.columns?.find(c => c.name === j.leftCol);
    const rc = re?.columns?.find(c => c.name === j.rightCol);
    if (!le || !re || !lc || !rc) return [];
    return [{ left_entity: le, left_column: lc, join_type: j.joinType, right_entity: re, right_column: rc }];
  });
}

function toSorts(uiSorts: UISort[], entities: Entity[]): Sort[] {
  return uiSorts.flatMap(s => {
    const col = entities.find(e => e.name === s.entity)?.columns?.find(c => c.name === s.col);
    return col ? [{ column: col, direction: s.direction, nulls_first: s.nullsFirst }] : [];
  });
}

function toAssignments(uiAssignments: UIAssignment[], entities: Entity[]): Assignment[] {
  return uiAssignments.flatMap(a => {
    const col = entities.find(e => e.name === a.entity)?.columns?.find(c => c.name === a.col);
    return col ? [{ column: col, value: a.value }] : [];
  });
}

// -- Section header -------------------------------------------------------------

function SectionHeader({ icon, title, action }: { icon: React.ReactNode; title: string; action?: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between mb-3">
      <div className="flex items-center gap-2">
        {icon}
        <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">{title}</span>
      </div>
      {action}
    </div>
  );
}

// -- JoinRow --------------------------------------------------------------------

function JoinRow({ join, entities, onChange, onRemove }: {
  join: UIJoin;
  entities: Entity[];
  onChange: (j: UIJoin) => void;
  onRemove: () => void;
}) {
  const leftCols  = entities.find(e => e.name === join.leftEntity)?.columns ?? [];
  const rightCols = entities.find(e => e.name === join.rightEntity)?.columns ?? [];

  return (
    <div className="flex flex-wrap items-center gap-2">
      <Select value={join.leftEntity} onValueChange={v => onChange({ ...join, leftEntity: v, leftCol: '' })}>
        <SelectTrigger className="h-8 w-36 text-xs"><SelectValue placeholder="entity" /></SelectTrigger>
        <SelectContent>
          {entities.map(e => <SelectItem key={e.name} value={e.name} className="text-xs">{e.alias ?? e.name}</SelectItem>)}
        </SelectContent>
      </Select>

      <Select value={join.leftCol} onValueChange={v => onChange({ ...join, leftCol: v })}>
        <SelectTrigger className="h-8 w-32 text-xs"><SelectValue placeholder="column" /></SelectTrigger>
        <SelectContent>
          {leftCols.map(c => <SelectItem key={c.name} value={c.name} className="text-xs">{c.alias ?? c.name}</SelectItem>)}
        </SelectContent>
      </Select>

      <Select value={join.joinType} onValueChange={v => onChange({ ...join, joinType: v as UIJoin['joinType'] })}>
        <SelectTrigger className="h-8 w-28 text-xs"><SelectValue /></SelectTrigger>
        <SelectContent>
          <SelectItem value="INNER" className="text-xs">INNER JOIN</SelectItem>
          <SelectItem value="LEFT"  className="text-xs">LEFT JOIN</SelectItem>
          <SelectItem value="OUTER" className="text-xs">OUTER JOIN</SelectItem>
        </SelectContent>
      </Select>

      <Select value={join.rightEntity} onValueChange={v => onChange({ ...join, rightEntity: v, rightCol: '' })}>
        <SelectTrigger className="h-8 w-36 text-xs"><SelectValue placeholder="entity" /></SelectTrigger>
        <SelectContent>
          {entities.map(e => <SelectItem key={e.name} value={e.name} className="text-xs">{e.alias ?? e.name}</SelectItem>)}
        </SelectContent>
      </Select>

      <Select value={join.rightCol} onValueChange={v => onChange({ ...join, rightCol: v })}>
        <SelectTrigger className="h-8 w-32 text-xs"><SelectValue placeholder="column" /></SelectTrigger>
        <SelectContent>
          {rightCols.map(c => <SelectItem key={c.name} value={c.name} className="text-xs">{c.alias ?? c.name}</SelectItem>)}
        </SelectContent>
      </Select>

      <Button variant="ghost" size="icon" className="h-8 w-8 shrink-0 text-muted-foreground hover:text-destructive" onClick={onRemove}>
        <X className="h-3.5 w-3.5" />
      </Button>
    </div>
  );
}

// -- SortRow --------------------------------------------------------------------

function SortRow({ sort, columns, onChange, onRemove }: {
  sort: UISort;
  columns: ColEntry[];
  onChange: (s: UISort) => void;
  onRemove: () => void;
}) {
  const key = sort.entity && sort.col ? `${sort.entity}.${sort.col}` : '';

  return (
    <div className="flex flex-wrap items-center gap-2">
      <Select value={key} onValueChange={v => {
        const c = columns.find(c => c.key === v);
        if (c) onChange({ ...sort, entity: c.entity, col: c.col });
      }}>
        <SelectTrigger className="h-8 w-48 text-xs"><SelectValue placeholder="column" /></SelectTrigger>
        <SelectContent>
          {columns.map(c => <SelectItem key={c.key} value={c.key} className="text-xs">{c.label}</SelectItem>)}
        </SelectContent>
      </Select>

      <div className="flex rounded-md border overflow-hidden">
        {(['ASC', 'DESC'] as const).map(d => (
          <button key={d} onClick={() => onChange({ ...sort, direction: d })}
            className={cn(
              'px-2.5 py-1 text-xs font-medium transition-colors',
              sort.direction === d
                ? 'bg-primary text-primary-foreground'
                : 'bg-background text-muted-foreground hover:bg-muted',
            )}>
            {d}
          </button>
        ))}
      </div>

      <Select
        value={sort.nullsFirst === null ? 'auto' : sort.nullsFirst ? 'first' : 'last'}
        onValueChange={v => onChange({ ...sort, nullsFirst: v === 'auto' ? null : v === 'first' })}
      >
        <SelectTrigger className="h-8 w-28 text-xs"><SelectValue /></SelectTrigger>
        <SelectContent>
          <SelectItem value="auto"  className="text-xs">Nulls auto</SelectItem>
          <SelectItem value="first" className="text-xs">Nulls first</SelectItem>
          <SelectItem value="last"  className="text-xs">Nulls last</SelectItem>
        </SelectContent>
      </Select>

      <Button variant="ghost" size="icon" className="h-8 w-8 shrink-0 text-muted-foreground hover:text-destructive" onClick={onRemove}>
        <X className="h-3.5 w-3.5" />
      </Button>
    </div>
  );
}

// -- AssignmentRow --------------------------------------------------------------

function AssignmentRow({ assignment, columns, onChange, onRemove }: {
  assignment: UIAssignment;
  columns: ColEntry[];
  onChange: (a: UIAssignment) => void;
  onRemove: () => void;
}) {
  const key = assignment.entity && assignment.col ? `${assignment.entity}.${assignment.col}` : '';

  return (
    <div className="flex flex-wrap items-center gap-2">
      <Select value={key} onValueChange={v => {
        const c = columns.find(c => c.key === v);
        if (c) onChange({ ...assignment, entity: c.entity, col: c.col });
      }}>
        <SelectTrigger className="h-8 w-48 text-xs"><SelectValue placeholder="column" /></SelectTrigger>
        <SelectContent>
          {columns.map(c => <SelectItem key={c.key} value={c.key} className="text-xs">{c.label}</SelectItem>)}
        </SelectContent>
      </Select>

      <span className="text-xs font-mono text-muted-foreground">=</span>

      <Input
        value={assignment.value}
        onChange={e => onChange({ ...assignment, value: e.target.value })}
        className="h-8 w-44 text-xs"
        placeholder="value"
      />

      <Button variant="ghost" size="icon" className="h-8 w-8 shrink-0 text-muted-foreground hover:text-destructive" onClick={onRemove}>
        <X className="h-3.5 w-3.5" />
      </Button>
    </div>
  );
}

// -- Main component -------------------------------------------------------------

interface RQBQueryBuilderProps {
  allEntities: Entity[];
  onRunQuery: (catalog: Catalog) => void;
  isPending: boolean;
}

export function RQBQueryBuilder({ allEntities, onRunQuery, isPending }: RQBQueryBuilderProps) {
  const [selectedEntityNames, setSelectedEntityNames] = useState<string[]>([]);
  const [query,         setQuery]         = useState<RuleGroupType>({ combinator: 'and', rules: [] });
  const [uiJoins,       setUiJoins]       = useState<UIJoin[]>([]);
  const [uiSorts,       setUiSorts]       = useState<UISort[]>([]);
  const [uiAssignments, setUiAssignments] = useState<UIAssignment[]>([]);
  const [limit,  setLimit]  = useState('100');
  const [offset, setOffset] = useState('');

  const selectedEntities = useMemo(() =>
    allEntities.filter(e => selectedEntityNames.includes(e.name)),
    [allEntities, selectedEntityNames]);

  const selectedNamesSet = useMemo(() => new Set(selectedEntityNames), [selectedEntityNames]);

  const fields = useMemo((): Field[] =>
    selectedEntities
      .flatMap(entity => (entity.columns ?? []).map(col => ({
        name:  `${entity.name}.${col.name}`,
        label: `${entity.alias ?? entity.name}.${col.alias ?? col.name}`,
        placeholder: `Enter ${col.name}`,
      })))
      .sort((a, b) => a.label.localeCompare(b.label)),
    [selectedEntities]);

  const allColumns = useMemo(() => allColumnsFor(selectedEntities), [selectedEntities]);

  const handleToggleEntity = (entity: Entity) =>
    setSelectedEntityNames(prev =>
      prev.includes(entity.name) ? prev.filter(n => n !== entity.name) : [...prev, entity.name]
    );

  const handleClear = () => {
    setQuery({ combinator: 'and', rules: [] });
    setUiJoins([]);
    setUiSorts([]);
    setUiAssignments([]);
    setLimit('100');
    setOffset('');
  };

  const handleRun = () => {
    const opGroup = ruleGroupToOperatorGroup(query, selectedEntities);
    const catalog: Catalog = {
      entities:     selectedEntities,
      joins:        toJoins(uiJoins, selectedEntities),
      filters:      opGroup ? [opGroup] : [],
      assignments:  toAssignments(uiAssignments, selectedEntities),
      sort_columns: toSorts(uiSorts, selectedEntities),
      limit:        limit  ? parseInt(limit,  10) : null,
      offset:       offset ? parseInt(offset, 10) : null,
    };
    onRunQuery(catalog);
  };

  const addJoin = () => setUiJoins(prev => [...prev, {
    id: uid(),
    leftEntity:  selectedEntities[0]?.name ?? '',
    leftCol:     '',
    joinType:    'INNER',
    rightEntity: selectedEntities[1]?.name ?? '',
    rightCol:    '',
  }]);

  const addSort = () => setUiSorts(prev => [...prev, {
    id: uid(),
    entity:     allColumns[0]?.entity ?? '',
    col:        allColumns[0]?.col ?? '',
    direction:  'ASC',
    nullsFirst: null,
  }]);

  const addAssignment = () => setUiAssignments(prev => [...prev, {
    id: uid(),
    entity: allColumns[0]?.entity ?? '',
    col:    allColumns[0]?.col ?? '',
    value:  '',
  }]);

  return (
    <Card className="w-full">
      <CardHeader className="flex flex-row items-center justify-between pb-4 pt-4 px-6 border-b bg-muted/20">
        <div className="flex items-center gap-2">
          <Database size={18} className="text-primary" />
          <CardTitle className="text-base font-semibold">Advanced Query Builder</CardTitle>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={handleClear} className="h-8">
            <Eraser size={14} className="mr-1.5" /> Clear
          </Button>
          <Button size="sm" onClick={handleRun} disabled={isPending || selectedEntities.length === 0} className="h-8">
            <Play size={14} className="mr-1.5" /> {isPending ? 'Running...' : 'Run Query'}
          </Button>
        </div>
      </CardHeader>

      <CardContent className="p-6 space-y-5">

        {/* Entities */}
        <div className="p-4 rounded-xl border bg-muted/30 space-y-3">
          <SectionHeader
            icon={<Database size={14} className="text-muted-foreground" />}
            title="Source Entities"
            action={
              <span className="text-[10px] text-muted-foreground bg-background px-2 py-0.5 rounded-full border">
                {selectedEntities.length} selected
              </span>
            }
          />
          <SourceEntitySelector
            entities={allEntities}
            selectedNames={selectedNamesSet}
            onToggleEntity={handleToggleEntity}
            className="w-full sm:max-w-md"
          />
          {selectedEntities.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {selectedEntities.map(entity => (
                <Badge key={entity.name} variant="secondary"
                  className="pl-2.5 pr-1 py-1 flex items-center gap-1.5 text-[11px] rounded-lg border-primary/20 bg-primary/5 hover:bg-primary/10 transition-colors">
                  {entity.alias ?? entity.name}
                  <button onClick={e => { e.stopPropagation(); handleToggleEntity(entity); }}
                    className="p-0.5 rounded-md hover:bg-primary/20 transition-colors">
                    <X size={12} className="text-primary/70" />
                  </button>
                </Badge>
              ))}
            </div>
          )}
        </div>

        {selectedEntities.length === 0 ? (
          <div className="flex flex-col items-center justify-center p-12 rounded-xl border border-dashed bg-muted/5">
            <Database size={32} className="text-muted-foreground/30 mb-3" />
            <p className="text-sm text-muted-foreground">Select entities to build your query.</p>
          </div>
        ) : (
          <>
            {/* Joins — only shown when 2+ entities selected */}
            {selectedEntities.length > 1 && (
              <div className="p-4 rounded-xl border bg-muted/30 space-y-3">
                <SectionHeader
                  icon={<Link2 size={14} className="text-muted-foreground" />}
                  title="Joins"
                  action={
                    <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={addJoin}>
                      <Plus size={12} className="mr-1" /> Add Join
                    </Button>
                  }
                />
                {uiJoins.length === 0 ? (
                  <p className="text-xs text-muted-foreground">No joins configured. Without joins, each entity is queried independently.</p>
                ) : (
                  <div className="space-y-2">
                    {uiJoins.map((j, i) => (
                      <JoinRow key={j.id} join={j} entities={selectedEntities}
                        onChange={updated => setUiJoins(prev => prev.map((x, xi) => xi === i ? updated : x))}
                        onRemove={() => setUiJoins(prev => prev.filter((_, xi) => xi !== i))} />
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* Filters (RQB) */}
            <div className="p-4 rounded-xl border bg-muted/30 space-y-3">
              <SectionHeader
                icon={<Filter size={14} className="text-muted-foreground" />}
                title="Filters (WHERE)"
              />
              <div className="rqb-container rounded-lg border p-3 bg-muted/10">
                <QueryBuilder
                  fields={fields}
                  operators={OPERATORS}
                  query={query}
                  onQueryChange={q => setQuery(q)}
                  showNotToggle
                  controlElements={{ valueEditor: CustomValueEditor }}
                />
              </div>
            </div>

            {/* Sort */}
            <div className="p-4 rounded-xl border bg-muted/30 space-y-3">
              <SectionHeader
                icon={<ArrowUpDown size={14} className="text-muted-foreground" />}
                title="Sort (ORDER BY)"
                action={
                  <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={addSort}>
                    <Plus size={12} className="mr-1" /> Add Sort
                  </Button>
                }
              />
              {uiSorts.length === 0 ? (
                <p className="text-xs text-muted-foreground">No sort applied. Default order will be used.</p>
              ) : (
                <div className="space-y-2">
                  {uiSorts.map((s, i) => (
                    <SortRow key={s.id} sort={s} columns={allColumns}
                      onChange={updated => setUiSorts(prev => prev.map((x, xi) => xi === i ? updated : x))}
                      onRemove={() => setUiSorts(prev => prev.filter((_, xi) => xi !== i))} />
                  ))}
                </div>
              )}
            </div>

            {/* Limit / Offset */}
            <div className="p-4 rounded-xl border bg-muted/30">
              <SectionHeader
                icon={<span className="text-xs font-mono text-muted-foreground leading-none">#</span>}
                title="Limit / Offset"
              />
              <div className="flex items-center gap-6">
                <div className="flex items-center gap-2">
                  <span className="text-xs text-muted-foreground">Limit</span>
                  <Input value={limit} onChange={e => setLimit(e.target.value)}
                    className="h-8 w-24 text-xs" type="number" min="1" placeholder="100" />
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-xs text-muted-foreground">Offset</span>
                  <Input value={offset} onChange={e => setOffset(e.target.value)}
                    className="h-8 w-24 text-xs" type="number" min="0" placeholder="0" />
                </div>
              </div>
            </div>

            {/* Assignments (SET) */}
            <div className="p-4 rounded-xl border bg-muted/30 space-y-3">
              <SectionHeader
                icon={<Pencil size={14} className="text-muted-foreground" />}
                title="Assignments (SET)"
                action={
                  <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={addAssignment}>
                    <Plus size={12} className="mr-1" /> Add Assignment
                  </Button>
                }
              />
              {uiAssignments.length === 0 ? (
                <p className="text-xs text-muted-foreground">No assignments. Used for UPDATE / UPSERT write operations.</p>
              ) : (
                <div className="space-y-2">
                  {uiAssignments.map((a, i) => (
                    <AssignmentRow key={a.id} assignment={a} columns={allColumns}
                      onChange={updated => setUiAssignments(prev => prev.map((x, xi) => xi === i ? updated : x))}
                      onRemove={() => setUiAssignments(prev => prev.filter((_, xi) => xi !== i))} />
                  ))}
                </div>
              )}
            </div>
          </>
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
        .rqb-container .ruleGroup-combinators select {
          color: var(--foreground) !important;
          font-weight: bold;
          text-transform: uppercase;
          letter-spacing: 0.025em;
        }
        .rqb-between-input {
          background: var(--background);
          color: var(--foreground);
          border: 1px solid var(--border);
          border-radius: var(--radius-sm);
          padding: 4px 8px;
          font-size: 13px;
          width: 80px;
          transition: border-color 0.1s;
        }
        .rqb-between-input:focus {
          border-color: var(--primary);
          outline: none;
        }
      `}} />
    </Card>
  );
}
