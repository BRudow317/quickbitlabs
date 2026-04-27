/**
 * TanStack Table v8 Implementation References:
 * 1. Shadcn/ui Data Table (Radix + Tailwind): https://github.com/shadcn-ui/ui/tree/main/apps/www/app/(app)/examples/tasks
 * 2. Official Basic Reference (Headless): https://github.com/TanStack/table/tree/main/examples/react/basic
 * 3. Roman86 V8 Production Demo (Unified State): https://github.com/Roman86/tanstack-table
 */

import { useMemo } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
  type ColumnDef,
} from '@tanstack/react-table';
import { Table, Box, Text, Flex } from '@radix-ui/themes';

interface DataTableProps<T extends object> {
  data: T[];
  columns?: ColumnDef<T, any>[];
  isLoading?: boolean;
  emptyMessage?: string;
  maxHeight?: string;
}

export function DataTable<T extends object>({
  data,
  columns: providedColumns,
  isLoading,
  emptyMessage = "No results found",
  maxHeight = "600px",
}: DataTableProps<T>) {
  
  // 1. Auto-generate columns if not provided
  const columns = useMemo(() => {
    if (providedColumns) return providedColumns;
    if (data.length === 0) return [];

    const helper = createColumnHelper<T>();
    const firstRow = data[0];
    
    return Object.keys(firstRow).map((key) => 
      helper.accessor(key as any, {
        header: key.charAt(0).toUpperCase() + key.slice(1).replace(/_/g, ' '),
        cell: (info) => {
          const value = info.getValue();
          if (value instanceof Date) return value.toLocaleDateString();
          if (typeof value === 'object' && value !== null) return JSON.stringify(value);
          return String(value ?? '');
        },
      })
    ) as ColumnDef<T, any>[];
  }, [data, providedColumns]);

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  if (isLoading) {
    return (
      <Flex align="center" justify="center" p="8" style={{ background: 'var(--gray-2)', borderRadius: 'var(--radius-3)' }}>
        <Spinner size="3" />
        <Text size="2" color="gray" ml="3">Loading data...</Text>
      </Flex>
    );
  }

  if (data.length === 0) {
    return (
      <Flex align="center" justify="center" p="8" style={{ background: 'var(--gray-2)', borderRadius: 'var(--radius-3)' }}>
        <Text size="2" color="gray">{emptyMessage}</Text>
      </Flex>
    );
  }

  return (
    <Box 
      className="qbl-table-container"
      style={{ 
        position: 'relative',
        border: '1px solid var(--gray-6)', 
        borderRadius: 'var(--radius-3)', 
        overflow: 'auto',
        maxHeight: maxHeight,
        background: 'var(--color-background)'
      }}
    >
      <style>{`
        /* 1. HIGH VISIBILITY SCROLLBARS */
        .qbl-table-container::-webkit-scrollbar {
          width: 14px;
          height: 14px;
        }
        .qbl-table-container::-webkit-scrollbar-track {
          background: var(--gray-3);
          border-radius: 0 0 var(--radius-3) 0;
        }
        .qbl-table-container::-webkit-scrollbar-thumb {
          background: var(--gray-8);
          border: 3px solid var(--gray-3);
          border-radius: 10px;
        }
        .qbl-table-container::-webkit-scrollbar-thumb:hover {
          background: var(--gray-9);
        }
        .qbl-table-container::-webkit-scrollbar-corner {
          background: var(--gray-3);
        }

        /* 2. STICKY HEADER & BORDERS */
        .qbl-table-container table {
          width: 100%;
          border-collapse: separate;
          border-spacing: 0;
        }
        
        .qbl-table-container thead {
          position: sticky;
          top: 0;
          z-index: 2;
        }

        .qbl-table-container thead th {
          background: var(--gray-2);
          /* Subtle shadow to act as bottom border that stays with header */
          box-shadow: inset 0 -1px 0 var(--gray-6);
          padding: var(--space-2) var(--space-3) !important;
          text-align: left;
        }

        .qbl-table-container .rt-TableCell, 
        .qbl-table-container .rt-TableColumnHeaderCell {
          border-right: 1px solid var(--gray-5);
          border-bottom: 1px solid var(--gray-4);
          padding: var(--space-2) var(--space-3) !important;
          white-space: nowrap;
        }

        /* Remove double borders on the last column */
        .qbl-table-container .rt-TableCell:last-child,
        .qbl-table-container .rt-TableColumnHeaderCell:last-child {
          border-right: none;
        }

        /* Hover effect for rows */
        .qbl-table-container tbody tr:hover {
          background: var(--accent-a2);
        }
      `}</style>
      
      <Table.Root variant="surface">
        <Table.Header>
          {table.getHeaderGroups().map((headerGroup) => (
            <Table.Row key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <Table.ColumnHeaderCell key={header.id}>
                  <Text size="2" weight="bold" color="gray">
                    {header.isPlaceholder
                      ? null
                      : flexRender(
                          header.column.columnDef.header,
                          header.getContext()
                        )}
                  </Text>
                </Table.ColumnHeaderCell>
              ))}
            </Table.Row>
          ))}
        </Table.Header>

        <Table.Body>
          {table.getRowModel().rows.map((row) => (
            <Table.Row key={row.id}>
              {row.getVisibleCells().map((cell) => (
                <Table.Cell key={cell.id}>
                  <Text size="2">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </Text>
                </Table.Cell>
              ))}
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Root>
    </Box>
  );
}

// Helper component for loading state
function Spinner({ size, ...props }: { size?: "1" | "2" | "3" } & any) {
  return (
    <Box 
      style={{ 
        width: size === "3" ? 24 : 16, 
        height: size === "3" ? 24 : 16, 
        border: '2px solid var(--gray-5)', 
        borderTopColor: 'var(--accent-9)', 
        borderRadius: '50%',
        animation: 'spin 0.6s linear infinite'
      }} 
      {...props}
    >
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </Box>
  );
}
