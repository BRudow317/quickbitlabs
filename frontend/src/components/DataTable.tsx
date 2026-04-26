import { useMemo } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
  type ColumnDef,
} from '@tanstack/react-table';
import { Table, ScrollArea, Box, Text, Flex } from '@radix-ui/themes';

interface DataTableProps<T extends object> {
  data: T[];
  columns?: ColumnDef<T, any>[];
  isLoading?: boolean;
  emptyMessage?: string;
}

export function DataTable<T extends object>({
  data,
  columns: providedColumns,
  isLoading,
  emptyMessage = "No results found",
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
        <Text size="2" color="gray">Loading data...</Text>
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
    <Box style={{ border: '1px solid var(--gray-4)', borderRadius: 'var(--radius-3)', overflow: 'hidden' }}>
      <ScrollArea scrollbars="both" style={{ maxHeight: '600px' }}>
        <Table.Root variant="surface">
          <Table.Header>
            {table.getHeaderGroups().map((headerGroup) => (
              <Table.Row key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <Table.ColumnHeaderCell key={header.id}>
                    {header.isPlaceholder
                      ? null
                      : flexRender(
                          header.column.columnDef.header,
                          header.getContext()
                        )}
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
      </ScrollArea>
    </Box>
  );
}
