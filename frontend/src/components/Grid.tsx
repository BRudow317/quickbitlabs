// React dynamically builds the grid columns
const tableColumns = useMemo(() => {
  if (!entityMetadata) return [];
  
  return entityMetadata.columns.map((col) => ({
    header: col.alias || col.name,
    accessorKey: col.name,
    // You can use your metadata to dynamically format the cells!
    cell: (info) => {
      const val = info.getValue();
      if (col.arrow_type_id?.startsWith("decimal")) return formatCurrency(val);
      if (col.arrow_type_id?.startsWith("timestamp")) return formatDate(val);
      return val;
    },
    // Make it editable unless the backend explicitly said no
    meta: {
      isEditable: !col.is_read_only && !col.primary_key
    }
  }));
}, [entityMetadata]);