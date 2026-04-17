// A dynamic field renderer component
const DynamicField = ({ column, value, onChange }) => {
  // If the backend sniffed predefined values (like SF picklists)
  if (column.enum_values && column.enum_values.length > 0) {
    return <Select options={column.enum_values} value={value} onChange={onChange} />;
  }
  
  // Map based on the raw type or arrow type
  if (column.arrow_type_id?.startsWith("timestamp")) {
    return <DatePicker value={value} onChange={onChange} />;
  }
  
  if (column.raw_type === "boolean") {
    return <Checkbox checked={value} onChange={onChange} />;
  }
  
  // Fallback to text
  return <TextInput value={value} onChange={onChange} maxLength={column.max_length} />;
}