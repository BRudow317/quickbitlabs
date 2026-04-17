import { z } from 'zod';

// Utility to convert your backend Entity into a Zod schema
function buildDynamicSchema(columns: Column[]) {
  const schemaShape: Record<string, z.ZodTypeAny> = {};

  columns.forEach((col) => {
    let fieldSchema: z.ZodTypeAny = z.string(); // Default fallback

    if (col.raw_type === 'integer' || col.raw_type === 'number') {
      fieldSchema = z.coerce.number();
    } else if (col.raw_type === 'boolean') {
      fieldSchema = z.boolean();
    }
    
    // Apply constraints dynamically
    if (!col.is_nullable) {
      fieldSchema = fieldSchema.min(1, `${col.alias || col.name} is required`);
    } else {
      fieldSchema = fieldSchema.optional().nullable();
    }

    schemaShape[col.name] = fieldSchema;
  });

  return z.object(schemaShape);
}