import { useForm } from '@tanstack/react-form';
import { zodValidator } from '@tanstack/zod-form-adapter';
import { useQuery } from '@tanstack/react-query';

export function DynamicCrudForm({ system, tableName }) {
  // 1. Fetch the blueprint
  const { data: entity } = useQuery({
    queryKey: ['metadata', system, tableName],
    queryFn: () => fetchMetadata(system, tableName)
  });

  // 2. Build the dynamic schema
  const dynamicZodSchema = useMemo(() => {
    return entity ? buildDynamicSchema(entity.columns) : z.object({});
  }, [entity]);

  // 3. Initialize TanStack Form
  const form = useForm({
    defaultValues: {},
    validatorAdapter: zodValidator(),
    validators: {
      onChange: dynamicZodSchema,
    },
    onSubmit: async ({ value }) => {
      // 'value' perfectly matches your backend Catalog/Operator structure
      // because the form keys were built from the Entity column names.
      await executeFederatedMutation(value);
    },
  });

  if (!entity) return <div>Loading schema...</div>;

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        e.stopPropagation();
        form.handleSubmit();
      }}
    >
      {/* 4. Dynamically render the form fields */}
      {entity.columns.map((col) => (
        <form.Field
          key={col.name}
          name={col.name}
          children={(field) => (
            <div className="flex flex-col gap-2 mb-4">
              <label className="text-sm font-medium">
                {col.alias || col.name}
              </label>
              
              {/* Inject your Radix UI or native inputs here */}
              <input
                className="border p-2 rounded"
                value={field.state.value || ''}
                onChange={(e) => field.handleChange(e.target.value)}
                disabled={col.is_read_only || col.primary_key}
              />
              
              {field.state.meta.errors ? (
                <span className="text-red-500 text-xs">
                  {field.state.meta.errors}
                </span>
              ) : null}
            </div>
          )}
        />
      ))}
      <button type="submit" className="bg-blue-600 text-white p-2 rounded">
        Save
      </button>
    </form>
  );
}