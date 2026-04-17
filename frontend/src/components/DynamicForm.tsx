import { useForm } from '@tanstack/react-form';
import { useQuery } from '@tanstack/react-query';
// Notice: We don't import ANY adapter!

export function DynamicCrudForm({ system, tableName }) {
  const { data: entity } = useQuery({ /*...*/ });
  const dynamicZodSchema = useMemo(() => { /*...*/ }, [entity]);

  const form = useForm({
    defaultValues: {},
    // No validatorAdapter needed!
    validators: {
      // Pass the Zod schema directly into the native validator API
      onChange: dynamicZodSchema,
    },
    onSubmit: async ({ value }) => {
      await executeFederatedMutation(value);
    },
  });

  return (
    <form>
      {/* TanStack Form natively understands the Zod Standard Schema.
        If a field fails validation, it will automatically extract the exact 
        error string from Zod and drop it right into field.state.meta.errors!
      */}
    </form>
  )
}