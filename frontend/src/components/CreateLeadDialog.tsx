import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Dialog, Button, Flex, Text, TextField, Box } from '@radix-ui/themes';
import { PlusIcon } from '@radix-ui/react-icons';
import { createLead, type LeadCreate } from '@/api';

export function CreateLeadDialog() {
  const [open, setOpen] = useState(false);
  const queryClient = useQueryClient();

  // Mutation handles the 'POST' logic strictly using LeadCreate
  const { mutate, isPending } = useMutation({
    mutationFn: async (newLead: LeadCreate) => {
      const { data, error } = await createLead({ body: newLead });
      if (error) throw error;
      return data;
    },
    onSuccess: () => {
      // Tells TanStack Query: "The leads list is old, go get the new data!"
      queryClient.invalidateQueries({ queryKey: ['leads'] });
      setOpen(false);
    },
  });

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const formData = new FormData(event.currentTarget);
    
    // Strictly typed payload based on your LeadCreate model
    const payload: LeadCreate = {
      first_name: formData.get('first_name') as string,
      last_name: formData.get('last_name') as string,
      email: formData.get('email') as string,
      status: 'New',
    };

    mutate(payload);
  };

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Trigger>
        <Button variant="solid">
          <PlusIcon /> Add Lead
        </Button>
      </Dialog.Trigger>

      <Dialog.Content style={{ maxWidth: 450 }}>
        <Dialog.Title>New Sales Lead</Dialog.Title>
        <Dialog.Description size="2" mb="4">
          Enter the contact details for the new prospect.
        </Dialog.Description>

        <form onSubmit={handleSubmit}>
          <Flex direction="column" gap="3">
            <Box>
              <Text as="div" size="2" mb="1" weight="bold">First Name</Text>
              <TextField.Root name="first_name" placeholder="Jane" required />
            </Box>
            <Box>
              <Text as="div" size="2" mb="1" weight="bold">Last Name</Text>
              <TextField.Root name="last_name" placeholder="Doe" required />
            </Box>
            <Box>
              <Text as="div" size="2" mb="1" weight="bold">Email</Text>
              <TextField.Root name="email" type="email" placeholder="jane@example.com" required />
            </Box>
          </Flex>

          <Flex gap="3" mt="4" justify="end">
            <Dialog.Close>
              <Button variant="soft" color="gray">Cancel</Button>
            </Dialog.Close>
            <Button type="submit" loading={isPending}>Save Lead</Button>
          </Flex>
        </form>
      </Dialog.Content>
    </Dialog.Root>
  );
}