import { useQuery } from '@tanstack/react-query';
import { Table, Badge, Card, Text, Flex, Spinner, Box } from '@radix-ui/themes';
import { getLeads } from '@/api';

export function LeadsTable() {
  const { data, isLoading, error } = useQuery({
    queryKey: ['leads'],
    queryFn: async () => {
      const { data, error } = await getLeads({});
      if (error) throw error;
      return data; // Strictly typed as Lead[]
    },
  });

  if (isLoading) return <Flex justify="center" p="5"><Spinner size="3" /></Flex>;
  if (error) return <Box p="3"><Text color="red" >Failed to load leads.</Text></Box>;

  return (
    <Card variant="surface">
      <Table.Root variant="ghost">
        <Table.Header>
          <Table.Row>
            <Table.ColumnHeaderCell>Full Name</Table.ColumnHeaderCell>
            <Table.ColumnHeaderCell>Email</Table.ColumnHeaderCell>
            <Table.ColumnHeaderCell>Status</Table.ColumnHeaderCell>
            <Table.ColumnHeaderCell>Added On</Table.ColumnHeaderCell>
          </Table.Row>
        </Table.Header>

        <Table.Body>
          {data?.map((lead) => (
            <Table.Row key={lead.id}>
              <Table.RowHeaderCell>
                {lead.first_name} {lead.last_name}
              </Table.RowHeaderCell>
              <Table.Cell>{lead.email}</Table.Cell>
              <Table.Cell>
                <Badge color={lead.status === 'New' ? 'blue' : 'green'} variant="soft">
                  {lead.status}
                </Badge>
              </Table.Cell>
              <Table.Cell>
                {lead.created_at ? new Date(lead.created_at).toLocaleDateString() : 'N/A'}
              </Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Root>
    </Card>
  );
}