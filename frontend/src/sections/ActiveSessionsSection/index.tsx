import { useState, useEffect, useCallback } from 'react';
import {
  Box, Button, Callout, Flex, Heading, Section,
} from '@radix-ui/themes';
import { createColumnHelper } from '@tanstack/react-table';
import { Trash2, RefreshCw, ShieldAlert } from 'lucide-react';
import { useAuth } from '@/auth/AuthContext';
import { DataTable } from '@/components/DataTable';
import { client } from '@/api/openapi/client.gen';

interface SessionRow {
  session_id: number;
  ip_address: string | null;
  user_agent: string | null;
  issued_at: string;
  expires_at: string;
}

const columnHelper = createColumnHelper<SessionRow>();

function buildColumns(onRevoke: (id: number) => void) {
  return [
    columnHelper.accessor('ip_address', {
      header: 'IP Address',
      cell: (info) => info.getValue() ?? '—',
    }),
    columnHelper.accessor('user_agent', {
      header: 'Browser / Device',
      cell: (info) => {
        const ua = info.getValue() ?? '';
        // Trim long UA strings for readability
        return ua.length > 80 ? ua.slice(0, 77) + '…' : ua || '—';
      },
    }),
    columnHelper.accessor('issued_at', {
      header: 'Signed In',
      cell: (info) => {
        try { return new Date(info.getValue()).toLocaleString(); } catch { return info.getValue(); }
      },
    }),
    columnHelper.accessor('expires_at', {
      header: 'Expires',
      cell: (info) => {
        try { return new Date(info.getValue()).toLocaleString(); } catch { return info.getValue(); }
      },
    }),
    columnHelper.display({
      id: 'actions',
      header: '',
      cell: ({ row }) => (
        <Button
          size="1"
          variant="soft"
          color="red"
          style={{ cursor: 'pointer' }}
          onClick={() => onRevoke(row.original.session_id)}
        >
          <Trash2 size={12} /> Revoke
        </Button>
      ),
    }),
  ];
}

export function ActiveSessionsSection() {
  const { isAuthenticated } = useAuth();
  const [sessions, setSessions] = useState<SessionRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchSessions = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const { data } = await client.instance.get<SessionRow[]>('/api/auth/sessions');
      setSessions(data ?? []);
    } catch {
      setError('Failed to load sessions.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (isAuthenticated) fetchSessions();
  }, [isAuthenticated, fetchSessions]);

  const handleRevoke = async (sessionId: number) => {
    try {
      await client.instance.delete(`/api/auth/sessions/${sessionId}`);
      setSessions((prev) => prev.filter((s) => s.session_id !== sessionId));
    } catch {
      setError('Failed to revoke session.');
    }
  };

  if (!isAuthenticated) {
    return (
      <Section>
        <Callout.Root color="amber">
          <Callout.Icon><ShieldAlert size={16} /></Callout.Icon>
          <Callout.Text>Authentication required to view active sessions.</Callout.Text>
        </Callout.Root>
      </Section>
    );
  }

  const columns = buildColumns(handleRevoke);

  return (
    <Section>
      <Flex direction="column" gap="3">
        <Flex align="center" justify="between">
          <Heading size="3">Active Sessions</Heading>
          <Button size="1" variant="ghost" onClick={fetchSessions} style={{ cursor: 'pointer' }}>
            <RefreshCw size={13} /> Refresh
          </Button>
        </Flex>

        {error && (
          <Callout.Root color="red" size="1">
            <Callout.Text>{error}</Callout.Text>
          </Callout.Root>
        )}

        <DataTable
          data={sessions}
          columns={columns}
          isLoading={loading}
          emptyMessage="No active sessions found."
          maxHeight="320px"
        />

        {sessions.length > 0 && (
          <Box>
            <Button
              size="1"
              variant="soft"
              color="red"
              style={{ cursor: 'pointer' }}
              onClick={async () => {
                for (const s of sessions) await handleRevoke(s.session_id);
              }}
            >
              <Trash2 size={12} /> Revoke All Sessions
            </Button>
          </Box>
        )}
      </Flex>
    </Section>
  );
}
