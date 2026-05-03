import { useState } from 'react';
import {
  Box, Button, Callout, Card, Flex,
  Heading, Separator, Text, TextField,
} from '@radix-ui/themes';
import { useAuth } from '@/auth/AuthContext';
import { client } from '@/api/openapi/client.gen';
import { CheckCircle } from 'lucide-react';
import { ActiveSessionsSection } from '@/sections/ActiveSessionSection';
import { ShadcnThemeSection } from '@/sections/ShadcnThemeSection';

const getErrorMessage = (err: unknown): string => {
  if (
    typeof err === 'object' &&
    err !== null &&
    'response' in err &&
    typeof (err as { response?: unknown }).response === 'object' &&
    (err as { response?: unknown }).response !== null
  ) {
    const response = (err as { response: { data?: { detail?: unknown } } }).response;
    const detail = response.data?.detail;
    if (typeof detail === 'string' && detail.length > 0) {
      return detail;
    }
  }

  return 'Failed to update profile';
};

export function ProfilePage() {
  const { user } = useAuth();
  const [email, setEmail]     = useState(user?.email ?? '');
  const [saving, setSaving]   = useState(false);
  const [saved, setSaved]     = useState(false);
  const [error, setError]     = useState<string | null>(null);

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    setError(null);
    setSaved(false);
    try {
      await client.instance.patch('/api/users/', { email });
      setSaved(true);
    } catch (err: unknown) {
      setError(getErrorMessage(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Box style={{ maxWidth: 720 }}>
      <Flex direction="column" gap="5">
        <Heading size="5">Profile</Heading>

        {/* Read-only info */}
        <Card size="3">
          <Flex direction="column" gap="4">
            <Flex direction="column" gap="1">
              <Text size="1" color="gray" weight="bold">USERNAME</Text>
              <Text size="3">{user?.username}</Text>
            </Flex>
          </Flex>
        </Card>

        {/* Editable fields */}
        <Card size="3">
          <form onSubmit={handleSave}>
            <Flex direction="column" gap="4">
              <Heading size="3">Edit Information</Heading>

              <Flex direction="column" gap="1">
                <Text size="2" weight="bold">Email</Text>
                <TextField.Root
                  type="email"
                  value={email}
                  onChange={(e) => { setEmail(e.target.value); setSaved(false); }}
                  required
                  autoComplete="email"
                />
              </Flex>

              {saved && (
                <Callout.Root color="green" size="1">
                  <Callout.Icon><CheckCircle size={14} /></Callout.Icon>
                  <Callout.Text>Profile updated.</Callout.Text>
                </Callout.Root>
              )}
              {error && <Text size="2" color="red">{error}</Text>}

              <Button type="submit" loading={saving} style={{ cursor: 'pointer' }}>
                Save Changes
              </Button>
            </Flex>
          </form>
        </Card>

        <ShadcnThemeSection />

        <Separator size="4" />

        <ActiveSessionsSection />
      </Flex>
    </Box>
  );
}
