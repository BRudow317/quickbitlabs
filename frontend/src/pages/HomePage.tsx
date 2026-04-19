import { useState } from 'react';
import { PlusIcon } from '@radix-ui/react-icons';
import {
  Box,
  Button,
  Card,
  Container,
  Flex,
  Heading,
  Section,
  Text,
  TextField,
} from '@radix-ui/themes';
import * as Form from '@radix-ui/react-form';
import { useAuth } from '@/auth/AuthContext';
import { useNavigate } from 'react-router';

export function HomePage() {
  const { login, isAuthenticated, user } = useAuth();
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);

  // If already logged in, show a welcome message or redirect
  if (isAuthenticated && user) {
    return (
      <Container size="1">
        <Section size="3">
          <Card size="3">
            <Flex direction="column" gap="3" align="center">
              <Heading>Welcome back, {user.username}!</Heading>
              <Button onClick={() => navigate('/datamart')}>Go to Query</Button>
            </Flex>
          </Card>
        </Section>
      </Container>
    );
  }

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(null);

    const formData = new FormData(event.currentTarget);
    const username = formData.get('username') as string;
    const password = formData.get('password') as string;

    // Use the strictly typed login from our Context
    const result = await login({ username, password });

    if (result.success) {
      navigate('/datamart');
    } else {
      setError(result.error || 'Login failed');
    }
  };

  return (
    <Box style={{ background: 'var(--gray-a2)', minHeight: '100vh' }}>
      <Container size="1">
        <Section size="3">
          <Flex direction="column" gap="4">
            <Heading align="center">Login</Heading>
            
            <Card size="2">
              <Form.Root onSubmit={handleSubmit}>
                <Flex direction="column" gap="4">
                  
                  {/* Username Field */}
                  <Form.Field name="username">
                    <Flex direction="column" gap="1">
                      <Form.Label>Username</Form.Label>
                      <Form.Control asChild>
                        <TextField.Root placeholder="Enter your username..." required />
                      </Form.Control>
                    </Flex>
                  </Form.Field>

                  {/* Password Field */}
                  <Form.Field name="password">
                    <Flex direction="column" gap="1">
                      <Form.Label>Password</Form.Label>
                      <Form.Control asChild>
                        <TextField.Root type="password" placeholder="Enter your password..." required />
                      </Form.Control>
                    </Flex>
                  </Form.Field>

                  {error && (
                    <Text color="red" size="2">
                      {error}
                    </Text>
                  )}

                  <Form.Submit asChild>
                    <Button size="3" variant="solid">
                      <PlusIcon /> Sign In
                    </Button>
                  </Form.Submit>
                </Flex>
              </Form.Root>
            </Card>
          </Flex>
        </Section>
      </Container>
    </Box>
  );
}