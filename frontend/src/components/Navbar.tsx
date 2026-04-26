import React, { useState } from 'react';
import {
  Box, Button, Container, Dialog, DropdownMenu,
  Flex, Heading, Separator, TabNav, Text, TextField, Avatar,
} from '@radix-ui/themes';
import { useLocation, useNavigate, Link } from 'react-router';
import { useAuth } from '@/auth/AuthContext';
import { useBreakpoint } from '@/context/BreakpointContext';
import { useToast } from '@/context/ToastContext';
import { Database, ArrowRightLeft, Upload, Users, LogOut, User, FlaskConical } from 'lucide-react';

type AuthDialog = 'none' | 'login' | 'register';

const NAV_ITEMS = [
  { path: '/datamart',  label: 'DataMart',  Icon: Database },
  { path: '/migration', label: 'Migration', Icon: ArrowRightLeft },
  { path: '/import',    label: 'Import',    Icon: Upload },
  { path: '/contacts',  label: 'Contacts',  Icon: Users },
  { path: '/prototype', label: 'Prototype', Icon: FlaskConical },
];

export function Navbar() {
  const { user, login, register, logout, isAuthenticated } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const screenSize = useBreakpoint();
  const { toast } = useToast();

  const isSmallScreen = ['xsm', 'sm', 'md'].includes(screenSize);

  const [dialog, setDialog]         = useState<AuthDialog>('none');
  const [submitting, setSubmitting] = useState(false);

  const openDialog = (d: AuthDialog) => { setDialog(d); };
  const closeDialog = () => setDialog('none');

  const handleLogin = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setSubmitting(true);
    const fd = new FormData(e.currentTarget);
    const result = await login({
      username: fd.get('username') as string,
      password: fd.get('password') as string,
    });
    setSubmitting(false);
    if (result.success) {
      toast.success(`Welcome back, ${fd.get('username')}!`);
      closeDialog();
      navigate('/datamart');
    } else {
      // API errors are handled by ApiErrorInterceptor
      // We only toast here if it's a manual/frontend error
      if (result.error && !result.error.toLowerCase().includes('axios')) {
        toast.error(result.error);
      }
    }
  };

  const handleRegister = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setSubmitting(true);
    const fd = new FormData(e.currentTarget);
    const result = await register({
      username: fd.get('username') as string,
      email:    fd.get('email')    as string,
      password: fd.get('password') as string,
    });
    setSubmitting(false);
    if (result.success) {
      toast.success('Account created successfully!');
      closeDialog();
      navigate('/datamart');
    } else {
      if (result.error && !result.error.toLowerCase().includes('axios')) {
        toast.error(result.error);
      }
    }
  };

  return (
    <>
      <Box style={{
        borderBottom: '1px solid var(--gray-a5)',
        background: 'var(--color-background)',
        position: 'sticky',
        top: 0,
        zIndex: 100,
      }}>
        <Container size="4">
          <Flex px="4" py="2" align="center" justify="between">

            {/* Logo + nav tabs */}
            <Flex align="center" gap="4">
              <Link to="/" style={{ textDecoration: 'none', color: 'inherit' }}>
                <Flex align="center" gap="2">
                  <Box style={{
                    width: 32, height: 32, borderRadius: 6,
                    background: 'var(--accent-9)',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                  }}>
                    <Database size={18} color="white" />
                  </Box>
                  <Heading size="4" weight="bold" style={{ letterSpacing: '-0.5px' }}>
                    QBL
                  </Heading>
                </Flex>
              </Link>

              {isSmallScreen ? (
                <NavSelector items={NAV_ITEMS} />
              ) : (
                <TabNav.Root>
                  {NAV_ITEMS.map(({ path, label, Icon }) =>
                    isAuthenticated ? (
                      <TabNav.Link key={path} asChild active={location.pathname === path}>
                        <Link to={path}>
                          <Flex align="center" gap="2">
                            <Icon size={14} />
                            {label}
                          </Flex>
                        </Link>
                      </TabNav.Link>
                    ) : (
                      <TabNav.Link
                        key={path}
                        style={{ opacity: 0.38, cursor: 'not-allowed', pointerEvents: 'none' }}
                      >
                        <Flex align="center" gap="2">
                          <Icon size={14} />
                          {label}
                        </Flex>
                      </TabNav.Link>
                    )
                  )}
                </TabNav.Root>
              )}
            </Flex>

            {/* Right side */}
            <Flex align="center" gap="2">
              {isAuthenticated ? (
                <DropdownMenu.Root>
                  <DropdownMenu.Trigger>
                    <Button variant="ghost" color="gray" size="2"
                      style={{ padding: 0, borderRadius: '50%', cursor: 'pointer' }}>
                      <Avatar
                        size="2"
                        fallback={user?.username?.charAt(0).toUpperCase() ?? 'U'}
                        radius="full"
                      />
                    </Button>
                  </DropdownMenu.Trigger>
                  <DropdownMenu.Content variant="soft" size="2">
                    <Box px="3" py="2">
                      <Text size="2" weight="bold">{user?.username}</Text>
                      <br />
                      <Text size="1" color="gray">{user?.email}</Text>
                    </Box>
                    <DropdownMenu.Separator />
                    <DropdownMenu.Item onClick={() => navigate('/profile')} style={{ cursor: 'pointer' }}>
                      <User size={14} style={{ marginRight: 8 }} /> Profile
                    </DropdownMenu.Item>
                    <DropdownMenu.Separator />
                    <DropdownMenu.Item color="red" onClick={() => { logout(); navigate('/'); }}
                      style={{ cursor: 'pointer' }}>
                      <LogOut size={14} style={{ marginRight: 8 }} /> Logout
                    </DropdownMenu.Item>
                  </DropdownMenu.Content>
                </DropdownMenu.Root>
              ) : (
                <>
                  <Button variant="ghost" size="2" onClick={() => openDialog('login')}
                    style={{ cursor: 'pointer' }}>
                    Sign In
                  </Button>
                  <Button variant="solid" size="2" onClick={() => openDialog('register')}
                    style={{ cursor: 'pointer' }}>
                    Register
                  </Button>
                </>
              )}
            </Flex>

          </Flex>
        </Container>
      </Box>

      {/* Login dialog */}
      <Dialog.Root open={dialog === 'login'} onOpenChange={(o) => !o && closeDialog()}>
        <Dialog.Content maxWidth="380px">
          <Dialog.Title>Sign In</Dialog.Title>
          <Dialog.Description size="2" color="gray" mb="4">
            Enter your credentials to continue.
          </Dialog.Description>
          <form onSubmit={handleLogin}>
            <Flex direction="column" gap="3">
              <TextField.Root name="username" placeholder="Username" required autoComplete="username" />
              <TextField.Root name="password" type="password" placeholder="Password" required
                autoComplete="current-password" />
              <Flex gap="2" justify="end" mt="1">
                <Button variant="ghost" type="button" onClick={closeDialog} style={{ cursor: 'pointer' }}>
                  Cancel
                </Button>
                <Button type="submit" loading={submitting} style={{ cursor: 'pointer' }}>
                  Sign In
                </Button>
              </Flex>
              <Separator size="4" />
              <Button variant="ghost" size="1" type="button" onClick={() => openDialog('register')}
                style={{ cursor: 'pointer' }}>
                Don't have an account? Register
              </Button>
            </Flex>
          </form>
        </Dialog.Content>
      </Dialog.Root>

      {/* Register dialog */}
      <Dialog.Root open={dialog === 'register'} onOpenChange={(o) => !o && closeDialog()}>
        <Dialog.Content maxWidth="380px">
          <Dialog.Title>Create Account</Dialog.Title>
          <Dialog.Description size="2" color="gray" mb="4">
            Create your QuickBitLabs account.
          </Dialog.Description>
          <form onSubmit={handleRegister}>
            <Flex direction="column" gap="3">
              <TextField.Root name="username" placeholder="Username" required autoComplete="username" />
              <TextField.Root name="email" type="email" placeholder="Email" required autoComplete="email" />
              <TextField.Root name="password" type="password" placeholder="Password (min 8 chars)" required
                autoComplete="new-password" />
              <Flex gap="2" justify="end" mt="1">
                <Button variant="ghost" type="button" onClick={closeDialog} style={{ cursor: 'pointer' }}>
                  Cancel
                </Button>
                <Button type="submit" loading={submitting} style={{ cursor: 'pointer' }}>
                  Create Account
                </Button>
              </Flex>
              <Separator size="4" />
              <Button variant="ghost" size="1" type="button" onClick={() => openDialog('login')}
                style={{ cursor: 'pointer' }}>
                Already have an account? Sign In
              </Button>
            </Flex>
          </form>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
}
