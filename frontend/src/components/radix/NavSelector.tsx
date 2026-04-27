import { Button, DropdownMenu, Flex, Text } from '@radix-ui/themes';
import { useLocation, useNavigate } from 'react-router';
import { useAuth } from '@/auth/AuthContext';
import { ChevronDown, type LucideIcon } from 'lucide-react';

interface NavItem {
  path: string;
  label: string;
  Icon: LucideIcon;
}

interface NavSelectorProps {
  items: NavItem[];
}

export function NavSelector({ items }: NavSelectorProps) {
  const { isAuthenticated } = useAuth();
  const location = useLocation();
  const navigate = useNavigate();

  const activeItem = items.find(item => item.path === location.pathname);

  return (
    <DropdownMenu.Root>
      <DropdownMenu.Trigger>
        <Button variant="ghost" color="gray" size="2" style={{ cursor: 'pointer' }}>
          <Flex align="center" gap="2">
            {activeItem ? (
              <>
                <activeItem.Icon size={16} />
                <Text size="2" weight="bold">{activeItem.label}</Text>
              </>
            ) : (
              <Text size="2" weight="bold">Menu</Text>
            )}
            <ChevronDown size={14} />
          </Flex>
        </Button>
      </DropdownMenu.Trigger>
      <DropdownMenu.Content variant="soft" size="2" align="start">
        {items.map(({ path, label, Icon }) => (
          <DropdownMenu.Item
            key={path}
            disabled={!isAuthenticated}
            onClick={() => isAuthenticated && navigate(path)}
            style={{ 
              cursor: isAuthenticated ? 'pointer' : 'not-allowed',
              opacity: isAuthenticated ? 1 : 0.5 
            }}
            color={location.pathname === path ? 'indigo' : undefined}
          >
            <Flex align="center" gap="2">
              <Icon size={14} />
              {label}
            </Flex>
          </DropdownMenu.Item>
        ))}
      </DropdownMenu.Content>
    </DropdownMenu.Root>
  );
}
