import { AlertCircle, AlertTriangle, CheckCircle2, Info, X } from 'lucide-react';
import { Box, Flex, IconButton, Text } from '@radix-ui/themes';
import { useToast } from '@/context/ToastContext';
import type { ToastItem, ToastType } from '@/context/ToastContext';

const CONFIG: Record<ToastType, { Icon: React.ElementType; color: string; bg: string; border: string }> = {
  error:   { Icon: AlertCircle,   color: 'var(--red-11)',   bg: 'var(--red-2)',   border: 'var(--red-6)'   },
  success: { Icon: CheckCircle2,  color: 'var(--green-11)', bg: 'var(--green-2)', border: 'var(--green-6)' },
  info:    { Icon: Info,          color: 'var(--blue-11)',  bg: 'var(--blue-2)',  border: 'var(--blue-6)'  },
  warning: { Icon: AlertTriangle, color: 'var(--amber-11)', bg: 'var(--amber-2)', border: 'var(--amber-6)' },
};

function ToastItem({ item }: { item: ToastItem }) {
  const { toast } = useToast();
  const { Icon, color, bg, border } = CONFIG[item.type];

  return (
    <Flex
      align="start"
      gap="2"
      style={{
        background: bg,
        border: `1px solid ${border}`,
        borderRadius: 'var(--radius-3)',
        padding: 'var(--space-3)',
        width: 'auto',
        minWidth: 'min(280px, calc(100vw - var(--space-8)))',
        maxWidth: 'min(420px, calc(100vw - var(--space-8)))',
        boxShadow: 'var(--shadow-4)',
        animation: 'toast-slide-in 180ms ease-out',
      }}
    >
      <Box style={{ color, flexShrink: 0, paddingTop: 1 }}>
        <Icon size={16} />
      </Box>
      <Text size="2" style={{ color, flex: 1, lineHeight: 1.5, wordBreak: 'break-word', overflowWrap: 'anywhere' }}>
        {item.message}
      </Text>
      <IconButton
        size="1"
        variant="ghost"
        style={{ color, flexShrink: 0, cursor: 'pointer' }}
        onClick={() => toast.dismiss(item.id)}
      >
        <X size={14} />
      </IconButton>
    </Flex>
  );
}

export function Toaster() {
  const { toasts } = useToast();

  return (
    <>
      <style>{`
        @keyframes toast-slide-in {
          from { opacity: 0; transform: translateX(12px); }
          to   { opacity: 1; transform: translateX(0); }
        }
      `}</style>
      {toasts.length > 0 && (
        <Flex
          direction="column"
          gap="2"
          style={{
            position: 'fixed',
            bottom: 'var(--space-5)',
            right: 'var(--space-5)',
            zIndex: 9999,
          }}
        >
          {toasts.map(t => <ToastItem key={t.id} item={t} />)}
        </Flex>
      )}
    </>
  );
}
