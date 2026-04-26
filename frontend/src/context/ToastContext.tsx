/* eslint-disable react-refresh/only-export-components */
import { createContext, useCallback, useContext, useMemo, useRef, useState } from 'react';
import type { ReactNode } from 'react';

export type ToastType = 'error' | 'success' | 'info' | 'warning';

export interface ToastItem {
  id: string;
  message: string;
  type: ToastType;
  duration: number;
}

interface ToastActions {
  error:   (message: string, duration?: number) => void;
  success: (message: string, duration?: number) => void;
  info:    (message: string, duration?: number) => void;
  warning: (message: string, duration?: number) => void;
  dismiss: (id: string) => void;
}

interface ToastContextValue {
  toasts: ToastItem[];
  toast: ToastActions;
}

const ToastContext = createContext<ToastContextValue | undefined>(undefined);

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<ToastItem[]>([]);
  const counter = useRef(0);

  const dismiss = useCallback((id: string) => {
    setToasts(prev => prev.filter(t => t.id !== id));
  }, []);

  const add = useCallback((message: string, type: ToastType, duration = 0) => {
    const id = `toast-${++counter.current}`;
    setToasts(prev => [...prev, { id, message, type, duration }]);
    if (duration > 0) setTimeout(() => dismiss(id), duration);
  }, [dismiss]);

  const toast: ToastActions = useMemo(() => ({
    error:   (msg, dur = 0)    => add(msg, 'error',   dur),
    success: (msg, dur = 5000) => add(msg, 'success', dur),
    info:    (msg, dur = 5000) => add(msg, 'info',    dur),
    warning: (msg, dur = 0)    => add(msg, 'warning', dur),
    dismiss,
  }), [add, dismiss]);

  const value = useMemo(() => ({ toasts, toast }), [toasts, toast]);

  return (
    <ToastContext.Provider value={value}>
      {children}
    </ToastContext.Provider>
  );
}

export function useToast(): ToastContextValue {
  const ctx = useContext(ToastContext);
  if (!ctx) throw new Error('useToast must be used within a ToastProvider');
  return ctx;
}
