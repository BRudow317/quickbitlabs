import React, { createContext, useState, useContext, useEffect } from 'react';
import { login as loginApi, getUser, register as registerApi, logout as logoutApi, type LoginData, type UserCreate, type UserOut } from '@/api/openapi';
import { client } from '@/api/openapi/client.gen';

type LoginResult = { success: boolean; error?: string };

type AuthContextType = {
  user: UserOut | null;
  role: string;
  isAuthenticated: boolean;
  isAdmin: boolean;
  login: (credentials: LoginData['body']) => Promise<LoginResult>;
  register: (data: UserCreate) => Promise<LoginResult>;
  logout: () => void;
  isLoading: boolean;
};

const AuthContext = createContext<AuthContextType | undefined>(undefined);

const getErrorMessage = (error: unknown, fallback: string): string => {
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return fallback;
};

function decodeJwtRole(token: string): string {
  try {
    const payload = JSON.parse(atob(token.split('.')[1]));
    return typeof payload.role === 'string' ? payload.role : 'user';
  } catch {
    return 'user';
  }
}

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) throw new Error("useAuth must be used within an AuthProvider");
  return context;
};

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [user, setUser] = useState<UserOut | null>(null);
  const [role, setRole] = useState<string>('user');
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const initializeAuth = async () => {
      const savedToken = localStorage.getItem('access_token');
      if (savedToken) {
        client.setConfig({ headers: { Authorization: `Bearer ${savedToken}` } });
        setRole(decodeJwtRole(savedToken));
        try {
          const { data } = await getUser({});
          if (data) setUser(data);
        } catch {
          localStorage.removeItem('access_token');
          setRole('user');
        }
      }
      setIsLoading(false);
    };
    initializeAuth();
  }, []);

  const login = async (credentials: LoginData['body']) => {
    try {
      const { data, error } = await loginApi({ body: credentials });
      if (error || !data) throw new Error("Invalid credentials");

      const token = data.access_token;
      // Refresh token is stored in the HttpOnly cookie set by the server.
      // We only persist the access token in localStorage for the Authorization header.
      localStorage.setItem('access_token', token);
      client.setConfig({ headers: { Authorization: `Bearer ${token}` } });
      setRole(decodeJwtRole(token));

      const profile = await getUser({});
      setUser(profile.data || null);
      return { success: true };
    } catch (error: unknown) {
      return { success: false, error: getErrorMessage(error, 'Login failed') };
    }
  };

  const register = async (data: UserCreate) => {
    try {
      const { error } = await registerApi({ body: data });
      if (error) throw new Error("Registration failed");
      return await login({ username: data.username, password: data.password });
    } catch (error: unknown) {
      return { success: false, error: getErrorMessage(error, 'Registration failed') };
    }
  };

  const logout = () => {
    // Fire server logout to invalidate session + clear the HttpOnly cookie
    logoutApi().catch(() => {});
    localStorage.removeItem('access_token');
    client.setConfig({ headers: { Authorization: undefined } });
    setUser(null);
    setRole('user');
  };

  const isAdmin = role === 'admin';

  return (
    <AuthContext.Provider value={{
      user,
      role,
      isAuthenticated: !!user,
      isAdmin,
      login,
      register,
      logout,
      isLoading,
    }}>
      {children}
    </AuthContext.Provider>
  );
};
