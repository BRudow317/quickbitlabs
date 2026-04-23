import React, { createContext, useState, useContext, useEffect } from 'react';
import { login as loginApi, getUser, register as registerApi, type LoginData, type UserCreate, type UserOut } from '@/api/openapi';
import { client } from '@/api/openapi/client.gen';

type LoginResult = { success: boolean; error?: string };

type AuthContextType = {
  user: UserOut | null;
  isAuthenticated: boolean;
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

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) throw new Error("useAuth must be used within an AuthProvider");
  return context;
};

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [user, setUser] = useState<UserOut | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const initializeAuth = async () => {
      const savedToken = localStorage.getItem('access_token');
      if (savedToken) {
        client.setConfig({ headers: { Authorization: `Bearer ${savedToken}` } });
        try {
          const { data } = await getUser({});
          if (data) setUser(data);
        } catch {
          localStorage.removeItem('access_token');
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
      localStorage.setItem('access_token', token);
      client.setConfig({ headers: { Authorization: `Bearer ${token}` } });

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
    localStorage.removeItem('access_token');
    client.setConfig({ headers: { Authorization: undefined } });
    setUser(null);
  };

  return (
    <AuthContext.Provider value={{
      user,
      login,
      register,
      logout,
      isAuthenticated: !!user,
      isLoading,
    }}>
      {children}
    </AuthContext.Provider>
  );
};
