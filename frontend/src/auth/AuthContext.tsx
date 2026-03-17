import React, { createContext, useState, useContext, useEffect } from 'react';
// Use the exact barrel exports we verified
import { login as loginApi, getUser, type LoginData, type UserBase } from '@/api/openapi';
import { client } from '@/api/openapi/client.gen';

type LoginResult = { success: boolean; error?: string };

type AuthContextType = {
  user: UserBase | null; 
  isAuthenticated: boolean;
  login: (credentials: LoginData['body']) => Promise<LoginResult>;
  logout: () => void;
  isLoading: boolean;
};

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) throw new Error("useAuth must be used within an AuthProvider");
  return context;
};

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [user, setUser] = useState<UserBase | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  // 1. Initial Load: Check if we have a valid session
  useEffect(() => {
    const initializeAuth = async () => {
      const savedToken = localStorage.getItem('access_token');
      if (savedToken) {
        // set the client header immediately for the get user call
        client.setConfig({ headers: { Authorization: `Bearer ${savedToken}` } });
        try {
          const { data } = await getUser({});
          if (data) setUser(data);
        } catch (err) {
          localStorage.removeItem('access_token');
        }
      }
      setIsLoading(false);
    };
    initializeAuth();
  }, []);

  const login = async (credentials: LoginData['body']) => {
    try {
      // Use the stable login method
      const { data, error } = await loginApi({ body: credentials });

      if (error || !data) throw new Error("Invalid credentials");

      const token = data.access_token;
      localStorage.setItem('access_token', token);
      
      // Update global client and fetch the full user profile
      client.setConfig({ headers: { Authorization: `Bearer ${token}` } });
      const profile = await getUser({});
      
      setUser(profile.data || null);
      return { success: true };
    } catch (error: any) {
      return { success: false, error: error.message || 'Login failed' };
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
      logout, 
      isAuthenticated: !!user,
      isLoading 
    }}>
      {children}
    </AuthContext.Provider>
  );
};