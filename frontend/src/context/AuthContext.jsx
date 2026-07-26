import React, { createContext, useContext, useState, useEffect } from 'react';

const AuthContext = createContext(null);

export const AuthProvider = ({ children }) => {
  const [token, setToken] = useState(() => localStorage.getItem('access_token'));
  const [refreshToken, setRefreshToken] = useState(() => localStorage.getItem('refresh_token'));
  const [user, setUser] = useState(null);

  useEffect(() => {
    if (token) {
      // Basic decode to get user email (if you stored it in token claims, which the backend does)
      try {
        const payload = JSON.parse(atob(token.split('.')[1]));
        setUser({ email: payload.sub });
      } catch (e) {
        console.error("Invalid token format");
        logout();
      }
    } else {
      setUser(null);
    }
  }, [token]);

  const login = (accessToken, refToken) => {
    localStorage.setItem('access_token', accessToken);
    localStorage.setItem('refresh_token', refToken);
    setToken(accessToken);
    setRefreshToken(refToken);
  };

  const logout = () => {
    localStorage.removeItem('access_token');
    localStorage.removeItem('refresh_token');
    setToken(null);
    setRefreshToken(null);
    setUser(null);
  };

  // Wrapper for fetch that automatically attaches the Bearer token
  const authenticatedFetch = async (url, options = {}) => {
    if (!token) {
      throw new Error("No access token available");
    }

    const headers = {
      ...options.headers,
      'Authorization': `Bearer ${token}`
    };

    let response = await fetch(url, { ...options, headers });

    // If unauthorized, attempt to refresh token
    if (response.status === 401 && refreshToken) {
      try {
        const refreshRes = await fetch('/auth/refresh', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ refresh_token: refreshToken })
        });
        
        if (refreshRes.ok) {
          const data = await refreshRes.json();
          login(data.access_token, data.refresh_token);
          
          // Retry the original request with new token
          headers['Authorization'] = `Bearer ${data.access_token}`;
          response = await fetch(url, { ...options, headers });
        } else {
          logout();
          throw new Error("Session expired. Please log in again.");
        }
      } catch (err) {
        logout();
        throw err;
      }
    }

    return response;
  };

  return (
    <AuthContext.Provider value={{ token, user, login, logout, authenticatedFetch }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => useContext(AuthContext);
