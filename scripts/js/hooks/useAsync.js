import { useState, useEffect, useCallback } from 'react';

/**
 * useAsync - Handle async operations with loading/error states
 * @param {Function} asyncFn - Async function to execute
 * @param {boolean} immediate - Run immediately on mount
 * @returns {Object} - { data, loading, error, execute }
 * 
 * @example
 * const { data, loading, error, execute } = useAsync(
 *   () => fetch('/api/users').then(r => r.json()),
 *   true // run immediately
 * );
 * 
 * if (loading) return <Spinner />;
 * if (error) return <Error message={error.message} />;
 * return <UserList users={data} />;
 */
export const useAsync = (asyncFn, immediate = true) => {
  const [state, setState] = useState({
    data: null,
    loading: immediate,
    error: null,
  });

  const execute = useCallback(async (...args) => {
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      const data = await asyncFn(...args);
      setState({ data, loading: false, error: null });
      return data;
    } catch (error) {
      setState((s) => ({ ...s, loading: false, error }));
      throw error;
    }
  }, [asyncFn]);

  useEffect(() => {
    if (immediate) execute();
  }, []);

  return { ...state, execute };
};
