'use client';

import { useState, useCallback, useEffect } from 'react';
import type { AuditMetadata } from '@/components/audit';

interface QueryState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  audit: AuditMetadata | null;
}

interface UseQueryOptions {
  immediate?: boolean;
  onSuccess?: (data: unknown) => void;
  onError?: (error: string) => void;
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export function useQuery<T>(
  endpoint: string,
  options: UseQueryOptions = {}
) {
  const { immediate = true, onSuccess, onError } = options;

  const [state, setState] = useState<QueryState<T>>({
    data: null,
    loading: immediate,
    error: null,
    audit: null,
  });

  const execute = useCallback(async (params?: Record<string, unknown>) => {
    setState((prev) => ({ ...prev, loading: true, error: null }));

    try {
      const url = new URL(`${API_BASE}${endpoint}`);
      
      if (params) {
        Object.entries(params).forEach(([key, value]) => {
          if (value !== undefined && value !== null) {
            if (Array.isArray(value)) {
              url.searchParams.set(key, value.join(','));
            } else {
              url.searchParams.set(key, String(value));
            }
          }
        });
      }

      const response = await fetch(url.toString(), {
        headers: { 'Content-Type': 'application/json' },
      });

      if (!response.ok) {
        throw new Error(`API error: ${response.status}`);
      }

      const result = await response.json();

      const audit: AuditMetadata | null = result._audit || result.audit || null;

      setState({
        data: result as T,
        loading: false,
        error: null,
        audit,
      });

      onSuccess?.(result);
      return result as T;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Unknown error';
      setState((prev) => ({
        ...prev,
        loading: false,
        error: errorMsg,
      }));
      onError?.(errorMsg);
      throw err;
    }
  }, [endpoint, onSuccess, onError]);

  useEffect(() => {
    if (immediate) {
      execute();
    }
  }, [immediate, execute]);

  return {
    ...state,
    execute,
    refetch: execute,
  };
}

export function useLazyQuery<T>(endpoint: string, options: UseQueryOptions = {}) {
  return useQuery<T>(endpoint, { ...options, immediate: false });
}
