'use client';

import { useState, useCallback, useMemo } from 'react';

export interface FilterState {
  parent_orgs: string[];
  states: string[];
  plan_types: string[];
  product_types: string[];
  snp_types: string[];
  group_types: string[];
  year: number | null;
  years: [number, number] | null;
  month: number;
  data_source: 'national' | 'geographic';
}

const DEFAULT_FILTERS: FilterState = {
  parent_orgs: [],
  states: [],
  plan_types: [],
  product_types: [],
  snp_types: [],
  group_types: [],
  year: null,
  years: null,
  month: 1,
  data_source: 'national',
};

export function useFilters(initialFilters: Partial<FilterState> = {}) {
  const [filters, setFilters] = useState<FilterState>({
    ...DEFAULT_FILTERS,
    ...initialFilters,
  });

  const setFilter = useCallback(<K extends keyof FilterState>(
    key: K,
    value: FilterState[K]
  ) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
  }, []);

  const toggleArrayFilter = useCallback(<K extends keyof FilterState>(
    key: K,
    value: string
  ) => {
    setFilters((prev) => {
      const current = prev[key] as string[];
      const updated = current.includes(value)
        ? current.filter((v) => v !== value)
        : [...current, value];
      return { ...prev, [key]: updated };
    });
  }, []);

  const clearFilter = useCallback(<K extends keyof FilterState>(key: K) => {
    setFilters((prev) => ({ ...prev, [key]: DEFAULT_FILTERS[key] }));
  }, []);

  const clearAllFilters = useCallback(() => {
    setFilters(DEFAULT_FILTERS);
  }, []);

  const hasActiveFilters = useMemo(() => {
    return (
      filters.parent_orgs.length > 0 ||
      filters.states.length > 0 ||
      filters.plan_types.length > 0 ||
      filters.product_types.length > 0 ||
      filters.snp_types.length > 0 ||
      filters.group_types.length > 0
    );
  }, [filters]);

  const toQueryParams = useCallback((): Record<string, string | undefined> => {
    const params: Record<string, string | undefined> = {};

    if (filters.parent_orgs.length > 0) {
      params.parent_orgs = filters.parent_orgs.join('|');
    }
    if (filters.states.length > 0) {
      params.states = filters.states.join(',');
    }
    if (filters.plan_types.length > 0) {
      params.plan_types = filters.plan_types.join(',');
    }
    if (filters.product_types.length > 0) {
      params.product_types = filters.product_types.join(',');
    }
    if (filters.snp_types.length > 0) {
      params.snp_types = filters.snp_types.join(',');
    }
    if (filters.group_types.length > 0) {
      params.group_types = filters.group_types.join(',');
    }
    if (filters.year) {
      params.year = String(filters.year);
    }
    if (filters.years) {
      params.start_year = String(filters.years[0]);
      params.end_year = String(filters.years[1]);
    }
    params.month = String(filters.month);
    params.data_source = filters.data_source;

    return params;
  }, [filters]);

  return {
    filters,
    setFilter,
    toggleArrayFilter,
    clearFilter,
    clearAllFilters,
    hasActiveFilters,
    toQueryParams,
  };
}
