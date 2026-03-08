"use client";

import React, { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import { Filter, Plus, X, ChevronDown, Info, Download } from "lucide-react";
import { AuditButton, type AuditMetadata } from "@/components/audit";

// Hook to check if component is mounted (fixes SSR hydration issues with charts)
function useIsMounted() {
  const [mounted, setMounted] = useState(false);
  useEffect(() => {
    setMounted(true);
  }, []);
  return mounted;
}

const API_BASE = (process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000").replace(/\/$/, "");

const COLORS = [
  "#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c",
  "#0891b2", "#4f46e5", "#c026d3", "#059669", "#d97706",
];

function formatNumber(num: number | undefined | null): string {
  if (num === undefined || num === null || isNaN(num)) return "0";
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(0)}K`;
  return num.toString();
}

function formatFullNumber(num: number | undefined | null): string {
  if (num === undefined || num === null || isNaN(num)) return "0";
  return Math.round(num).toLocaleString();
}

function formatPercent(num: number | undefined | null): string {
  if (num === undefined || num === null || isNaN(num)) return "0%";
  return `${num.toFixed(1)}%`;
}

interface FilterOptions {
  years: number[];
  plan_types: string[];
  plan_types_simplified: string[];
  product_types: string[];
  snp_types: string[];
  states: string[];
  parent_orgs: string[];
  contracts: string[];
}

interface TimeseriesData {
  years: number[];
  total_enrollment?: number[];
  series?: Record<string, number[]>;
  group_by?: string;
  audit_id?: string;
  filters?: Record<string, unknown>;
}

export default function EnrollmentPage() {
  const isMounted = useIsMounted();
  const [selectedPlanTypes, setSelectedPlanTypes] = useState<string[]>([]);
  const [selectedProductTypes, setSelectedProductTypes] = useState<string[]>(['MA']); // Default to MA (includes MA-only + MAPD)
  const [selectedSnpTypes, setSelectedSnpTypes] = useState<string[]>([]);
  const [selectedGroupTypes, setSelectedGroupTypes] = useState<string[]>([]); // Individual vs Group
  const [selectedStates, setSelectedStates] = useState<string[]>([]);
  const [selectedCounties, setSelectedCounties] = useState<string[]>([]);
  const [availableCounties, setAvailableCounties] = useState<{state: string, county: string}[]>([]);
  const [selectedParentOrgs, setSelectedParentOrgs] = useState<string[]>([]);
  const [showIndustryTotal, setShowIndustryTotal] = useState(true); // Show industry total line
  const [groupBy, setGroupBy] = useState<string | null>(null);
  const [yearRange, setYearRange] = useState<number | null>(null); // null = all years
  const [dataSource, setDataSource] = useState<"national" | "geographic">("national"); // national = exact totals, geographic = has state/county but suppressed

  // Popup states
  const [showFilterPopup, setShowFilterPopup] = useState(false);
  const [showGeoPopup, setShowGeoPopup] = useState(false);
  const [showPayerPopup, setShowPayerPopup] = useState(false);
  const [payerSearch, setPayerSearch] = useState("");
  const [stateSearch, setStateSearch] = useState("");
  
  // Cell detail modal
  const [cellDetail, setCellDetail] = useState<{
    payer: string;
    year: number;
    value: number;
  } | null>(null);

  // Fetch filter options using v5 (Gold layer) - no fallback
  const { data: filterOptions } = useQuery<FilterOptions>({
    queryKey: ["enrollment-filters-v5"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/v5/filters`);
      const data = await res.json();
      return {
        ...data,
        plan_types_simplified: data.plan_types || [],
        contracts: [],
      };
    },
  });

  // Fetch counties when states change - using v5
  useEffect(() => {
    if (selectedStates.length > 0) {
      fetch(`${API_BASE}/api/v5/counties?states=${selectedStates.join(',')}`)
        .then(res => res.json())
        .then(data => setAvailableCounties(data.counties || []))
        .catch(() => setAvailableCounties([]));
    } else {
      setAvailableCounties([]);
      setSelectedCounties([]);
    }
  }, [selectedStates]);

  // Build query params
  const buildQueryParams = () => {
    const params = new URLSearchParams();
    if (selectedPlanTypes.length > 0) params.set("plan_types", selectedPlanTypes.join(","));
    if (selectedProductTypes.length > 0) {
      // Both sources now use same table (fact_enrollment_unified) with product_type = 'MAPD' or 'PDP'
      // Map UI "MA" -> API "MAPD"
      const apiProductTypes = selectedProductTypes.map(t => t === "MA" ? "MAPD" : t);
      params.set("product_types", apiProductTypes.join(","));
    }
    if (selectedSnpTypes.length > 0) params.set("snp_types", selectedSnpTypes.join(","));
    if (selectedGroupTypes.length > 0) params.set("group_types", selectedGroupTypes.join(","));
    // Only include geography filters when using geographic data source
    if (dataSource === "geographic") {
      if (selectedStates.length > 0) params.set("states", selectedStates.join(","));
      if (selectedCounties.length > 0) params.set("counties", selectedCounties.join("|"));
    }
    if (selectedParentOrgs.length > 0) params.set("parent_orgs", selectedParentOrgs.join("|"));
    if (groupBy) params.set("group_by", groupBy);
    params.set("data_source", dataSource);
    params.set("include_total", "true");
    return params.toString();
  };

  // Fetch timeseries data using v5 (Gold layer) - no fallback
  // Supports multiple payers by making parallel requests
  const { data: rawTimeseriesData, isLoading } = useQuery<TimeseriesData>({
    queryKey: ["enrollment-timeseries-v5", selectedPlanTypes, selectedProductTypes, selectedSnpTypes, selectedGroupTypes, selectedStates, selectedCounties, selectedParentOrgs, showIndustryTotal, groupBy],
    queryFn: async () => {
      // Build base params (without parent_org)
      const buildBaseParams = () => {
        const params = new URLSearchParams();
        if (selectedPlanTypes.length > 0) params.set("plan_types", selectedPlanTypes.join(","));
        if (selectedProductTypes.length > 0) {
          const apiProductTypes = selectedProductTypes.map(t => t === "MA" ? "MAPD" : t);
          params.set("product_types", apiProductTypes.join(","));
        }
        if (selectedSnpTypes.length > 0) params.set("snp_types", selectedSnpTypes.join(","));
        if (selectedGroupTypes.length > 0) params.set("group_types", selectedGroupTypes.join(","));
        if (selectedStates.length > 0) params.set("states", selectedStates.join(","));
        if (selectedCounties.length > 0) params.set("counties", selectedCounties.join(","));
        params.set("start_year", "2013");
        params.set("end_year", "2026");
        return params;
      };

      const series: Record<string, number[]> = {};
      let years: number[] = [];
      let auditId = "";

      // Fetch industry total if showing it or no payers selected
      if (showIndustryTotal || selectedParentOrgs.length === 0) {
        const params = buildBaseParams();
        const res = await fetch(`${API_BASE}/api/v5/enrollment/timeseries?${params.toString()}`);
        const data = await res.json();
        years = data.years || [];
        series["Total"] = data.enrollment || [];
        auditId = data.audit_id || "";
      }

      // Fetch each selected payer in parallel
      if (selectedParentOrgs.length > 0) {
        const payerPromises = selectedParentOrgs.map(async (org) => {
          const params = buildBaseParams();
          params.set("parent_org", org);
          const res = await fetch(`${API_BASE}/api/v5/enrollment/timeseries?${params.toString()}`);
          const data = await res.json();
          return { org, data };
        });

        const payerResults = await Promise.all(payerPromises);
        
        for (const { org, data } of payerResults) {
          if (data.years && data.enrollment) {
            if (years.length === 0) years = data.years;
            series[org] = data.enrollment;
          }
        }
      }

      return {
        years,
        series,
        total_enrollment: series["Total"] || [],
        audit_id: auditId,
        filters: {},
      };
    },
  });

  // Filter out Industry Total if not showing it (client-side filtering)
  const timeseriesData = rawTimeseriesData ? {
    ...rawTimeseriesData,
    series: rawTimeseriesData.series
      ? Object.fromEntries(
          Object.entries(rawTimeseriesData.series).filter(([key]) =>
            showIndustryTotal || (key !== 'Industry Total' && key !== 'Total')
          )
        )
      : undefined
  } : undefined;

  // Use unified timeseries data (includes SNP filtering)
  const activeData = timeseriesData;
  const activeLoading = isLoading;

  // Transform data for chart
  const allChartData = activeData?.years?.map((year, i) => {
    const point: Record<string, any> = { year };
    if (activeData.series) {
      Object.entries(activeData.series).forEach(([key, values]) => {
        point[key] = values[i];
      });
    } else if (activeData.total_enrollment) {
      point["Total"] = activeData.total_enrollment[i];
    }
    return point;
  }) || [];

  // Filter by year range
  const chartData = yearRange
    ? allChartData.slice(-yearRange)
    : allChartData;

  const seriesKeys = activeData?.series
    ? Object.keys(activeData.series).filter(k => {
        const vals = activeData.series![k];
        return vals.some(v => v > 0);
      })
    : activeData?.total_enrollment ? ["Total"] : [];

  const latestTotal = activeData?.total_enrollment
    ? activeData.total_enrollment[activeData.total_enrollment.length - 1]
    : activeData?.series
      ? Object.values(activeData.series).reduce((sum, arr) => sum + (arr[arr.length - 1] || 0), 0)
      : 0;

  // Calculate max value for Y-axis scaling
  const maxDataValue = (() => {
    if (!chartData || chartData.length === 0) return 1000000;
    let max = 0;
    chartData.forEach(point => {
      seriesKeys.forEach(key => {
        const val = point[key];
        if (typeof val === 'number' && val > max) max = val;
      });
    });
    return max || 1000000;
  })();

  const hasTypeFilters = selectedPlanTypes.length > 0 || selectedProductTypes.length > 0 || selectedSnpTypes.length > 0 || selectedGroupTypes.length > 0;
  const hasGeoFilters = selectedStates.length > 0 || selectedCounties.length > 0;
  const typeFilterCount = selectedPlanTypes.length + selectedProductTypes.length + selectedSnpTypes.length + selectedGroupTypes.length;
  const geoFilterCount = selectedStates.length + selectedCounties.length;

  const clearTypeFilters = () => {
    setSelectedPlanTypes([]);
    setSelectedProductTypes([]);
    setSelectedSnpTypes([]);
    setSelectedGroupTypes([]);
    setGroupBy(null);
  };

  const clearGeoFilters = () => {
    setSelectedStates([]);
    setSelectedCounties([]);
    setAvailableCounties([]);
  };

  const removeParentOrg = (org: string) => {
    const newOrgs = selectedParentOrgs.filter(o => o !== org);
    setSelectedParentOrgs(newOrgs);
    // Reset to show industry total when all payers are removed
    if (newOrgs.length === 0) {
      setShowIndustryTotal(true);
    }
  };

  const addParentOrg = (org: string) => {
    if (!selectedParentOrgs.includes(org)) {
      setSelectedParentOrgs(prev => [...prev, org]);
    }
    setPayerSearch("");
    setShowPayerPopup(false);
  };

  const removeIndustryTotal = () => {
    setShowIndustryTotal(false);
  };

  const addIndustryTotal = () => {
    setShowIndustryTotal(true);
  };

  // Filter payers - show all when no search, filter when searching
  const filteredPayers = filterOptions?.parent_orgs?.filter(org =>
    org.toLowerCase().includes(payerSearch.toLowerCase()) &&
    !selectedParentOrgs.includes(org)
  ).slice(0, payerSearch ? 30 : 15) || [];

  return (
    <div className="max-w-7xl mx-auto px-4 py-6 space-y-4">
      {/* Control Bar */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4">
        <div className="flex items-center gap-3 flex-wrap">
          {/* Filter by Type Button */}
          <div className="relative">
            <button
              onClick={() => { setShowFilterPopup(!showFilterPopup); setShowGeoPopup(false); }}
              className={`flex items-center gap-2 px-4 py-2.5 rounded-lg font-medium transition-all shadow-sm ${
                hasTypeFilters
                  ? "bg-blue-600 text-white hover:bg-blue-700"
                  : "bg-gray-100 text-gray-700 hover:bg-gray-200"
              }`}
            >
              <Filter className="w-4 h-4" />
              <span>Filter by Type</span>
              {typeFilterCount > 0 && (
                <span className="bg-white text-blue-600 text-xs px-1.5 py-0.5 rounded-full font-bold">{typeFilterCount}</span>
              )}
              <ChevronDown className={`w-4 h-4 transition-transform ${showFilterPopup ? 'rotate-180' : ''}`} />
            </button>

            {/* Filter by Type Popup */}
            {showFilterPopup && (
              <div className="absolute top-full left-0 mt-2 w-[420px] bg-white rounded-xl shadow-xl border border-gray-200 p-5 z-50 max-h-[80vh] overflow-y-auto">
                <div className="flex justify-between items-center mb-4">
                  <span className="font-semibold text-gray-900">Filter by Type</span>
                  {hasTypeFilters && (
                    <button onClick={clearTypeFilters} className="text-sm text-blue-600 hover:text-blue-800 font-medium">
                      Clear all
                    </button>
                  )}
                </div>

                {/* Product Type - MA vs PDP */}
                <div className="mb-5">
                  <label className="block text-sm font-medium text-gray-700 mb-2">Product Type</label>
                  <div className="flex flex-wrap gap-2">
                    {["MA", "PDP"].map((type) => (
                      <button
                        key={type}
                        onClick={() => setSelectedProductTypes(prev =>
                          prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type]
                        )}
                        className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                          selectedProductTypes.includes(type)
                            ? "bg-emerald-600 text-white shadow-md"
                            : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                        }`}
                      >
                        {type === "MA" ? "Medicare Advantage" : "Part D (PDP)"}
                      </button>
                    ))}
                  </div>
                  <p className="text-xs text-gray-500 mt-1">MA includes MAPD and MA-only plans</p>
                </div>

                {/* Plan Type - HMO, PPO, PFFS, etc. */}
                <div className="mb-5">
                  <label className="block text-sm font-medium text-gray-700 mb-2">Plan Type</label>
                  <div className="flex flex-wrap gap-2">
                    {["HMO", "PPO", "PFFS", "MSA"].map((type) => (
                      <button
                        key={type}
                        onClick={() => setSelectedPlanTypes(prev =>
                          prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type]
                        )}
                        className={`px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                          selectedPlanTypes.includes(type)
                            ? "bg-blue-600 text-white shadow-md"
                            : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                        }`}
                      >
                        {type}
                      </button>
                    ))}
                  </div>
                  <p className="text-xs text-gray-500 mt-1">PPO includes Local & Regional PPO</p>
                </div>

                {/* Group Type - Individual vs Group/Employer - works on both sources */}
                <div className="mb-5">
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Market Segment
                  </label>
                  <div className="flex flex-wrap gap-2">
                    {["Individual", "Group"].map((type) => (
                      <button
                        key={type}
                        onClick={() => setSelectedGroupTypes(prev =>
                          prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type]
                        )}
                        className={`px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                          selectedGroupTypes.includes(type)
                            ? "bg-purple-600 text-white shadow-md"
                            : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                        }`}
                      >
                        {type}
                      </button>
                    ))}
                  </div>
                  <p className="text-xs text-gray-500 mt-1">Individual (direct enrollment) vs Group (employer-sponsored)</p>
                </div>

                {/* SNP Type (Special Needs Plans) - works on both sources */}
                <div className="mb-5">
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Special Needs Plans (SNP)
                  </label>
                  <div className="flex flex-wrap gap-2">
                    {(filterOptions?.snp_types || ["Non-SNP", "D-SNP", "C-SNP", "I-SNP"]).map((type) => (
                      <button
                        key={type}
                        onClick={() => setSelectedSnpTypes(prev =>
                          prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type]
                        )}
                        className={`px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                          selectedSnpTypes.includes(type)
                            ? "bg-orange-600 text-white shadow-md"
                            : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                        }`}
                      >
                        {type}
                      </button>
                    ))}
                  </div>
                  <p className="text-xs text-gray-500 mt-1">D-SNP: Dual Eligible, C-SNP: Chronic Condition, I-SNP: Institutional</p>
                </div>


                <button
                  onClick={() => setShowFilterPopup(false)}
                  className="w-full py-2.5 bg-gray-900 hover:bg-gray-800 text-white rounded-lg text-sm font-medium transition-colors"
                >
                  Apply Filters
                </button>
              </div>
            )}
          </div>

          {/* Filter by Geography Button - auto-switches to geographic source when clicked */}
          <div className="relative">
            <button
              onClick={() => { 
                if (dataSource === "national") {
                  setDataSource("geographic");
                }
                setShowGeoPopup(!showGeoPopup); 
                setShowFilterPopup(false); 
              }}
              title="Filter by state and county (uses CPSC data)"
              className={`flex items-center gap-2 px-4 py-2.5 rounded-lg font-medium transition-all shadow-sm ${
                hasGeoFilters
                  ? "bg-emerald-600 text-white hover:bg-emerald-700"
                  : "bg-gray-100 text-gray-700 hover:bg-gray-200"
              }`}
            >
              <Filter className="w-4 h-4" />
              <span>Filter by Geography</span>
              {geoFilterCount > 0 && (
                <span className={`text-xs px-1.5 py-0.5 rounded-full font-bold ${hasGeoFilters ? 'bg-white text-emerald-600' : 'bg-emerald-100 text-emerald-600'}`}>{geoFilterCount}</span>
              )}
              <ChevronDown className={`w-4 h-4 transition-transform ${showGeoPopup ? 'rotate-180' : ''}`} />
            </button>

            {/* Geography Filter Popup */}
            {showGeoPopup && (
              <div className="absolute top-full left-0 mt-2 w-96 bg-white rounded-xl shadow-xl border border-gray-200 p-5 z-50">
                <div className="flex justify-between items-center mb-4">
                  <span className="font-semibold text-gray-900">Filter by Geography</span>
                  {hasGeoFilters && (
                    <button onClick={clearGeoFilters} className="text-sm text-emerald-600 hover:text-emerald-800 font-medium">
                      Clear all
                    </button>
                  )}
                </div>

                {/* State Selection */}
                <div className="mb-4">
                  <label className="block text-sm font-medium text-gray-700 mb-2">States</label>
                  <input
                    type="text"
                    placeholder="Search states..."
                    value={stateSearch}
                    onChange={(e) => setStateSearch(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg mb-2 text-sm focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500 outline-none"
                  />
                  <div className="max-h-48 overflow-y-auto border border-gray-200 rounded-lg">
                    {filterOptions?.states?.filter(s =>
                      s.toLowerCase().includes(stateSearch.toLowerCase())
                    ).map((state) => (
                      <button
                        key={state}
                        onClick={() => setSelectedStates(prev =>
                          prev.includes(state) ? prev.filter(s => s !== state) : [...prev, state]
                        )}
                        className={`w-full text-left px-3 py-2 text-sm transition-colors flex items-center justify-between ${
                          selectedStates.includes(state)
                            ? "bg-emerald-50 text-emerald-700"
                            : "hover:bg-gray-50"
                        }`}
                      >
                        <span>{state}</span>
                        {selectedStates.includes(state) && (
                          <span className="text-emerald-600">✓</span>
                        )}
                      </button>
                    ))}
                  </div>
                  {selectedStates.length > 0 && (
                    <div className="mt-2 flex flex-wrap gap-1">
                      {selectedStates.map(state => (
                        <span key={state} className="inline-flex items-center gap-1 px-2 py-1 bg-emerald-100 text-emerald-700 rounded text-xs">
                          {state}
                          <button onClick={() => setSelectedStates(prev => prev.filter(s => s !== state))} className="hover:text-emerald-900">×</button>
                        </span>
                      ))}
                    </div>
                  )}
                </div>

                {/* County Selection - only show when states are selected */}
                {selectedStates.length > 0 && availableCounties.length > 0 && (
                  <div className="mb-4">
                    <label className="block text-sm font-medium text-gray-700 mb-2">Counties (optional)</label>
                    <div className="max-h-48 overflow-y-auto border border-gray-200 rounded-lg">
                      {availableCounties.map(({ state, county }) => (
                        <button
                          key={`${state}-${county}`}
                          onClick={() => setSelectedCounties(prev =>
                            prev.includes(county) ? prev.filter(c => c !== county) : [...prev, county]
                          )}
                          className={`w-full text-left px-3 py-2 text-sm transition-colors flex items-center justify-between ${
                            selectedCounties.includes(county)
                              ? "bg-emerald-50 text-emerald-700"
                              : "hover:bg-gray-50"
                          }`}
                        >
                          <span>{county} <span className="text-gray-400 text-xs">({state})</span></span>
                          {selectedCounties.includes(county) && (
                            <span className="text-emerald-600">✓</span>
                          )}
                        </button>
                      ))}
                    </div>
                    {selectedCounties.length > 0 && (
                      <div className="mt-2 flex flex-wrap gap-1">
                        {selectedCounties.map(county => (
                          <span key={county} className="inline-flex items-center gap-1 px-2 py-1 bg-emerald-100 text-emerald-700 rounded text-xs">
                            {county}
                            <button onClick={() => setSelectedCounties(prev => prev.filter(c => c !== county))} className="hover:text-emerald-900">×</button>
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                )}

                <button
                  onClick={() => setShowGeoPopup(false)}
                  className="w-full py-2.5 bg-gray-900 hover:bg-gray-800 text-white rounded-lg text-sm font-medium transition-colors"
                >
                  Apply Filters
                </button>
              </div>
            )}
          </div>

          {/* Add Payer Button */}
          <div className="relative">
            <button
              onClick={() => setShowPayerPopup(!showPayerPopup)}
              className="flex items-center gap-2 px-4 py-2.5 rounded-lg bg-gray-100 text-gray-700 hover:bg-gray-200 font-medium transition-all shadow-sm"
            >
              <Plus className="w-4 h-4" />
              <span>Add Payer</span>
            </button>

            {/* Payer Popup */}
            {showPayerPopup && (
              <div className="absolute top-full left-0 mt-2 w-[420px] bg-white rounded-xl shadow-xl border border-gray-200 p-4 z-50">
                <input
                  type="text"
                  placeholder="Search payers (e.g., United, Humana, CVS)..."
                  value={payerSearch}
                  onChange={(e) => setPayerSearch(e.target.value)}
                  className="w-full px-4 py-3 border border-gray-300 rounded-lg mb-3 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
                  autoFocus
                />
                <div className="max-h-72 overflow-y-auto">
                  {filteredPayers.map((org, idx) => (
                    <button
                      key={org}
                      onClick={() => addParentOrg(org)}
                      className="w-full text-left px-4 py-3 hover:bg-blue-50 rounded-lg text-sm transition-colors flex items-center justify-between group"
                    >
                      <span className="font-medium text-gray-800">{org}</span>
                      <span className="text-xs text-gray-400 group-hover:text-blue-600">#{idx + 1}</span>
                    </button>
                  ))}
                  {filteredPayers.length === 0 && payerSearch && (
                    <div className="text-gray-500 text-sm px-4 py-3">No payers found matching "{payerSearch}"</div>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* Industry Total Chip - show when payers are selected */}
          {selectedParentOrgs.length > 0 && showIndustryTotal && (
            <span
              className="inline-flex items-center gap-1.5 px-3 py-2 rounded-lg bg-gray-200 text-gray-800 text-sm font-medium"
            >
              Industry Total
              <button onClick={removeIndustryTotal} className="hover:text-gray-600 ml-1">
                <X className="w-4 h-4" />
              </button>
            </span>
          )}

          {/* Add Industry Total back button - show when hidden and payers exist */}
          {selectedParentOrgs.length > 0 && !showIndustryTotal && (
            <button
              onClick={addIndustryTotal}
              className="inline-flex items-center gap-1.5 px-3 py-2 rounded-lg bg-gray-100 text-gray-600 text-sm font-medium hover:bg-gray-200 transition-colors"
            >
              <Plus className="w-4 h-4" />
              Industry
            </button>
          )}

          {/* Selected Payers */}
          {selectedParentOrgs.map((org) => (
            <span
              key={org}
              className="inline-flex items-center gap-1.5 px-3 py-2 rounded-lg bg-purple-100 text-purple-800 text-sm font-medium"
            >
              {org.length > 30 ? org.substring(0, 30) + "..." : org}
              <button onClick={() => removeParentOrg(org)} className="hover:text-purple-600 ml-1">
                <X className="w-4 h-4" />
              </button>
            </span>
          ))}

          {/* Total Display */}
          <div className="ml-auto text-right bg-gray-50 px-4 py-2 rounded-lg">
            <div className="text-xs text-gray-500 uppercase tracking-wide">Total Enrollment</div>
            <div className="text-2xl font-bold text-gray-900">{formatNumber(latestTotal)}</div>
          </div>
        </div>
      </div>

      {/* Click outside to close popups */}
      {(showFilterPopup || showGeoPopup || showPayerPopup) && (
        <div
          className="fixed inset-0 z-40"
          onClick={() => {
            setShowFilterPopup(false);
            setShowGeoPopup(false);
            setShowPayerPopup(false);
          }}
        />
      )}

      {/* CPSC Data Source Notice */}
      {dataSource === "geographic" && (
        <div className="bg-amber-50 border border-amber-200 rounded-lg px-4 py-3 mb-4 flex items-center gap-3">
          <Info className="w-5 h-5 text-amber-600 flex-shrink-0" />
          <div className="text-sm">
            <span className="font-medium text-amber-800">Using CPSC Data Source</span>
            <span className="text-amber-700 ml-2">
              Geographic data may have suppressed values (&lt;10 enrollees) per HIPAA. 
              {hasGeoFilters && <span className="font-medium"> Filtered by: {selectedStates.join(", ")}{selectedCounties.length > 0 && `, ${selectedCounties.length} counties`}</span>}
            </span>
          </div>
          <button 
            onClick={() => { setDataSource("national"); setSelectedStates([]); setSelectedCounties([]); }}
            className="ml-auto text-xs text-amber-600 hover:text-amber-800 font-medium whitespace-nowrap"
          >
            Switch to National
          </button>
        </div>
      )}

      {/* Chart */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-gray-900">
            MA Enrollment Over Time
            {(hasTypeFilters || hasGeoFilters) && <span className="text-sm font-normal text-gray-500 ml-2">(filtered)</span>}
          </h2>
          <div className="flex items-center gap-4">
            {/* Data Source Toggle */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">Source:</span>
              {[
                { value: "national" as const, label: "National", tooltip: "Exact totals (no geography)" },
                { value: "geographic" as const, label: "Geographic", tooltip: "Has state/county (suppressed <10)" },
              ].map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => {
                    setDataSource(opt.value);
                    // Only clear geography filters when switching to national (geography not available)
                    // Group type and SNP type filters now work on both sources
                    if (opt.value === "national") {
                      setSelectedStates([]);
                      setSelectedCounties([]);
                    }
                  }}
                  title={opt.tooltip}
                  className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                    dataSource === opt.value
                      ? "bg-green-600 text-white"
                      : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
            {/* Year Range Toggle */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">Show:</span>
              {[
                { value: null, label: "All" },
                { value: 10, label: "10Y" },
                { value: 5, label: "5Y" },
                { value: 3, label: "3Y" },
              ].map((opt) => (
                <button
                  key={opt.label}
                  onClick={() => setYearRange(opt.value)}
                  className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                    yearRange === opt.value
                      ? "bg-gray-900 text-white"
                      : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>
        </div>
        <div className="h-96">
          {activeLoading || !isMounted ? (
            <div className="flex items-center justify-center h-full">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            </div>
          ) : chartData.length > 0 ? (
            <ResponsiveContainer width="100%" height={380} minHeight={300}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="year" tick={{ fill: '#6b7280' }} />
                <YAxis
                  tickFormatter={formatNumber}
                  tick={{ fill: '#6b7280' }}
                  domain={[0, Math.ceil(maxDataValue * 1.1)]}
                  width={70}
                />
                <Tooltip
                  formatter={(value) => [formatNumber(value as number), ""]}
                  labelFormatter={(year) => `Year: ${year}`}
                  contentStyle={{ borderRadius: '8px', border: '1px solid #e5e7eb' }}
                />
                <Legend />
                {seriesKeys.map((key, i) => (
                  <Line
                    key={key}
                    type="monotone"
                    dataKey={key}
                    stroke={COLORS[i % COLORS.length]}
                    strokeWidth={2.5}
                    dot={{ fill: COLORS[i % COLORS.length], r: 4 }}
                    activeDot={{ 
                      r: 8, 
                      cursor: 'pointer',
                      onClick: (data: any) => {
                        if (data?.payload) {
                          setCellDetail({ payer: key, year: data.payload.year, value: data.payload[key] });
                        }
                      }
                    }}
                    name={key}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex items-center justify-center h-full text-gray-500">
              No data available
            </div>
          )}
        </div>
      </div>

      {/* Data Table */}
      {chartData.length > 0 && (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
            <h3 className="text-lg font-semibold text-gray-900">
              Enrollment by Year
            </h3>
            {rawTimeseriesData?.audit_id && (
              <AuditButton
                audit={{
                  query_id: rawTimeseriesData.audit_id,
                  sql: `-- Enrollment Timeseries Query\n-- Source: ${dataSource === 'geographic' ? 'Geographic (state-level)' : 'National'}`,
                  tables_queried: ['fact_enrollment_unified'],
                  filters_applied: rawTimeseriesData.filters || {},
                  row_count: rawTimeseriesData.years?.length || 0,
                  executed_at: new Date().toISOString(),
                }}
                label="View Query"
              />
            )}
          </div>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider sticky left-0 bg-gray-50">
                    Payer
                  </th>
                  {chartData.map((row) => (
                    <th key={row.year} className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider whitespace-nowrap">
                      {row.year}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {seriesKeys.map((key, idx) => (
                  <React.Fragment key={key}>
                    {/* Data row */}
                    <tr className={idx % 2 === 0 ? "bg-white" : "bg-gray-50/50"}>
                      <td className="px-4 py-3 text-sm font-medium text-gray-900 sticky left-0 bg-inherit">
                        <div className="flex items-center gap-2">
                          <span
                            className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                            style={{ backgroundColor: COLORS[idx % COLORS.length] }}
                          />
                          <span className="truncate max-w-[200px]" title={key}>{key}</span>
                        </div>
                      </td>
                      {chartData.map((row) => (
                        <td 
                          key={row.year} 
                          className="px-4 py-3 text-sm text-gray-700 text-right font-mono whitespace-nowrap cursor-pointer hover:bg-blue-50 transition-colors"
                          onClick={() => setCellDetail({ payer: key, year: row.year, value: row[key] })}
                          title="Click to view data source"
                        >
                          {formatFullNumber(row[key])}
                        </td>
                      ))}
                    </tr>
                    {/* YoY % change row */}
                    <tr key={`${key}-yoy`} className="bg-gray-50/30">
                      <td className="px-4 py-1 text-xs text-gray-400 sticky left-0 bg-inherit italic">
                        YoY %
                      </td>
                      {chartData.map((row, i) => {
                        if (i === 0) {
                          return <td key={`${row.year}-yoy`} className="px-4 py-1 text-xs text-gray-400 text-right">—</td>;
                        }
                        const prev = chartData[i - 1][key];
                        const curr = row[key];
                        const change = prev && prev > 0 ? ((curr - prev) / prev * 100) : 0;
                        const isPositive = change > 0;
                        const isNegative = change < 0;
                        return (
                          <td key={`${row.year}-yoy`} className={`px-4 py-1 text-xs text-right font-mono ${isPositive ? 'text-green-600' : isNegative ? 'text-red-600' : 'text-gray-400'}`}>
                            {change !== 0 ? `${isPositive ? '+' : ''}${change.toFixed(1)}%` : '—'}
                          </td>
                        );
                      })}
                    </tr>
                  </React.Fragment>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Cell Detail Modal */}
      {cellDetail && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" onClick={() => setCellDetail(null)}>
          <div className="bg-white rounded-xl shadow-2xl max-w-lg w-full max-h-[85vh] overflow-auto" onClick={e => e.stopPropagation()}>
            {/* Header with value */}
            <div className="px-5 py-4 border-b border-gray-200 flex items-center justify-between sticky top-0 bg-white">
              <div className="flex items-center gap-4">
                <div className="bg-blue-100 rounded-lg px-4 py-2">
                  <div className="text-2xl font-bold text-blue-900">{formatFullNumber(cellDetail.value)}</div>
                </div>
                <div>
                  <div className="font-semibold text-gray-900">{cellDetail.payer}</div>
                  <div className="text-sm text-gray-500">{cellDetail.year}</div>
                </div>
              </div>
              <button onClick={() => setCellDetail(null)} className="p-1.5 hover:bg-gray-100 rounded-lg">
                <X className="w-5 h-5 text-gray-400" />
              </button>
            </div>

            {/* Content - compact */}
            <div className="p-4 space-y-3">
              {/* Data Source & Filters in one row */}
              <div className="grid grid-cols-2 gap-3">
                <div className="bg-gray-50 rounded-lg p-3">
                  <div className="text-xs font-medium text-gray-500 mb-1">Source</div>
                  <div className="text-sm font-medium text-gray-900">CMS CPSC</div>
                  <div className="text-xs text-gray-500">{dataSource === "geographic" ? "State/County" : "National"}</div>
                </div>
                <div className="bg-gray-50 rounded-lg p-3">
                  <div className="text-xs font-medium text-gray-500 mb-1">Filters</div>
                  <div className="text-xs text-gray-700 space-y-0.5">
                    {selectedPlanTypes.length > 0 && <div>Plans: {selectedPlanTypes.join(", ")}</div>}
                    {selectedSnpTypes.length > 0 && <div>SNP: {selectedSnpTypes.join(", ")}</div>}
                    {selectedStates.length > 0 && <div>States: {selectedStates.join(", ")}</div>}
                    {selectedPlanTypes.length === 0 && selectedSnpTypes.length === 0 && selectedStates.length === 0 && (
                      <div className="text-gray-400">None</div>
                    )}
                  </div>
                </div>
              </div>

              {/* SQL Query - collapsible */}
              <details className="bg-gray-900 rounded-lg">
                <summary className="px-3 py-2 text-xs font-medium text-gray-400 cursor-pointer hover:text-gray-300">
                  View SQL Query
                </summary>
                <pre className="px-3 pb-3 text-xs text-green-400 overflow-x-auto whitespace-pre-wrap">
{`SELECT year, SUM(enrollment) as enrollment
FROM fact_enrollment_unified
WHERE year = ${cellDetail.year}
  AND parent_org = '${cellDetail.payer}'${selectedPlanTypes.length > 0 ? `
  AND plan_type IN ('${selectedPlanTypes.join("', '")}')` : ''}${selectedStates.length > 0 ? `
  AND state IN ('${selectedStates.join("', '")}')` : ''}
GROUP BY year`}
                </pre>
              </details>

              {/* Download Button - Downloads ZIP with raw CMS files + README */}
              <button
                onClick={() => {
                  const params = new URLSearchParams();
                  params.set('year', cellDetail.year.toString());
                  params.set('parent_org', cellDetail.payer);
                  params.set('data_source', dataSource === 'geographic' ? 'geographic' : 'national');
                  if (selectedPlanTypes.length > 0) {
                    params.set('plan_types', selectedPlanTypes.join(','));
                  }
                  window.open(`${API_BASE}/api/v5/enrollment/audit-download?${params.toString()}`, '_blank');
                }}
                className="w-full flex items-center justify-center gap-2 px-4 py-2.5 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors font-medium"
              >
                <Download className="w-4 h-4" />
                Download Audit Package (ZIP)
              </button>
              <p className="text-xs text-gray-400 text-center mt-1">
                Includes raw CMS files + README to replicate calculation
              </p>

              {/* Audit ID */}
              {rawTimeseriesData?.audit_id && (
                <div className="text-xs text-gray-400 text-center">
                  Audit: {rawTimeseriesData.audit_id}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
