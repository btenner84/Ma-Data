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
import { Filter, Plus, X, ChevronDown } from "lucide-react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

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
  group_types: string[];
  snp_types: string[];
  states: string[];
  parent_orgs: string[];
}

interface TimeseriesData {
  years: number[];
  total_enrollment?: number[];
  series?: Record<string, number[]>;
  group_by?: string;
}

export default function EnrollmentPage() {
  const [selectedPlanTypes, setSelectedPlanTypes] = useState<string[]>([]);
  const [selectedProductTypes, setSelectedProductTypes] = useState<string[]>(['MA']); // Default to MA (includes MA-only + MAPD)
  const [selectedGroupTypes, setSelectedGroupTypes] = useState<string[]>([]);
  const [selectedSnpTypes, setSelectedSnpTypes] = useState<string[]>([]);
  const [selectedStates, setSelectedStates] = useState<string[]>([]);
  const [selectedCounties, setSelectedCounties] = useState<string[]>([]);
  const [availableCounties, setAvailableCounties] = useState<{state: string, county: string}[]>([]);
  const [selectedParentOrgs, setSelectedParentOrgs] = useState<string[]>([]);
  const [showIndustryTotal, setShowIndustryTotal] = useState(true); // Show industry total line
  const [groupBy, setGroupBy] = useState<string | null>(null);
  const [yearRange, setYearRange] = useState<number | null>(null); // null = all years
  const [showSnpData, setShowSnpData] = useState(false); // Toggle SNP view
  const [viewMode, setViewMode] = useState<"enrollment" | "market_share">("enrollment"); // Toggle enrollment vs market share

  // Popup states
  const [showFilterPopup, setShowFilterPopup] = useState(false);
  const [showGeoPopup, setShowGeoPopup] = useState(false);
  const [showPayerPopup, setShowPayerPopup] = useState(false);
  const [payerSearch, setPayerSearch] = useState("");
  const [stateSearch, setStateSearch] = useState("");

  // Fetch filter options
  const { data: filterOptions } = useQuery<FilterOptions>({
    queryKey: ["enrollment-filters"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/v2/enrollment/filters`);
      return res.json();
    },
  });

  // Fetch counties when states change
  useEffect(() => {
    if (selectedStates.length > 0) {
      fetch(`${API_BASE}/api/v2/enrollment/counties?states=${selectedStates.join(',')}`)
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
      // Map "MA" to "MA-only,MAPD" for the API
      const apiProductTypes = selectedProductTypes.flatMap(t => t === "MA" ? ["MA-only", "MAPD"] : [t]);
      params.set("product_types", apiProductTypes.join(","));
    }
    if (selectedGroupTypes.length > 0) params.set("group_types", selectedGroupTypes.join(","));
    if (selectedSnpTypes.length > 0) params.set("snp_types", selectedSnpTypes.join(",")); // SNP filter
    if (selectedStates.length > 0) params.set("states", selectedStates.join(",")); // State filter
    if (selectedCounties.length > 0) params.set("counties", selectedCounties.join("|")); // County filter (| separator)
    if (selectedParentOrgs.length > 0) params.set("parent_orgs", selectedParentOrgs.join("|")); // Use | separator since org names contain commas
    if (groupBy) params.set("group_by", groupBy);
    params.set("view_mode", viewMode);
    params.set("include_total", "true"); // Always fetch, we filter client-side
    return params.toString();
  };

  // Fetch timeseries data (includes SNP filtering)
  const { data: rawTimeseriesData, isLoading } = useQuery<TimeseriesData>({
    queryKey: ["enrollment-timeseries", selectedPlanTypes, selectedProductTypes, selectedGroupTypes, selectedSnpTypes, selectedStates, selectedCounties, selectedParentOrgs, groupBy, viewMode],
    queryFn: async () => {
      const params = buildQueryParams();
      const res = await fetch(`${API_BASE}/api/v2/enrollment/timeseries?${params}`);
      return res.json();
    },
  });

  // Filter out Industry Total if not showing it (client-side filtering)
  const timeseriesData = rawTimeseriesData ? {
    ...rawTimeseriesData,
    series: rawTimeseriesData.series
      ? Object.fromEntries(
          Object.entries(rawTimeseriesData.series).filter(([key]) =>
            showIndustryTotal || key !== 'Industry Total'
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

  const hasTypeFilters = selectedPlanTypes.length > 0 || selectedProductTypes.length > 0 || selectedGroupTypes.length > 0 || selectedSnpTypes.length > 0;
  const hasGeoFilters = selectedStates.length > 0 || selectedCounties.length > 0;
  const typeFilterCount = selectedPlanTypes.length + selectedProductTypes.length + selectedGroupTypes.length + selectedSnpTypes.length;
  const geoFilterCount = selectedStates.length + selectedCounties.length;

  const clearTypeFilters = () => {
    setSelectedPlanTypes([]);
    setSelectedProductTypes([]);
    setSelectedGroupTypes([]);
    setSelectedSnpTypes([]);
    setGroupBy(null);
    setShowSnpData(false);
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
    <div className="space-y-4">
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
              <div className="absolute top-full left-0 mt-2 w-96 bg-white rounded-xl shadow-xl border border-gray-200 p-5 z-50">
                <div className="flex justify-between items-center mb-4">
                  <span className="font-semibold text-gray-900">Filter by Type</span>
                  {hasTypeFilters && (
                    <button onClick={clearTypeFilters} className="text-sm text-blue-600 hover:text-blue-800 font-medium">
                      Clear all
                    </button>
                  )}
                </div>

                {/* Group Type (Individual vs Group) */}
                {filterOptions?.group_types && filterOptions.group_types.length > 0 && (
                  <div className="mb-5">
                    <label className="block text-sm font-medium text-gray-700 mb-2">Market Segment</label>
                    <div className="flex gap-2">
                      {filterOptions.group_types.map((type) => (
                        <button
                          key={type}
                          onClick={() => setSelectedGroupTypes(prev =>
                            prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type]
                          )}
                          className={`flex-1 px-4 py-2.5 rounded-lg text-sm font-medium transition-all ${
                            selectedGroupTypes.includes(type)
                              ? "bg-indigo-600 text-white shadow-md"
                              : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                          }`}
                        >
                          {type}
                        </button>
                      ))}
                    </div>
                  </div>
                )}

                {/* Product Type - MA (includes MA-only + MAPD) vs PDP */}
                <div className="mb-5">
                  <label className="block text-sm font-medium text-gray-700 mb-2">Product Type</label>
                  <div className="flex flex-wrap gap-2">
                    {["MA", "PDP"].map((type) => (
                      <button
                        key={type}
                        onClick={() => setSelectedProductTypes(prev =>
                          prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type]
                        )}
                        className={`px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                          selectedProductTypes.includes(type)
                            ? "bg-emerald-600 text-white shadow-md"
                            : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                        }`}
                      >
                        {type}
                      </button>
                    ))}
                  </div>
                </div>

                {/* Plan Type (simplified: HMO includes HMO-POS, PPO includes Local/Regional) */}
                <div className="mb-5">
                  <label className="block text-sm font-medium text-gray-700 mb-2">Plan Type</label>
                  <div className="flex flex-wrap gap-2">
                    {(filterOptions?.plan_types_simplified || filterOptions?.plan_types)?.filter(t => !['Unknown', 'Other', 'Employer PDP', 'Employer PFFS', '1876 Cost', 'Employer/Union Only Direct Contract PDP', 'PACE', 'National PACE', 'Medicare-Medicaid Plan', 'Medicare-Medicaid Plan HMO/HMOPOS', 'Medicare Prescription Drug Plan'].includes(t) && !t.includes('Medicare-Medicaid')).map((type) => (
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
                </div>

                {/* SNP Type (Special Needs Plans) */}
                {filterOptions?.snp_types && filterOptions.snp_types.length > 0 && (
                  <div className="mb-5">
                    <label className="block text-sm font-medium text-gray-700 mb-2">Special Needs Plans (SNP)</label>
                    <div className="flex flex-wrap gap-2">
                      {filterOptions.snp_types.map((type) => (
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
                )}

                <button
                  onClick={() => setShowFilterPopup(false)}
                  className="mt-2 w-full py-2.5 bg-gray-900 hover:bg-gray-800 text-white rounded-lg text-sm font-medium transition-colors"
                >
                  Apply Filters
                </button>
              </div>
            )}
          </div>

          {/* Filter by Geography Button */}
          <div className="relative">
            <button
              onClick={() => { setShowGeoPopup(!showGeoPopup); setShowFilterPopup(false); }}
              className={`flex items-center gap-2 px-4 py-2.5 rounded-lg font-medium transition-all shadow-sm ${
                hasGeoFilters
                  ? "bg-emerald-600 text-white hover:bg-emerald-700"
                  : "bg-gray-100 text-gray-700 hover:bg-gray-200"
              }`}
            >
              <Filter className="w-4 h-4" />
              <span>Filter by Geography</span>
              {geoFilterCount > 0 && (
                <span className="bg-white text-emerald-600 text-xs px-1.5 py-0.5 rounded-full font-bold">{geoFilterCount}</span>
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

      {/* Chart */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-gray-900">
            MA Enrollment Over Time
            {(hasTypeFilters || hasGeoFilters) && <span className="text-sm font-normal text-gray-500 ml-2">(filtered)</span>}
          </h2>
          <div className="flex items-center gap-4">
            {/* View Mode Toggle */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">View:</span>
              {[
                { value: "enrollment" as const, label: "Enrollment" },
                { value: "market_share" as const, label: "Market Share %" },
              ].map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => setViewMode(opt.value)}
                  className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                    viewMode === opt.value
                      ? "bg-blue-600 text-white"
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
          {activeLoading ? (
            <div className="flex items-center justify-center h-full">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            </div>
          ) : chartData.length > 0 ? (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="year" tick={{ fill: '#6b7280' }} />
                <YAxis
                  tickFormatter={viewMode === "market_share" ? formatPercent : formatNumber}
                  tick={{ fill: '#6b7280' }}
                  domain={viewMode === "market_share" ? [0, Math.min(100, Math.ceil(maxDataValue * 1.2))] : [0, Math.ceil(maxDataValue * 1.1)]}
                  width={70}
                />
                <Tooltip
                  formatter={(value) => [viewMode === "market_share" ? formatPercent(value as number) : formatNumber(value as number), ""]}
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
                    activeDot={{ r: 6 }}
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
          <div className="px-6 py-4 border-b border-gray-200">
            <h3 className="text-lg font-semibold text-gray-900">
              {viewMode === "market_share" ? "Market Share by Year" : "Enrollment by Year"}
            </h3>
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
                        <td key={row.year} className="px-4 py-3 text-sm text-gray-700 text-right font-mono whitespace-nowrap">
                          {viewMode === "market_share"
                            ? formatPercent(row[key])
                            : formatFullNumber(row[key])}
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
    </div>
  );
}
