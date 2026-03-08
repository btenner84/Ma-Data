"use client";

import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { ChevronDown, Search, Filter } from "lucide-react";

const API_BASE = (process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000").replace(/\/$/, "");

function useIsMounted() {
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  return mounted;
}

function formatNumber(num: number | null | undefined): string {
  if (num === null || num === undefined || isNaN(num)) return "-";
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(0)}K`;
  return num.toLocaleString();
}

function formatPercent(num: number | null | undefined): string {
  if (num === null || num === undefined || isNaN(num)) return "-";
  return `${num.toFixed(1)}%`;
}

function formatRisk(num: number | null | undefined): string {
  if (num === null || num === undefined || isNaN(num)) return "-";
  return num.toFixed(3);
}

interface FilterOptions {
  years: number[];
  parent_orgs: string[];
  plan_types: string[];
  product_types: string[];
  snp_types: string[];
  group_types: string[];
  states: string[];
}

export default function SummaryPage() {
  const isMounted = useIsMounted();
  
  // Selected payer (null = Industry)
  const [selectedPayer, setSelectedPayer] = useState<string | null>(null);
  const [payerSearch, setPayerSearch] = useState("");
  const [showPayerDropdown, setShowPayerDropdown] = useState(false);
  
  // Filters
  const [planTypes, setPlanTypes] = useState<string[]>([]);
  const [productTypes, setProductTypes] = useState<string[]>([]);
  const [snpTypes, setSnpTypes] = useState<string[]>([]);
  const [groupTypes, setGroupTypes] = useState<string[]>([]);
  const [states, setStates] = useState<string[]>([]);
  const [startYear, setStartYear] = useState<number>(2017);
  const [endYear, setEndYear] = useState<number>(2026);
  
  // UI state
  const [showFilters, setShowFilters] = useState(false);

  // ========== FETCH FILTER OPTIONS (v5) - no fallback ==========
  const { data: filterOptions, isLoading: filtersLoading } = useQuery<FilterOptions>({
    queryKey: ["v5-filters"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/v5/filters`);
      return res.json();
    },
  });

  // Build query params for v5 endpoints
  const buildParams = () => {
    const params = new URLSearchParams();
    if (selectedPayer) params.append("parent_org", selectedPayer);
    if (planTypes.length) params.append("plan_types", planTypes.join(","));
    if (productTypes.length) params.append("product_types", productTypes.join(","));
    if (snpTypes.length) params.append("snp_types", snpTypes.join(","));
    if (groupTypes.length) params.append("group_types", groupTypes.join(","));
    if (states.length) params.append("states", states.join(","));
    params.append("start_year", startYear.toString());
    params.append("end_year", endYear.toString());
    return params.toString();
  };

  // ========== ENROLLMENT TIMESERIES (v5) ==========
  const { data: enrollmentData, isLoading: enrollmentLoading } = useQuery({
    queryKey: ["v5-enrollment", selectedPayer, planTypes, productTypes, snpTypes, groupTypes, states, startYear, endYear],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/v5/enrollment/timeseries?${buildParams()}`);
      return res.json();
    },
  });

  // ========== STARS TIMESERIES (v5) ==========
  const { data: starsData, isLoading: starsLoading } = useQuery({
    queryKey: ["v5-stars", selectedPayer, planTypes, productTypes, snpTypes, startYear, endYear],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (selectedPayer) params.append("parent_org", selectedPayer);
      if (planTypes.length) params.append("plan_types", planTypes.join(","));
      if (productTypes.length) params.append("product_types", productTypes.join(","));
      if (snpTypes.length) params.append("snp_types", snpTypes.join(","));
      params.append("start_year", startYear.toString());
      params.append("end_year", endYear.toString());
      const res = await fetch(`${API_BASE}/api/v5/stars/timeseries?${params.toString()}`);
      return res.json();
    },
  });

  // ========== RISK TIMESERIES (v5) ==========
  const { data: riskData, isLoading: riskLoading } = useQuery({
    queryKey: ["v5-risk", selectedPayer, planTypes, snpTypes, startYear, endYear],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (selectedPayer) params.append("parent_org", selectedPayer);
      if (planTypes.length) params.append("plan_types", planTypes.join(","));
      if (snpTypes.length) params.append("snp_types", snpTypes.join(","));
      params.append("start_year", startYear.toString());
      params.append("end_year", Math.min(endYear, 2024).toString()); // Risk only to 2024
      const res = await fetch(`${API_BASE}/api/v5/risk/timeseries?${params.toString()}`);
      return res.json();
    },
  });

  // Transform data for charts
  const enrollmentChartData = enrollmentData?.years?.map((year: number, i: number) => ({
    year,
    enrollment: enrollmentData.enrollment?.[i] || 0,
  })) || [];

  const starsChartData = starsData?.years?.map((year: number, i: number) => ({
    year,
    pct_fourplus: starsData.pct_fourplus?.[i] || 0,
  })) || [];

  const riskChartData = riskData?.years?.map((year: number, i: number) => ({
    year,
    risk: riskData.wavg_risk?.[i] || 0,
  })) || [];

  // Filter payers for dropdown
  const filteredPayers = filterOptions?.parent_orgs?.filter(
    p => p?.toLowerCase().includes(payerSearch.toLowerCase())
  ).slice(0, 20) || [];

  const displayName = selectedPayer || "Industry";
  const activeFilterCount = planTypes.length + productTypes.length + snpTypes.length + groupTypes.length + states.length;

  // Get latest values for summary cards
  const latestEnrollment = enrollmentData?.enrollment?.[enrollmentData.enrollment.length - 1] || 0;
  const latestStars = starsData?.pct_fourplus?.[starsData.pct_fourplus.length - 1] || 0;
  const latestRisk = riskData?.wavg_risk?.[riskData.wavg_risk.length - 1] || 0;

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b sticky top-0 z-20">
        <div className="max-w-7xl mx-auto px-4 py-4">
          <div className="flex items-center justify-between gap-4 flex-wrap">
            {/* Payer Selector */}
            <div className="relative">
              <button
                onClick={() => setShowPayerDropdown(!showPayerDropdown)}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-medium min-w-[200px]"
              >
                <span className="truncate">{displayName}</span>
                <ChevronDown className="w-4 h-4 flex-shrink-0" />
              </button>
              
              {showPayerDropdown && (
                <div className="absolute top-full left-0 mt-1 w-80 bg-white rounded-lg shadow-xl border z-30">
                  <div className="p-2 border-b">
                    <div className="relative">
                      <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                      <input
                        type="text"
                        placeholder="Search parent org..."
                        value={payerSearch}
                        onChange={(e) => setPayerSearch(e.target.value)}
                        className="w-full pl-9 pr-3 py-2 border rounded-lg text-sm"
                        autoFocus
                      />
                    </div>
                  </div>
                  <div className="max-h-64 overflow-y-auto">
                    <button
                      onClick={() => { setSelectedPayer(null); setShowPayerDropdown(false); setPayerSearch(""); }}
                      className={`w-full text-left px-4 py-2 hover:bg-gray-100 ${!selectedPayer ? 'bg-blue-50 text-blue-700 font-medium' : ''}`}
                    >
                      Industry (All)
                    </button>
                    {filteredPayers.map(payer => (
                      <button
                        key={payer}
                        onClick={() => { setSelectedPayer(payer); setShowPayerDropdown(false); setPayerSearch(""); }}
                        className={`w-full text-left px-4 py-2 hover:bg-gray-100 text-sm ${selectedPayer === payer ? 'bg-blue-50 text-blue-700 font-medium' : ''}`}
                      >
                        {payer}
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>

            {/* Filter Button */}
            <button
              onClick={() => setShowFilters(!showFilters)}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg border ${activeFilterCount > 0 ? 'bg-blue-50 border-blue-300 text-blue-700' : 'bg-white hover:bg-gray-50'}`}
            >
              <Filter className="w-4 h-4" />
              <span>Filters</span>
              {activeFilterCount > 0 && (
                <span className="bg-blue-600 text-white text-xs px-1.5 py-0.5 rounded-full">{activeFilterCount}</span>
              )}
            </button>

            {/* Year Range */}
            <div className="flex items-center gap-2 text-sm">
              <span className="text-gray-500">Years:</span>
              <select
                value={startYear}
                onChange={(e) => setStartYear(Number(e.target.value))}
                className="border rounded px-2 py-1"
              >
                {filterOptions?.years?.map(y => <option key={y} value={y}>{y}</option>)}
              </select>
              <span>to</span>
              <select
                value={endYear}
                onChange={(e) => setEndYear(Number(e.target.value))}
                className="border rounded px-2 py-1"
              >
                {filterOptions?.years?.map(y => <option key={y} value={y}>{y}</option>)}
              </select>
            </div>
          </div>

          {/* Filter Panel */}
          {showFilters && (
            <div className="mt-4 p-4 bg-gray-50 rounded-lg border">
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                {/* Plan Type */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Plan Type</label>
                  <div className="space-y-1 max-h-32 overflow-y-auto">
                    {(filterOptions?.plan_types?.length ? filterOptions.plan_types : ['HMO/HMOPOS', 'Local PPO', 'Regional PPO', 'PFFS', 'MSA']).map(pt => (
                      <label key={pt} className="flex items-center gap-2 text-sm">
                        <input
                          type="checkbox"
                          checked={planTypes.includes(pt)}
                          onChange={(e) => {
                            if (e.target.checked) setPlanTypes([...planTypes, pt]);
                            else setPlanTypes(planTypes.filter(x => x !== pt));
                          }}
                        />
                        {pt}
                      </label>
                    ))}
                  </div>
                </div>

                {/* Product Type */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Product Type</label>
                  <div className="space-y-1">
                    {(filterOptions?.product_types?.length ? filterOptions.product_types : ['MAPD', 'PDP']).map(pt => (
                      <label key={pt} className="flex items-center gap-2 text-sm">
                        <input
                          type="checkbox"
                          checked={productTypes.includes(pt)}
                          onChange={(e) => {
                            if (e.target.checked) setProductTypes([...productTypes, pt]);
                            else setProductTypes(productTypes.filter(x => x !== pt));
                          }}
                        />
                        {pt}
                      </label>
                    ))}
                  </div>
                </div>

                {/* SNP Type */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">SNP Type</label>
                  <div className="space-y-1">
                    {(filterOptions?.snp_types?.length ? filterOptions.snp_types : ['Non-SNP', 'D-SNP', 'C-SNP', 'I-SNP']).map(st => (
                      <label key={st} className="flex items-center gap-2 text-sm">
                        <input
                          type="checkbox"
                          checked={snpTypes.includes(st)}
                          onChange={(e) => {
                            if (e.target.checked) setSnpTypes([...snpTypes, st]);
                            else setSnpTypes(snpTypes.filter(x => x !== st));
                          }}
                        />
                        {st}
                      </label>
                    ))}
                  </div>
                </div>

                {/* Group Type */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Group Type</label>
                  <div className="space-y-1">
                    {(filterOptions?.group_types?.length ? filterOptions.group_types : ['Individual', 'Group']).map(gt => (
                      <label key={gt} className="flex items-center gap-2 text-sm">
                        <input
                          type="checkbox"
                          checked={groupTypes.includes(gt)}
                          onChange={(e) => {
                            if (e.target.checked) setGroupTypes([...groupTypes, gt]);
                            else setGroupTypes(groupTypes.filter(x => x !== gt));
                          }}
                        />
                        {gt}
                      </label>
                    ))}
                  </div>
                </div>

                {/* State */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">State</label>
                  <select
                    multiple
                    value={states}
                    onChange={(e) => setStates(Array.from(e.target.selectedOptions, o => o.value))}
                    className="w-full border rounded text-sm h-32"
                  >
                    {filterOptions?.states?.map(s => <option key={s} value={s}>{s}</option>)}
                  </select>
                </div>
              </div>

              {activeFilterCount > 0 && (
                <button
                  onClick={() => { setPlanTypes([]); setProductTypes([]); setSnpTypes([]); setGroupTypes([]); setStates([]); }}
                  className="mt-3 text-sm text-red-600 hover:text-red-800"
                >
                  Clear all filters
                </button>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Summary Cards */}
      <div className="max-w-7xl mx-auto px-4 py-6">
        <div className="grid grid-cols-3 gap-4 mb-6">
          <div className="bg-white rounded-lg shadow p-4">
            <div className="text-sm text-gray-500">Total Enrollment ({endYear})</div>
            <div className="text-2xl font-bold text-blue-600">{formatNumber(latestEnrollment)}</div>
          </div>
          <div className="bg-white rounded-lg shadow p-4">
            <div className="text-sm text-gray-500">4+ Star Enrollment %</div>
            <div className="text-2xl font-bold text-yellow-600">{formatPercent(latestStars)}</div>
          </div>
          <div className="bg-white rounded-lg shadow p-4">
            <div className="text-sm text-gray-500">Avg Risk Score</div>
            <div className="text-2xl font-bold text-green-600">{formatRisk(latestRisk)}</div>
          </div>
        </div>

        {/* Charts - 3 across */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          
          {/* ENROLLMENT CHART */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold mb-4">Enrollment Over Time</h2>
            <div style={{ width: '100%', height: 256, minHeight: 200 }}>
              {!isMounted || enrollmentLoading ? (
                <div className="flex items-center justify-center h-full">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
                </div>
              ) : enrollmentData?.error ? (
                <div className="flex items-center justify-center h-full text-red-500 text-sm">
                  {enrollmentData.error}
                </div>
              ) : (
                <ResponsiveContainer width="100%" height={240} minHeight={200}>
                  <LineChart data={enrollmentChartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="year" />
                    <YAxis tickFormatter={formatNumber} />
                    <Tooltip formatter={(v) => [formatNumber(v as number), "Enrollment"]} />
                    <Line type="monotone" dataKey="enrollment" stroke="#2563eb" strokeWidth={2} dot={{ r: 3 }} />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>

          {/* STARS CHART */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold mb-4">4+ Star % Over Time</h2>
            <div style={{ width: '100%', height: 256, minHeight: 200 }}>
              {!isMounted || starsLoading ? (
                <div className="flex items-center justify-center h-full">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-yellow-600" />
                </div>
              ) : starsData?.error ? (
                <div className="flex items-center justify-center h-full text-red-500 text-sm">
                  {starsData.error}
                </div>
              ) : (
                <ResponsiveContainer width="100%" height={240} minHeight={200}>
                  <LineChart data={starsChartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="year" />
                    <YAxis tickFormatter={(v) => `${v}%`} domain={[0, 100]} />
                    <Tooltip formatter={(v) => [`${(v as number).toFixed(1)}%`, "4+ Star %"]} />
                    <Line type="monotone" dataKey="pct_fourplus" stroke="#eab308" strokeWidth={2} dot={{ r: 3 }} />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>

          {/* RISK CHART */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold mb-4">Risk Score Over Time</h2>
            <div style={{ width: '100%', height: 256, minHeight: 200 }}>
              {!isMounted || riskLoading ? (
                <div className="flex items-center justify-center h-full">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-green-600" />
                </div>
              ) : riskData?.error ? (
                <div className="flex items-center justify-center h-full text-red-500 text-sm">
                  {riskData.error}
                </div>
              ) : (
                <ResponsiveContainer width="100%" height={240} minHeight={200}>
                  <LineChart data={riskChartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="year" />
                    <YAxis tickFormatter={formatRisk} domain={['auto', 'auto']} />
                    <Tooltip formatter={(v) => [formatRisk(v as number), "Risk Score"]} />
                    <Line type="monotone" dataKey="risk" stroke="#16a34a" strokeWidth={2} dot={{ r: 3 }} />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>

        </div>

        {/* Data Tables - Below charts */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mt-6">
          
          {/* ENROLLMENT TABLE */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold mb-4">Enrollment by Year</h2>
            <div className="overflow-x-auto max-h-64 overflow-y-auto">
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-white">
                  <tr className="border-b bg-gray-50">
                    <th className="text-left py-2 px-3">Year</th>
                    <th className="text-right py-2 px-3">Enrollment</th>
                    <th className="text-right py-2 px-3">Contracts</th>
                  </tr>
                </thead>
                <tbody>
                  {enrollmentChartData.length > 0 ? (
                    [...enrollmentChartData].reverse().map((row: any) => (
                      <tr key={row.year} className="border-b hover:bg-gray-50">
                        <td className="py-2 px-3 font-medium">{row.year}</td>
                        <td className="text-right py-2 px-3">{formatNumber(row.enrollment)}</td>
                        <td className="text-right py-2 px-3">{enrollmentData?.contract_count?.[enrollmentData.years.indexOf(row.year)]?.toLocaleString() || "-"}</td>
                      </tr>
                    ))
                  ) : (
                    <tr><td colSpan={3} className="py-4 text-center text-gray-500">No data</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {/* STARS TABLE */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold mb-4">4+ Star % by Year</h2>
            <div className="overflow-x-auto max-h-64 overflow-y-auto">
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-white">
                  <tr className="border-b bg-gray-50">
                    <th className="text-left py-2 px-3">Year</th>
                    <th className="text-right py-2 px-3">4+ Star %</th>
                    <th className="text-right py-2 px-3">Enrollment</th>
                  </tr>
                </thead>
                <tbody>
                  {starsChartData.length > 0 ? (
                    [...starsChartData].reverse().map((row: any, i: number) => (
                      <tr key={row.year} className="border-b hover:bg-gray-50">
                        <td className="py-2 px-3 font-medium">{row.year}</td>
                        <td className="text-right py-2 px-3">{formatPercent(row.pct_fourplus)}</td>
                        <td className="text-right py-2 px-3">{formatNumber(starsData?.total_enrollment?.[starsData.years.indexOf(row.year)])}</td>
                      </tr>
                    ))
                  ) : (
                    <tr><td colSpan={3} className="py-4 text-center text-gray-500">No data</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {/* RISK TABLE */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold mb-4">Risk Score by Year</h2>
            <div className="overflow-x-auto max-h-64 overflow-y-auto">
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-white">
                  <tr className="border-b bg-gray-50">
                    <th className="text-left py-2 px-3">Year</th>
                    <th className="text-right py-2 px-3">Avg Risk</th>
                    <th className="text-right py-2 px-3">Enrollment</th>
                  </tr>
                </thead>
                <tbody>
                  {riskChartData.length > 0 ? (
                    [...riskChartData].reverse().map((row: any, i: number) => (
                      <tr key={row.year} className="border-b hover:bg-gray-50">
                        <td className="py-2 px-3 font-medium">{row.year}</td>
                        <td className="text-right py-2 px-3">{formatRisk(row.risk)}</td>
                        <td className="text-right py-2 px-3">{formatNumber(riskData?.total_enrollment?.[riskData.years.indexOf(row.year)])}</td>
                      </tr>
                    ))
                  ) : (
                    <tr><td colSpan={3} className="py-4 text-center text-gray-500">No data</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

        </div>

        {/* Audit Info */}
        {(enrollmentData?.audit_id || starsData?.audit_id || riskData?.audit_id) && (
          <div className="mt-4 text-xs text-gray-400 text-right">
            Query IDs: {[enrollmentData?.audit_id, starsData?.audit_id, riskData?.audit_id].filter(Boolean).join(", ")}
          </div>
        )}
      </div>

      {/* Click outside to close dropdowns */}
      {showPayerDropdown && (
        <div className="fixed inset-0 z-10" onClick={() => setShowPayerDropdown(false)} />
      )}
    </div>
  );
}
