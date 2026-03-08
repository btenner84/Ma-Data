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
  parent_orgs: string[];
  product_types: string[];
  snp_types: string[];
  group_types: string[];
  states: string[];
  years: number[];
}

export default function SummaryPage() {
  const isMounted = useIsMounted();
  
  // Selected entity (null = Industry)
  const [selectedPayer, setSelectedPayer] = useState<string | null>(null);
  const [payerSearch, setPayerSearch] = useState("");
  const [showPayerDropdown, setShowPayerDropdown] = useState(false);
  
  // Filters
  const [productTypes, setProductTypes] = useState<string[]>([]);
  const [snpTypes, setSnpTypes] = useState<string[]>([]);
  const [groupTypes, setGroupTypes] = useState<string[]>([]);
  const [states, setStates] = useState<string[]>([]);
  const [startYear, setStartYear] = useState<number>(2017);
  const [endYear, setEndYear] = useState<number>(2026);
  
  // Filter popup
  const [showFilters, setShowFilters] = useState(false);
  
  // Table year toggles
  const [starsYear, setStarsYear] = useState<number>(2026);
  const [enrollmentYear, setEnrollmentYear] = useState<number>(2026);
  const [riskYear, setRiskYear] = useState<number>(2024); // Risk data typically lags

  // Fetch filter options
  const { data: filterOptions } = useQuery<FilterOptions>({
    queryKey: ["summary-filters"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/v3/enrollment/filters`);
      const data = await res.json();
      return {
        parent_orgs: data.parent_orgs || [],
        product_types: data.product_types || [],
        snp_types: data.snp_types || [],
        group_types: data.group_types || [],
        states: data.states || [],
        years: data.years || [],
      };
    },
  });

  // Build enrollment query params
  const buildEnrollmentParams = () => {
    const params = new URLSearchParams();
    if (selectedPayer) params.append("parent_orgs", selectedPayer);
    if (productTypes.length) params.append("product_types", productTypes.join(","));
    if (snpTypes.length) params.append("snp_types", snpTypes.join(","));
    if (groupTypes.length) params.append("group_types", groupTypes.join(","));
    if (states.length) params.append("states", states.join(","));
    params.append("start_year", startYear.toString());
    params.append("end_year", endYear.toString());
    params.append("data_source", "national");
    params.append("include_total", "false");
    return params.toString();
  };

  // ========== ENROLLMENT DATA ==========
  const { data: enrollmentTimeseries, isLoading: enrollmentLoading } = useQuery({
    queryKey: ["summary-enrollment-ts", selectedPayer, productTypes, snpTypes, groupTypes, states, startYear, endYear],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/v3/enrollment/timeseries?${buildEnrollmentParams()}`);
      return res.json();
    },
  });

  // Enrollment by plan type - use risk dimensions which has plan_type breakdown with enrollment
  const { data: enrollmentByPlanType } = useQuery({
    queryKey: ["summary-enrollment-plantype", selectedPayer, states, enrollmentYear],
    queryFn: async () => {
      const params = new URLSearchParams();
      params.append("year", enrollmentYear.toString());
      if (selectedPayer) params.append("parent_orgs", selectedPayer);
      if (states.length) params.append("states", states.join(","));
      const res = await fetch(`${API_BASE}/api/v3/risk/by-dimensions?${params.toString()}`);
      const data = await res.json();
      // Aggregate by plan_type
      if (!data.data) return { data: [] };
      const byPlanType: Record<string, number> = {};
      data.data.forEach((row: any) => {
        const pt = row.plan_type || "Unknown";
        byPlanType[pt] = (byPlanType[pt] || 0) + (row.enrollment || 0);
      });
      return {
        data: Object.entries(byPlanType)
          .map(([plan_type, enrollment]) => ({ plan_type, total_enrollment: enrollment }))
          .sort((a, b) => b.total_enrollment - a.total_enrollment)
      };
    },
  });

  // ========== STARS DATA ==========
  const { data: starsTimeseries, isLoading: starsLoading } = useQuery({
    queryKey: ["summary-stars-ts", selectedPayer, productTypes, snpTypes, groupTypes, startYear, endYear],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (selectedPayer) params.append("parent_orgs", selectedPayer);
      if (productTypes.length) params.append("plan_types", productTypes.join(","));
      if (snpTypes.length) params.append("snp_types", snpTypes.join(","));
      if (groupTypes.length) params.append("group_types", groupTypes.join(","));
      params.append("include_total", "true");
      const res = await fetch(`${API_BASE}/api/stars/fourplus-timeseries?${params.toString()}`);
      return res.json();
    },
  });

  const { data: starsDistribution } = useQuery({
    queryKey: ["summary-stars-dist", selectedPayer, starsYear],
    queryFn: async () => {
      const params = new URLSearchParams();
      params.append("star_year", starsYear.toString());
      if (selectedPayer) params.append("parent_orgs", selectedPayer);
      const res = await fetch(`${API_BASE}/api/stars/distribution?${params.toString()}`);
      return res.json();
    },
  });

  // ========== RISK DATA ==========
  const { data: riskTimeseries, isLoading: riskLoading } = useQuery({
    queryKey: ["summary-risk-ts", selectedPayer, productTypes, snpTypes, groupTypes, startYear, endYear],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (selectedPayer) params.append("parent_orgs", selectedPayer);
      if (productTypes.length) params.append("plan_types", productTypes.join(","));
      if (snpTypes.length) params.append("snp_types", snpTypes.join(","));
      if (groupTypes.length) params.append("group_types", groupTypes.join(","));
      params.append("include_industry_total", "true");
      const res = await fetch(`${API_BASE}/api/v3/risk/timeseries?${params.toString()}`);
      return res.json();
    },
  });

  const { data: riskByProduct } = useQuery({
    queryKey: ["summary-risk-product", selectedPayer, snpTypes, groupTypes, riskYear],
    queryFn: async () => {
      const params = new URLSearchParams();
      params.append("year", riskYear.toString());
      if (selectedPayer) params.append("parent_orgs", selectedPayer);
      const res = await fetch(`${API_BASE}/api/v3/risk/by-dimensions?${params.toString()}`);
      return res.json();
    },
  });

  // Transform enrollment data for chart
  const enrollmentChartData = enrollmentTimeseries?.years?.map((year: number, i: number) => {
    // If series exists (payer selected), use that
    if (enrollmentTimeseries.series) {
      const seriesKeys = Object.keys(enrollmentTimeseries.series);
      const key = seriesKeys[0];
      return { year, enrollment: enrollmentTimeseries.series[key]?.[i] || 0 };
    }
    // Otherwise use total_enrollment
    return { year, enrollment: enrollmentTimeseries.total_enrollment?.[i] || 0 };
  }) || [];

  // Transform stars data for chart
  const starsChartData = starsTimeseries?.years?.map((year: number, i: number) => {
    if (starsTimeseries.series) {
      // If payer selected, use that; otherwise use "Industry" total
      const key = selectedPayer || "Industry";
      const value = starsTimeseries.series[key]?.[i];
      return { year, fourplus: value };
    }
    return { year, fourplus: null };
  }).filter((d: any) => d.fourplus !== null && d.fourplus !== undefined) || [];

  // Transform risk data for chart
  const riskChartData = riskTimeseries?.years?.map((year: number, i: number) => {
    if (riskTimeseries.series) {
      // If payer selected, use that; otherwise use "Industry" total
      const key = selectedPayer || "Industry";
      const value = riskTimeseries.series[key]?.[i];
      return { year, risk: value };
    }
    // Fallback for older API format
    return { year, risk: riskTimeseries.wavg?.[i] || riskTimeseries.avg_risk?.[i] };
  }).filter((d: any) => d.risk !== null && d.risk !== undefined) || [];

  // Filter payers for dropdown
  const filteredPayers = filterOptions?.parent_orgs?.filter(
    p => p.toLowerCase().includes(payerSearch.toLowerCase())
  ).slice(0, 20) || [];

  const displayName = selectedPayer || "Industry";
  const activeFilterCount = productTypes.length + snpTypes.length + groupTypes.length + states.length;

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
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {/* Product Type */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Product Type</label>
                  <div className="space-y-1">
                    {['MAPD', 'MA-only', 'PDP'].map(pt => (
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
                    {['Non-SNP', 'D-SNP', 'C-SNP', 'I-SNP'].map(st => (
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
                    {['Individual', 'Group'].map(gt => (
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
                    className="w-full border rounded text-sm h-24"
                  >
                    {filterOptions?.states?.map(s => <option key={s} value={s}>{s}</option>)}
                  </select>
                </div>
              </div>

              {activeFilterCount > 0 && (
                <button
                  onClick={() => { setProductTypes([]); setSnpTypes([]); setGroupTypes([]); setStates([]); }}
                  className="mt-3 text-sm text-red-600 hover:text-red-800"
                >
                  Clear all filters
                </button>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Main Content Grid */}
      <div className="max-w-7xl mx-auto px-4 py-6">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          
          {/* ENROLLMENT OVER TIME */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold mb-4">Enrollment Over Time</h2>
            <div style={{ width: '100%', height: 256, minHeight: 200 }}>
              {!isMounted || enrollmentLoading ? (
                <div className="flex items-center justify-center h-full">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
                </div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
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

          {/* ENROLLMENT BY PLAN TYPE */}
          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold">Enrollment by Plan Type</h2>
              <select
                value={enrollmentYear}
                onChange={(e) => setEnrollmentYear(Number(e.target.value))}
                className="border rounded px-2 py-1 text-sm"
              >
                {filterOptions?.years?.slice(-10).reverse().map(y => (
                  <option key={y} value={y}>{y}</option>
                ))}
              </select>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-gray-50">
                    <th className="text-left py-2 px-3">Plan Type</th>
                    <th className="text-right py-2 px-3">Enrollment</th>
                    <th className="text-right py-2 px-3">Share</th>
                  </tr>
                </thead>
                <tbody>
                  {(() => {
                    const data = enrollmentByPlanType?.data;
                    if (!data || data.length === 0) return (
                      <tr><td colSpan={3} className="py-4 text-center text-gray-500">No data</td></tr>
                    );
                    const total = data.reduce((sum: number, r: any) => sum + (r.total_enrollment || 0), 0);
                    return data.map((row: any, i: number) => (
                      <tr key={i} className="border-b hover:bg-gray-50">
                        <td className="py-2 px-3 font-medium">{row.plan_type}</td>
                        <td className="text-right py-2 px-3">{formatNumber(row.total_enrollment)}</td>
                        <td className="text-right py-2 px-3">{formatPercent(total > 0 ? (row.total_enrollment / total) * 100 : 0)}</td>
                      </tr>
                    ));
                  })()}
                </tbody>
              </table>
            </div>
          </div>

          {/* 4+ STAR % OVER TIME */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold mb-4">4+ Star Enrollment % Over Time</h2>
            <div style={{ width: '100%', height: 256, minHeight: 200 }}>
              {!isMounted || starsLoading ? (
                <div className="flex items-center justify-center h-full">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-yellow-600" />
                </div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={starsChartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="year" />
                    <YAxis tickFormatter={(v) => `${v}%`} domain={[0, 100]} />
                    <Tooltip formatter={(v) => [`${(v as number).toFixed(1)}%`, "4+ Star %"]} />
                    <Line type="monotone" dataKey="fourplus" stroke="#eab308" strokeWidth={2} dot={{ r: 3 }} />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>

          {/* STARS DISTRIBUTION */}
          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold">Star Rating Distribution</h2>
              <select
                value={starsYear}
                onChange={(e) => setStarsYear(Number(e.target.value))}
                className="border rounded px-2 py-1 text-sm"
              >
                {filterOptions?.years?.slice(-10).reverse().map(y => (
                  <option key={y} value={y}>{y}</option>
                ))}
              </select>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-gray-50">
                    <th className="text-left py-2 px-3">Rating</th>
                    <th className="text-right py-2 px-3">Contracts</th>
                    <th className="text-right py-2 px-3">Enrollment</th>
                    <th className="text-right py-2 px-3">Share</th>
                  </tr>
                </thead>
                <tbody>
                  {(() => {
                    const column = selectedPayer 
                      ? starsDistribution?.columns?.[selectedPayer] 
                      : starsDistribution?.columns?.Industry;
                    const distribution = column?.distribution;
                    if (!distribution || Object.keys(distribution).length === 0) return (
                      <tr><td colSpan={4} className="py-4 text-center text-gray-500">No data</td></tr>
                    );
                    const ratings = Object.keys(distribution).map(Number).filter(n => !isNaN(n)).sort((a, b) => b - a);
                    if (ratings.length === 0) return (
                      <tr><td colSpan={4} className="py-4 text-center text-gray-500">No data</td></tr>
                    );
                    return ratings.map(rating => {
                      const data = distribution[rating] || {};
                      return (
                        <tr key={rating} className="border-b hover:bg-gray-50">
                          <td className="py-2 px-3 font-medium">{rating} Stars</td>
                          <td className="text-right py-2 px-3">{data.contracts ? data.contracts.toLocaleString() : "-"}</td>
                          <td className="text-right py-2 px-3">{formatNumber(data.enrollment)}</td>
                          <td className="text-right py-2 px-3">{formatPercent(data.pct)}</td>
                        </tr>
                      );
                    });
                  })()}
                </tbody>
              </table>
            </div>
          </div>

          {/* RISK SCORE OVER TIME */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold mb-4">Risk Score Over Time</h2>
            <div style={{ width: '100%', height: 256, minHeight: 200 }}>
              {!isMounted || riskLoading ? (
                <div className="flex items-center justify-center h-full">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-green-600" />
                </div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
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

          {/* RISK BY PLAN TYPE */}
          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold">Risk Score by Plan Type</h2>
              <select
                value={riskYear}
                onChange={(e) => setRiskYear(Number(e.target.value))}
                className="border rounded px-2 py-1 text-sm"
              >
                {[2024, 2023, 2022, 2021, 2020, 2019, 2018].map(y => (
                  <option key={y} value={y}>{y}</option>
                ))}
              </select>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-gray-50">
                    <th className="text-left py-2 px-3">Plan Type</th>
                    <th className="text-right py-2 px-3">Avg Risk</th>
                    <th className="text-right py-2 px-3">Enrollment</th>
                  </tr>
                </thead>
                <tbody>
                  {(() => {
                    const data = riskByProduct?.data;
                    if (!data || data.length === 0) return (
                      <tr><td colSpan={3} className="py-4 text-center text-gray-500">No data</td></tr>
                    );
                    // Aggregate by plan_type
                    const byPlanType: Record<string, { enrollment: number, weightedSum: number }> = {};
                    data.forEach((row: any) => {
                      const pt = row.plan_type || "Unknown";
                      if (!byPlanType[pt]) byPlanType[pt] = { enrollment: 0, weightedSum: 0 };
                      const enr = row.enrollment || 0;
                      const wavg = row.wavg || row.simple_avg || 0;
                      byPlanType[pt].enrollment += enr;
                      byPlanType[pt].weightedSum += enr * wavg;
                    });
                    const rows = Object.entries(byPlanType)
                      .map(([pt, d]) => ({
                        plan_type: pt,
                        enrollment: d.enrollment,
                        wavg: d.enrollment > 0 ? d.weightedSum / d.enrollment : 0
                      }))
                      .sort((a, b) => b.enrollment - a.enrollment);
                    return rows.map((row, i) => (
                      <tr key={i} className="border-b hover:bg-gray-50">
                        <td className="py-2 px-3 font-medium">{row.plan_type}</td>
                        <td className="text-right py-2 px-3">{formatRisk(row.wavg)}</td>
                        <td className="text-right py-2 px-3">{formatNumber(row.enrollment)}</td>
                      </tr>
                    ));
                  })()}
                </tbody>
              </table>
            </div>
          </div>

        </div>
      </div>

      {/* Click outside to close dropdowns */}
      {showPayerDropdown && (
        <div className="fixed inset-0 z-10" onClick={() => setShowPayerDropdown(false)} />
      )}
    </div>
  );
}
