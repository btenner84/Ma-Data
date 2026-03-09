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
  BarChart,
  Bar,
  Cell,
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

interface FilterOptions {
  years: number[];
  parent_orgs: string[];
  plan_types: string[];
  product_types: string[];
  snp_types: string[];
  group_types: string[];
  states: string[];
}

const COLORS = {
  blue: "#2563eb",
  green: "#16a34a", 
  yellow: "#eab308",
  purple: "#9333ea",
  red: "#dc2626",
  orange: "#ea580c",
  teal: "#0d9488",
  pink: "#db2777",
};

const BREAKDOWN_COLORS = [COLORS.blue, COLORS.green, COLORS.yellow, COLORS.purple, COLORS.red, COLORS.orange];

export default function SummaryPage() {
  const isMounted = useIsMounted();
  
  // ============ ENROLLMENT SECTION FILTERS ============
  const [enrollPayer, setEnrollPayer] = useState<string | null>(null);
  const [enrollPayerSearch, setEnrollPayerSearch] = useState("");
  const [showEnrollPayerDropdown, setShowEnrollPayerDropdown] = useState(false);
  const [enrollPlanTypes, setEnrollPlanTypes] = useState<string[]>([]);
  const [enrollProductTypes, setEnrollProductTypes] = useState<string[]>([]);
  const [enrollSnpTypes, setEnrollSnpTypes] = useState<string[]>([]);
  const [enrollGroupTypes, setEnrollGroupTypes] = useState<string[]>([]);
  const [enrollStates, setEnrollStates] = useState<string[]>([]);
  const [enrollStartYear, setEnrollStartYear] = useState<number>(2015);
  const [enrollEndYear, setEnrollEndYear] = useState<number>(2026);
  const [showEnrollFilters, setShowEnrollFilters] = useState(false);
  
  // Year for breakdown panels (single year)
  const [breakdownYear, setBreakdownYear] = useState<number>(2026);

  // ========== FETCH FILTER OPTIONS ==========
  const { data: filterOptions } = useQuery<FilterOptions>({
    queryKey: ["v5-filters"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/v5/filters`);
      return res.json();
    },
  });

  // ========== ENROLLMENT TIMESERIES (for chart) ==========
  const buildEnrollParams = () => {
    const params = new URLSearchParams();
    if (enrollPayer) params.append("parent_org", enrollPayer);
    if (enrollPlanTypes.length) params.append("plan_types", enrollPlanTypes.join(","));
    if (enrollProductTypes.length) params.append("product_types", enrollProductTypes.join(","));
    if (enrollSnpTypes.length) params.append("snp_types", enrollSnpTypes.join(","));
    if (enrollGroupTypes.length) params.append("group_types", enrollGroupTypes.join(","));
    if (enrollStates.length) params.append("states", enrollStates.join(","));
    params.append("start_year", enrollStartYear.toString());
    params.append("end_year", enrollEndYear.toString());
    return params.toString();
  };

  const { data: enrollmentData, isLoading: enrollmentLoading } = useQuery({
    queryKey: ["enrollment-chart", enrollPayer, enrollPlanTypes, enrollProductTypes, enrollSnpTypes, enrollGroupTypes, enrollStates, enrollStartYear, enrollEndYear],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/v5/enrollment/timeseries?${buildEnrollParams()}`);
      return res.json();
    },
  });

  // ========== BREAKDOWN QUERIES (for single year) ==========
  // Product Type breakdown
  const { data: productBreakdown } = useQuery({
    queryKey: ["breakdown-product", breakdownYear],
    queryFn: async () => {
      const results = await Promise.all([
        fetch(`${API_BASE}/api/v5/enrollment/timeseries?start_year=${breakdownYear}&end_year=${breakdownYear}&product_types=MAPD`).then(r => r.json()),
        fetch(`${API_BASE}/api/v5/enrollment/timeseries?start_year=${breakdownYear}&end_year=${breakdownYear}&product_types=PDP`).then(r => r.json()),
      ]);
      return [
        { name: "MAPD", value: results[0]?.enrollment?.[0] || 0 },
        { name: "PDP", value: results[1]?.enrollment?.[0] || 0 },
      ];
    },
  });

  // Group Type breakdown
  const { data: groupBreakdown } = useQuery({
    queryKey: ["breakdown-group", breakdownYear],
    queryFn: async () => {
      const results = await Promise.all([
        fetch(`${API_BASE}/api/v5/enrollment/timeseries?start_year=${breakdownYear}&end_year=${breakdownYear}&group_types=Individual`).then(r => r.json()),
        fetch(`${API_BASE}/api/v5/enrollment/timeseries?start_year=${breakdownYear}&end_year=${breakdownYear}&group_types=Group`).then(r => r.json()),
      ]);
      return [
        { name: "Individual", value: results[0]?.enrollment?.[0] || 0 },
        { name: "Group", value: results[1]?.enrollment?.[0] || 0 },
      ];
    },
  });

  // SNP Type breakdown
  const { data: snpBreakdown } = useQuery({
    queryKey: ["breakdown-snp", breakdownYear],
    queryFn: async () => {
      const results = await Promise.all([
        fetch(`${API_BASE}/api/v5/enrollment/timeseries?start_year=${breakdownYear}&end_year=${breakdownYear}&snp_types=Non-SNP`).then(r => r.json()),
        fetch(`${API_BASE}/api/v5/enrollment/timeseries?start_year=${breakdownYear}&end_year=${breakdownYear}&snp_types=D-SNP`).then(r => r.json()),
        fetch(`${API_BASE}/api/v5/enrollment/timeseries?start_year=${breakdownYear}&end_year=${breakdownYear}&snp_types=C-SNP`).then(r => r.json()),
        fetch(`${API_BASE}/api/v5/enrollment/timeseries?start_year=${breakdownYear}&end_year=${breakdownYear}&snp_types=I-SNP`).then(r => r.json()),
      ]);
      return [
        { name: "Non-SNP", value: results[0]?.enrollment?.[0] || 0 },
        { name: "D-SNP", value: results[1]?.enrollment?.[0] || 0 },
        { name: "C-SNP", value: results[2]?.enrollment?.[0] || 0 },
        { name: "I-SNP", value: results[3]?.enrollment?.[0] || 0 },
      ];
    },
  });

  // Plan Type breakdown
  const { data: planBreakdown } = useQuery({
    queryKey: ["breakdown-plan", breakdownYear],
    queryFn: async () => {
      const results = await Promise.all([
        fetch(`${API_BASE}/api/v5/enrollment/timeseries?start_year=${breakdownYear}&end_year=${breakdownYear}&plan_types=HMO`).then(r => r.json()),
        fetch(`${API_BASE}/api/v5/enrollment/timeseries?start_year=${breakdownYear}&end_year=${breakdownYear}&plan_types=PPO`).then(r => r.json()),
        fetch(`${API_BASE}/api/v5/enrollment/timeseries?start_year=${breakdownYear}&end_year=${breakdownYear}&plan_types=PFFS`).then(r => r.json()),
        fetch(`${API_BASE}/api/v5/enrollment/timeseries?start_year=${breakdownYear}&end_year=${breakdownYear}&plan_types=Cost`).then(r => r.json()),
      ]);
      return [
        { name: "HMO", value: results[0]?.enrollment?.[0] || 0 },
        { name: "PPO", value: results[1]?.enrollment?.[0] || 0 },
        { name: "PFFS", value: results[2]?.enrollment?.[0] || 0 },
        { name: "Cost", value: results[3]?.enrollment?.[0] || 0 },
      ].filter(x => x.value > 0);
    },
  });

  // Transform enrollment data for chart
  const enrollmentChartData = enrollmentData?.years?.map((year: number, i: number) => ({
    year,
    enrollment: enrollmentData.enrollment?.[i] || 0,
  })) || [];

  // Filter payers for dropdown
  const filteredPayers = filterOptions?.parent_orgs?.filter(
    p => p?.toLowerCase().includes(enrollPayerSearch.toLowerCase())
  ).slice(0, 20) || [];

  const enrollDisplayName = enrollPayer || "Industry (All)";
  const enrollActiveFilterCount = enrollPlanTypes.length + enrollProductTypes.length + enrollSnpTypes.length + enrollGroupTypes.length + enrollStates.length;

  // Get latest enrollment
  const latestEnrollment = enrollmentData?.enrollment?.[enrollmentData.enrollment.length - 1] || 0;

  // Calculate totals for breakdown panels
  const totalBreakdown = (data: {name: string, value: number}[] | undefined) => {
    return data?.reduce((sum, item) => sum + item.value, 0) || 0;
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Page Title */}
      <div className="bg-white border-b">
        <div className="max-w-[1800px] mx-auto px-6 py-4">
          <h1 className="text-2xl font-bold text-gray-900">MA Market Summary</h1>
        </div>
      </div>

      {/* ENROLLMENT SECTION */}
      <div className="max-w-[1800px] mx-auto px-6 py-6">
        <div className="bg-white rounded-xl shadow-sm border">
          {/* Section Header with Filters */}
          <div className="border-b px-6 py-4">
            <div className="flex items-center justify-between flex-wrap gap-4">
              <h2 className="text-xl font-semibold text-gray-900">Enrollment</h2>
              
              <div className="flex items-center gap-3 flex-wrap">
                {/* Payer Selector */}
                <div className="relative">
                  <button
                    onClick={() => setShowEnrollPayerDropdown(!showEnrollPayerDropdown)}
                    className="flex items-center gap-2 px-3 py-1.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm font-medium min-w-[160px]"
                  >
                    <span className="truncate">{enrollDisplayName}</span>
                    <ChevronDown className="w-4 h-4 flex-shrink-0" />
                  </button>
                  
                  {showEnrollPayerDropdown && (
                    <div className="absolute top-full left-0 mt-1 w-72 bg-white rounded-lg shadow-xl border z-30">
                      <div className="p-2 border-b">
                        <div className="relative">
                          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                          <input
                            type="text"
                            placeholder="Search payer..."
                            value={enrollPayerSearch}
                            onChange={(e) => setEnrollPayerSearch(e.target.value)}
                            className="w-full pl-9 pr-3 py-2 border rounded-lg text-sm"
                            autoFocus
                          />
                        </div>
                      </div>
                      <div className="max-h-64 overflow-y-auto">
                        <button
                          onClick={() => { setEnrollPayer(null); setShowEnrollPayerDropdown(false); setEnrollPayerSearch(""); }}
                          className={`w-full text-left px-4 py-2 hover:bg-gray-100 text-sm ${!enrollPayer ? 'bg-blue-50 text-blue-700 font-medium' : ''}`}
                        >
                          Industry (All)
                        </button>
                        {filteredPayers.map(payer => (
                          <button
                            key={payer}
                            onClick={() => { setEnrollPayer(payer); setShowEnrollPayerDropdown(false); setEnrollPayerSearch(""); }}
                            className={`w-full text-left px-4 py-2 hover:bg-gray-100 text-sm ${enrollPayer === payer ? 'bg-blue-50 text-blue-700 font-medium' : ''}`}
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
                  onClick={() => setShowEnrollFilters(!showEnrollFilters)}
                  className={`flex items-center gap-2 px-3 py-1.5 rounded-lg border text-sm ${enrollActiveFilterCount > 0 ? 'bg-blue-50 border-blue-300 text-blue-700' : 'bg-white hover:bg-gray-50'}`}
                >
                  <Filter className="w-4 h-4" />
                  <span>Filters</span>
                  {enrollActiveFilterCount > 0 && (
                    <span className="bg-blue-600 text-white text-xs px-1.5 py-0.5 rounded-full">{enrollActiveFilterCount}</span>
                  )}
                </button>

                {/* Year Range for Chart */}
                <div className="flex items-center gap-2 text-sm">
                  <select
                    value={enrollStartYear}
                    onChange={(e) => setEnrollStartYear(Number(e.target.value))}
                    className="border rounded px-2 py-1 text-sm"
                  >
                    {filterOptions?.years?.filter(y => y >= 2013 && y <= 2026).map(y => <option key={y} value={y}>{y}</option>)}
                  </select>
                  <span className="text-gray-500">to</span>
                  <select
                    value={enrollEndYear}
                    onChange={(e) => setEnrollEndYear(Number(e.target.value))}
                    className="border rounded px-2 py-1 text-sm"
                  >
                    {filterOptions?.years?.filter(y => y >= 2013 && y <= 2026).map(y => <option key={y} value={y}>{y}</option>)}
                  </select>
                </div>
              </div>
            </div>

            {/* Expanded Filter Panel */}
            {showEnrollFilters && (
              <div className="mt-4 p-4 bg-gray-50 rounded-lg border">
                <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                  {/* Plan Type */}
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">Plan Type</label>
                    <div className="space-y-1 max-h-24 overflow-y-auto">
                      {['HMO', 'PPO', 'PFFS', 'MSA', 'Cost'].map(pt => (
                        <label key={pt} className="flex items-center gap-2 text-sm">
                          <input
                            type="checkbox"
                            checked={enrollPlanTypes.includes(pt)}
                            onChange={(e) => {
                              if (e.target.checked) setEnrollPlanTypes([...enrollPlanTypes, pt]);
                              else setEnrollPlanTypes(enrollPlanTypes.filter(x => x !== pt));
                            }}
                          />
                          {pt}
                        </label>
                      ))}
                    </div>
                  </div>

                  {/* Product Type */}
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">Product Type</label>
                    <div className="space-y-1">
                      {['MAPD', 'PDP'].map(pt => (
                        <label key={pt} className="flex items-center gap-2 text-sm">
                          <input
                            type="checkbox"
                            checked={enrollProductTypes.includes(pt)}
                            onChange={(e) => {
                              if (e.target.checked) setEnrollProductTypes([...enrollProductTypes, pt]);
                              else setEnrollProductTypes(enrollProductTypes.filter(x => x !== pt));
                            }}
                          />
                          {pt}
                        </label>
                      ))}
                    </div>
                  </div>

                  {/* SNP Type */}
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">SNP Type</label>
                    <div className="space-y-1">
                      {['Non-SNP', 'D-SNP', 'C-SNP', 'I-SNP'].map(st => (
                        <label key={st} className="flex items-center gap-2 text-sm">
                          <input
                            type="checkbox"
                            checked={enrollSnpTypes.includes(st)}
                            onChange={(e) => {
                              if (e.target.checked) setEnrollSnpTypes([...enrollSnpTypes, st]);
                              else setEnrollSnpTypes(enrollSnpTypes.filter(x => x !== st));
                            }}
                          />
                          {st}
                        </label>
                      ))}
                    </div>
                  </div>

                  {/* Group Type */}
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">Market Segment</label>
                    <div className="space-y-1">
                      {['Individual', 'Group'].map(gt => (
                        <label key={gt} className="flex items-center gap-2 text-sm">
                          <input
                            type="checkbox"
                            checked={enrollGroupTypes.includes(gt)}
                            onChange={(e) => {
                              if (e.target.checked) setEnrollGroupTypes([...enrollGroupTypes, gt]);
                              else setEnrollGroupTypes(enrollGroupTypes.filter(x => x !== gt));
                            }}
                          />
                          {gt}
                        </label>
                      ))}
                    </div>
                  </div>

                  {/* State */}
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">State</label>
                    <select
                      multiple
                      value={enrollStates}
                      onChange={(e) => setEnrollStates(Array.from(e.target.selectedOptions, o => o.value))}
                      className="w-full border rounded text-sm h-24"
                    >
                      {filterOptions?.states?.map(s => <option key={s} value={s}>{s}</option>)}
                    </select>
                  </div>
                </div>

                {enrollActiveFilterCount > 0 && (
                  <button
                    onClick={() => { setEnrollPlanTypes([]); setEnrollProductTypes([]); setEnrollSnpTypes([]); setEnrollGroupTypes([]); setEnrollStates([]); }}
                    className="mt-3 text-sm text-red-600 hover:text-red-800"
                  >
                    Clear all filters
                  </button>
                )}
              </div>
            )}
          </div>

          {/* Main Content: 50/50 Split */}
          <div className="flex flex-col lg:flex-row" style={{ minHeight: 500 }}>
            
            {/* LEFT: Enrollment Chart (50%) */}
            <div className="lg:w-1/2 p-6 border-r flex flex-col">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold text-gray-800">Enrollment Over Time</h3>
                <div className="text-2xl font-bold text-blue-600">{formatNumber(latestEnrollment)}</div>
              </div>
              
              <div className="flex-1 min-h-[400px]">
                {!isMounted || enrollmentLoading ? (
                  <div className="flex items-center justify-center h-full">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
                  </div>
                ) : (
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={enrollmentChartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                      <XAxis dataKey="year" stroke="#6b7280" fontSize={12} />
                      <YAxis tickFormatter={formatNumber} stroke="#6b7280" fontSize={12} />
                      <Tooltip 
                        formatter={(v) => [formatNumber(v as number), "Enrollment"]} 
                        contentStyle={{ borderRadius: 8, border: '1px solid #e5e7eb' }}
                      />
                      <Line 
                        type="monotone" 
                        dataKey="enrollment" 
                        stroke={COLORS.blue} 
                        strokeWidth={2.5} 
                        dot={{ r: 4, fill: COLORS.blue }} 
                        activeDot={{ r: 6 }}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                )}
              </div>
            </div>

            {/* RIGHT: Breakdown Panels (50%) */}
            <div className="lg:w-1/2 p-6 flex flex-col">
              {/* Year Selector for Breakdowns */}
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold text-gray-800">Market Breakdown</h3>
                <div className="flex items-center gap-2">
                  <span className="text-sm text-gray-500">Year:</span>
                  <select
                    value={breakdownYear}
                    onChange={(e) => setBreakdownYear(Number(e.target.value))}
                    className="border rounded px-3 py-1.5 text-sm font-medium"
                  >
                    {[2026, 2025, 2024, 2023, 2022, 2021, 2020].map(y => (
                      <option key={y} value={y}>{y}</option>
                    ))}
                  </select>
                </div>
              </div>

              {/* Breakdown Grid - 2x2 */}
              <div className="grid grid-cols-2 gap-4 flex-1">
                
                {/* Product Type Breakdown */}
                <div className="bg-gray-50 rounded-lg p-4">
                  <h4 className="text-sm font-semibold text-gray-700 mb-3">By Product Type</h4>
                  <div className="space-y-2">
                    {productBreakdown?.map((item, i) => {
                      const total = totalBreakdown(productBreakdown);
                      const pct = total > 0 ? (item.value / total) * 100 : 0;
                      return (
                        <div key={item.name}>
                          <div className="flex justify-between text-sm mb-1">
                            <span className="font-medium">{item.name}</span>
                            <span className="text-gray-600">{formatNumber(item.value)} ({pct.toFixed(1)}%)</span>
                          </div>
                          <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
                            <div 
                              className="h-full rounded-full transition-all" 
                              style={{ width: `${pct}%`, backgroundColor: BREAKDOWN_COLORS[i] }}
                            />
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>

                {/* Group Type Breakdown */}
                <div className="bg-gray-50 rounded-lg p-4">
                  <h4 className="text-sm font-semibold text-gray-700 mb-3">By Market Segment</h4>
                  <div className="space-y-2">
                    {groupBreakdown?.map((item, i) => {
                      const total = totalBreakdown(groupBreakdown);
                      const pct = total > 0 ? (item.value / total) * 100 : 0;
                      return (
                        <div key={item.name}>
                          <div className="flex justify-between text-sm mb-1">
                            <span className="font-medium">{item.name}</span>
                            <span className="text-gray-600">{formatNumber(item.value)} ({pct.toFixed(1)}%)</span>
                          </div>
                          <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
                            <div 
                              className="h-full rounded-full transition-all" 
                              style={{ width: `${pct}%`, backgroundColor: BREAKDOWN_COLORS[i] }}
                            />
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>

                {/* SNP Type Breakdown */}
                <div className="bg-gray-50 rounded-lg p-4">
                  <h4 className="text-sm font-semibold text-gray-700 mb-3">By SNP Type</h4>
                  <div className="space-y-2">
                    {snpBreakdown?.map((item, i) => {
                      const total = totalBreakdown(snpBreakdown);
                      const pct = total > 0 ? (item.value / total) * 100 : 0;
                      return (
                        <div key={item.name}>
                          <div className="flex justify-between text-sm mb-1">
                            <span className="font-medium">{item.name}</span>
                            <span className="text-gray-600">{formatNumber(item.value)} ({pct.toFixed(1)}%)</span>
                          </div>
                          <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
                            <div 
                              className="h-full rounded-full transition-all" 
                              style={{ width: `${pct}%`, backgroundColor: BREAKDOWN_COLORS[i] }}
                            />
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>

                {/* Plan Type Breakdown */}
                <div className="bg-gray-50 rounded-lg p-4">
                  <h4 className="text-sm font-semibold text-gray-700 mb-3">By Plan Type</h4>
                  <div className="space-y-2">
                    {planBreakdown?.map((item, i) => {
                      const total = totalBreakdown(planBreakdown);
                      const pct = total > 0 ? (item.value / total) * 100 : 0;
                      return (
                        <div key={item.name}>
                          <div className="flex justify-between text-sm mb-1">
                            <span className="font-medium">{item.name}</span>
                            <span className="text-gray-600">{formatNumber(item.value)} ({pct.toFixed(1)}%)</span>
                          </div>
                          <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
                            <div 
                              className="h-full rounded-full transition-all" 
                              style={{ width: `${pct}%`, backgroundColor: BREAKDOWN_COLORS[i] }}
                            />
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>

              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Click outside to close dropdowns */}
      {showEnrollPayerDropdown && (
        <div className="fixed inset-0 z-10" onClick={() => setShowEnrollPayerDropdown(false)} />
      )}
    </div>
  );
}
