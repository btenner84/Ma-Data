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

function formatRiskScore(num: number | undefined | null): string {
  if (num === undefined || num === null || isNaN(num)) return "-";
  return num.toFixed(2);
}

function formatEnrollment(num: number | undefined | null): string {
  if (num === undefined || num === null || isNaN(num)) return "-";
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(0)}K`;
  return num.toLocaleString();
}

interface RiskScoreFilters {
  years: number[];
  parent_orgs: string[];
  plan_types: string[];
  plan_types_simplified: string[];
  snp_types: string[];
  group_types: string[];
}

interface RiskScoreTimeSeriesV2 {
  years: number[];
  series: Record<string, (number | null)[]>;
  enrollment: Record<string, (number | null)[]>;
  metric: "avg" | "wavg";
  group_by: string | null;
  error?: string;
}

interface ContractDetail {
  contract_id: string;
  plan_id: string;
  contract_name: string;
  parent_org: string;
  plan_type: string;
  group_type: string;
  snp_type: string;
  risk_score: number;
  enrollment: number;
  weighted_score: number;
}

interface ContractDetailsResponse {
  year: number;
  parent_org: string;
  contracts: ContractDetail[];
  summary: {
    contract_count: number;
    total_enrollment: number;
    weighted_avg: number;
    simple_avg: number;
    total_weighted_score: number;
  };
  audit_id: string;
}

export default function RiskScoresPage() {
  const [selectedPlanTypes, setSelectedPlanTypes] = useState<string[]>([]);
  const [selectedGroupTypes, setSelectedGroupTypes] = useState<string[]>([]);
  const [selectedSnpTypes, setSelectedSnpTypes] = useState<string[]>([]);
  const [selectedParentOrgs, setSelectedParentOrgs] = useState<string[]>([]);
  const [showIndustryTotal, setShowIndustryTotal] = useState(true);
  const [yearRange, setYearRange] = useState<number | null>(null);
  const [metric, setMetric] = useState<"avg" | "wavg">("wavg");

  // Popup states
  const [showFilterPopup, setShowFilterPopup] = useState(false);
  const [showPayerPopup, setShowPayerPopup] = useState(false);
  const [payerSearch, setPayerSearch] = useState("");

  // Detail modal state for auditing
  const [detailSelection, setDetailSelection] = useState<{
    parentOrg: string;
    year: number;
  } | null>(null);

  // Fetch filter options (v3 API with audit trail)
  const { data: filterOptions } = useQuery<RiskScoreFilters>({
    queryKey: ["risk-filters-v3"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/v3/risk/filters`);
      return res.json();
    },
  });

  // Build query params
  const buildQueryParams = () => {
    const params = new URLSearchParams();
    if (selectedPlanTypes.length > 0) params.set("plan_types", selectedPlanTypes.join(","));
    if (selectedGroupTypes.length > 0) params.set("group_types", selectedGroupTypes.join(","));
    if (selectedSnpTypes.length > 0) params.set("snp_types", selectedSnpTypes.join(","));
    if (selectedParentOrgs.length > 0) params.set("parent_orgs", selectedParentOrgs.join("|"));
    params.set("include_total", "true");
    params.set("metric", metric);
    return params.toString();
  };

  // Fetch timeseries data (v3 API with audit trail)
  const { data: rawTimeseriesData, isLoading } = useQuery<RiskScoreTimeSeriesV2>({
    queryKey: ["risk-timeseries-v3", selectedPlanTypes, selectedGroupTypes, selectedSnpTypes, selectedParentOrgs, metric],
    queryFn: async () => {
      const params = buildQueryParams();
      const res = await fetch(`${API_BASE}/api/v3/risk/timeseries?${params}`);
      return res.json();
    },
  });

  // Fetch contract details when a cell is clicked
  const { data: contractDetails, isLoading: isLoadingDetails } = useQuery<ContractDetailsResponse>({
    queryKey: ["risk-contracts", detailSelection?.year, detailSelection?.parentOrg, selectedPlanTypes, selectedGroupTypes, selectedSnpTypes],
    queryFn: async () => {
      if (!detailSelection) return null;
      const params = new URLSearchParams();
      params.set("year", String(detailSelection.year));
      if (detailSelection.parentOrg !== "Industry Total") {
        params.set("parent_org", detailSelection.parentOrg);
      }
      if (selectedPlanTypes.length > 0) params.set("plan_types", selectedPlanTypes.join(","));
      if (selectedGroupTypes.length > 0) params.set("group_types", selectedGroupTypes.join(","));
      if (selectedSnpTypes.length > 0) params.set("snp_types", selectedSnpTypes.join(","));
      const res = await fetch(`${API_BASE}/api/v3/risk/contracts?${params}`);
      return res.json();
    },
    enabled: !!detailSelection,
  });

  // Filter out Industry Total if not showing it
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

  // Transform data for chart
  const allChartData = timeseriesData?.years?.map((year, i) => {
    const point: Record<string, any> = { year };
    if (timeseriesData.series) {
      Object.entries(timeseriesData.series).forEach(([key, values]) => {
        point[key] = values[i];
      });
    }
    return point;
  }) || [];

  // Filter by year range
  const chartData = yearRange
    ? allChartData.slice(-yearRange)
    : allChartData;

  const seriesKeys = timeseriesData?.series
    ? Object.keys(timeseriesData.series).filter(k => {
        const vals = timeseriesData.series![k];
        return vals.some(v => v !== null && v > 0);
      })
    : [];

  // Get current weighted avg risk score
  const currentRiskScore = (() => {
    if (!chartData.length || !seriesKeys.length) return null;
    const lastPoint = chartData[chartData.length - 1];
    const totalKey = seriesKeys.includes('Total') ? 'Total' : seriesKeys.includes('Industry Total') ? 'Industry Total' : seriesKeys[0];
    return lastPoint?.[totalKey];
  })();

  const hasTypeFilters = selectedPlanTypes.length > 0 || selectedGroupTypes.length > 0 || selectedSnpTypes.length > 0;
  const typeFilterCount = selectedPlanTypes.length + selectedGroupTypes.length + selectedSnpTypes.length;

  const clearTypeFilters = () => {
    setSelectedPlanTypes([]);
    setSelectedGroupTypes([]);
    setSelectedSnpTypes([]);
  };

  const removeParentOrg = (org: string) => {
    const newOrgs = selectedParentOrgs.filter(o => o !== org);
    setSelectedParentOrgs(newOrgs);
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

  // Filter payers
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
              onClick={() => { setShowFilterPopup(!showFilterPopup); setShowPayerPopup(false); }}
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
                <div className="mb-5">
                  <label className="block text-sm font-medium text-gray-700 mb-2">Market Segment</label>
                  <div className="flex gap-2">
                    {["Individual", "Group"].map((type) => (
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

                {/* Plan Type */}
                <div className="mb-5">
                  <label className="block text-sm font-medium text-gray-700 mb-2">Plan Type</label>
                  <div className="flex flex-wrap gap-2">
                    {["HMO", "PPO", "RPPO", "PFFS", "MSA"].map((type) => (
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

                {/* SNP Type */}
                <div className="mb-5">
                  <label className="block text-sm font-medium text-gray-700 mb-2">Special Needs Plans (SNP)</label>
                  <div className="flex flex-wrap gap-2">
                    {["Non-SNP", "D-SNP", "C-SNP", "I-SNP"].map((type) => (
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
                  <p className="text-xs text-gray-500 mt-1">D-SNP: Dual-Eligible, C-SNP: Chronic Condition, I-SNP: Institutional</p>
                </div>

                <button
                  onClick={() => setShowFilterPopup(false)}
                  className="mt-2 w-full py-2.5 bg-gray-900 hover:bg-gray-800 text-white rounded-lg text-sm font-medium transition-colors"
                >
                  Apply Filters
                </button>
              </div>
            )}
          </div>

          {/* Add Payer Button */}
          <div className="relative">
            <button
              onClick={() => { setShowPayerPopup(!showPayerPopup); setShowFilterPopup(false); }}
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

          {/* Add Industry Total back button */}
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

          {/* Spacer */}
          <div className="flex-1" />

          {/* Metric Toggle */}
          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-500">Metric:</span>
            {[
              { value: "wavg" as const, label: "Weighted" },
              { value: "avg" as const, label: "Simple" },
            ].map((opt) => (
              <button
                key={opt.value}
                onClick={() => setMetric(opt.value)}
                className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                  metric === opt.value
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

          {/* Current Risk Score */}
          {currentRiskScore && (
            <div className="text-right bg-gray-50 px-4 py-2 rounded-lg">
              <div className="text-xs text-gray-500 uppercase tracking-wide">{metric === "wavg" ? "Weighted Avg" : "Simple Avg"}</div>
              <div className="text-2xl font-bold text-gray-900">{formatRiskScore(currentRiskScore)}</div>
            </div>
          )}
        </div>
      </div>

      {/* Click outside to close popups */}
      {(showFilterPopup || showPayerPopup) && (
        <div
          className="fixed inset-0 z-40"
          onClick={() => {
            setShowFilterPopup(false);
            setShowPayerPopup(false);
          }}
        />
      )}

      {/* Chart */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-gray-900">
            Risk Score Over Time
            {hasTypeFilters && <span className="text-sm font-normal text-gray-500 ml-2">(filtered)</span>}
          </h2>
        </div>
        <div className="h-96">
          {isLoading ? (
            <div className="flex items-center justify-center h-full">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            </div>
          ) : chartData.length > 0 ? (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="year" tick={{ fill: '#6b7280' }} />
                <YAxis
                  domain={[0.9, 1.4]}
                  tickFormatter={(v) => v.toFixed(2)}
                  tick={{ fill: '#6b7280' }}
                  width={50}
                />
                <Tooltip
                  formatter={(value) => [typeof value === 'number' ? value.toFixed(4) : "-", "Risk Score"]}
                  labelFormatter={(year) => `Year: ${year}`}
                  contentStyle={{ borderRadius: '8px', border: '1px solid #e5e7eb' }}
                />
                <Legend />
                {seriesKeys.map((key, i) => (
                  <Line
                    key={key}
                    type="monotone"
                    dataKey={key}
                    stroke={key === "Industry Total" ? "#9ca3af" : COLORS[i % COLORS.length]}
                    strokeWidth={key === "Industry Total" ? 2 : 2.5}
                    strokeDasharray={key === "Industry Total" ? "5 5" : undefined}
                    dot={{ fill: key === "Industry Total" ? "#9ca3af" : COLORS[i % COLORS.length], r: 4 }}
                    activeDot={{ r: 6 }}
                    name={key}
                    connectNulls
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

      {/* Risk Score Data Table */}
      {chartData.length > 0 && (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200">
            <h3 className="text-lg font-semibold text-gray-900">
              Risk Score by Year ({metric === "wavg" ? "Enrollment-Weighted" : "Simple Average"})
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
                            style={{ backgroundColor: key === "Industry Total" ? "#9ca3af" : COLORS[idx % COLORS.length] }}
                          />
                          <span className="truncate max-w-[200px]" title={key}>{key}</span>
                        </div>
                      </td>
                      {chartData.map((row) => (
                        <td
                          key={row.year}
                          className="px-4 py-3 text-sm text-gray-700 text-right font-mono whitespace-nowrap cursor-pointer hover:bg-blue-50 transition-colors"
                          onClick={() => setDetailSelection({ parentOrg: key, year: row.year })}
                          title="Click to see contract breakdown"
                        >
                          {formatRiskScore(row[key])}
                        </td>
                      ))}
                    </tr>
                    {/* YoY % change row */}
                    <tr className="bg-gray-50/30">
                      <td className="px-4 py-1 text-xs text-gray-400 sticky left-0 bg-inherit italic">
                        YoY %
                      </td>
                      {chartData.map((row, i) => {
                        if (i === 0) {
                          return <td key={`${row.year}-yoy`} className="px-4 py-1 text-xs text-gray-400 text-right">—</td>;
                        }
                        const prev = chartData[i - 1][key] as number | null;
                        const curr = row[key] as number | null;
                        if (prev === null || curr === null || prev === 0) {
                          return <td key={`${row.year}-yoy`} className="px-4 py-1 text-xs text-gray-400 text-right">—</td>;
                        }
                        const change = ((curr - prev) / prev) * 100;
                        const isPositive = change > 0;
                        const isNegative = change < 0;
                        // Standard convention: increase = green, decrease = red
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

      {/* Enrollment Table */}
      {timeseriesData?.enrollment && Object.keys(timeseriesData.enrollment).length > 0 && chartData.length > 0 && (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200">
            <h3 className="text-lg font-semibold text-gray-900">
              Enrollment by Year (for weighting)
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
                {seriesKeys.map((key, idx) => {
                  const enrollmentValues = timeseriesData.enrollment?.[key];
                  if (!enrollmentValues) return null;
                  return (
                    <tr key={key} className={idx % 2 === 0 ? "bg-white" : "bg-gray-50/50"}>
                      <td className="px-4 py-3 text-sm font-medium text-gray-900 sticky left-0 bg-inherit">
                        <div className="flex items-center gap-2">
                          <span
                            className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                            style={{ backgroundColor: key === "Industry Total" ? "#9ca3af" : COLORS[idx % COLORS.length] }}
                          />
                          <span className="truncate max-w-[200px]" title={key}>{key}</span>
                        </div>
                      </td>
                      {chartData.map((row) => {
                        const yearIndex = timeseriesData.years?.indexOf(row.year as number) ?? -1;
                        const enrollment = yearIndex >= 0 ? enrollmentValues[yearIndex] : null;
                        return (
                          <td key={row.year} className="px-4 py-3 text-sm text-gray-700 text-right font-mono whitespace-nowrap">
                            {formatEnrollment(enrollment)}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Contract Details Modal */}
      {detailSelection && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-xl shadow-2xl max-w-5xl w-full max-h-[90vh] flex flex-col">
            {/* Modal Header */}
            <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between flex-shrink-0">
              <div>
                <h2 className="text-xl font-bold text-gray-900">
                  Contract Breakdown: {detailSelection.parentOrg}
                </h2>
                <p className="text-sm text-gray-500 mt-1">
                  Year {detailSelection.year} • Click to audit weighted average calculation
                </p>
              </div>
              <button
                onClick={() => setDetailSelection(null)}
                className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
              >
                <X className="w-5 h-5 text-gray-500" />
              </button>
            </div>

            {/* Modal Content */}
            <div className="flex-1 overflow-auto p-6">
              {isLoadingDetails ? (
                <div className="flex items-center justify-center py-12">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                </div>
              ) : contractDetails?.contracts ? (
                <>
                  {/* Summary Stats */}
                  <div className="grid grid-cols-4 gap-4 mb-6">
                    <div className="bg-blue-50 rounded-lg p-4">
                      <div className="text-sm text-blue-600 font-medium">Contracts</div>
                      <div className="text-2xl font-bold text-blue-900">{contractDetails.summary.contract_count}</div>
                    </div>
                    <div className="bg-green-50 rounded-lg p-4">
                      <div className="text-sm text-green-600 font-medium">Total Enrollment</div>
                      <div className="text-2xl font-bold text-green-900">{formatEnrollment(contractDetails.summary.total_enrollment)}</div>
                    </div>
                    <div className="bg-purple-50 rounded-lg p-4">
                      <div className="text-sm text-purple-600 font-medium">Weighted Avg</div>
                      <div className="text-2xl font-bold text-purple-900">{contractDetails.summary.weighted_avg?.toFixed(4) || '-'}</div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <div className="text-sm text-gray-600 font-medium">Simple Avg</div>
                      <div className="text-2xl font-bold text-gray-900">{contractDetails.summary.simple_avg?.toFixed(4) || '-'}</div>
                    </div>
                  </div>

                  {/* Formula Explanation */}
                  <div className="bg-amber-50 border border-amber-200 rounded-lg p-4 mb-6">
                    <div className="text-sm text-amber-800">
                      <strong>Weighted Average Formula:</strong> SUM(Risk Score × % Weight)
                      <br />
                      Where % Weight = Contract Enrollment / Total Enrollment
                      <br />
                      <span className="text-amber-600">Sum of all Contributions = <strong>{contractDetails.summary.weighted_avg?.toFixed(4)}</strong></span>
                    </div>
                  </div>

                  {/* Contracts Table */}
                  <div className="border border-gray-200 rounded-lg overflow-hidden">
                    <div className="max-h-[400px] overflow-auto">
                      <table className="w-full text-sm">
                        <thead className="bg-gray-50 sticky top-0">
                          <tr>
                            <th className="px-4 py-3 text-left font-semibold text-gray-600">Contract</th>
                            <th className="px-4 py-3 text-left font-semibold text-gray-600">Plan Type</th>
                            <th className="px-4 py-3 text-left font-semibold text-gray-600">Group</th>
                            <th className="px-4 py-3 text-left font-semibold text-gray-600">SNP</th>
                            <th className="px-4 py-3 text-right font-semibold text-gray-600">Risk Score</th>
                            <th className="px-4 py-3 text-right font-semibold text-gray-600">Enrollment</th>
                            <th className="px-4 py-3 text-right font-semibold text-gray-600">% Weight</th>
                            <th className="px-4 py-3 text-right font-semibold text-gray-600">Contribution</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-100">
                          {contractDetails.contracts.map((contract, idx) => {
                            const pctWeight = contractDetails.summary.total_enrollment > 0
                              ? (contract.enrollment / contractDetails.summary.total_enrollment) * 100
                              : 0;
                            const contribution = contract.risk_score * (pctWeight / 100);
                            return (
                              <tr key={`${contract.contract_id}-${contract.plan_id}-${idx}`} className="hover:bg-gray-50">
                                <td className="px-4 py-2">
                                  <div className="font-medium text-gray-900">{contract.contract_id}-{contract.plan_id}</div>
                                  <div className="text-xs text-gray-500 truncate max-w-[200px]" title={contract.contract_name}>
                                    {contract.contract_name}
                                  </div>
                                </td>
                                <td className="px-4 py-2 text-gray-600">{contract.plan_type}</td>
                                <td className="px-4 py-2 text-gray-600">{contract.group_type}</td>
                                <td className="px-4 py-2 text-gray-600">{contract.snp_type}</td>
                                <td className="px-4 py-2 text-right font-mono text-gray-900">{contract.risk_score?.toFixed(4)}</td>
                                <td className="px-4 py-2 text-right font-mono text-gray-600">{contract.enrollment?.toLocaleString()}</td>
                                <td className="px-4 py-2 text-right font-mono text-gray-600">{pctWeight.toFixed(2)}%</td>
                                <td className="px-4 py-2 text-right font-mono text-gray-600">{contribution.toFixed(4)}</td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  {/* Audit Info */}
                  <div className="mt-4 text-xs text-gray-400">
                    Audit ID: {contractDetails.audit_id}
                  </div>
                </>
              ) : (
                <div className="text-center py-12 text-gray-500">
                  No contract data available
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
