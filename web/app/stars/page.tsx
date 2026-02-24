"use client";

import { useState } from "react";
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
import { Filter, Plus, X, ChevronDown, Home, Target, BarChart3, FileText } from "lucide-react";
import { starsAPI } from "@/lib/api";

type StarsTab = "home" | "cutpoints" | "measures" | "contract";

const TABS: { id: StarsTab; label: string; icon: React.ReactNode }[] = [
  { id: "home", label: "Home", icon: <Home className="w-4 h-4" /> },
  { id: "cutpoints", label: "Cutpoints", icon: <Target className="w-4 h-4" /> },
  { id: "measures", label: "Measure Performance", icon: <BarChart3 className="w-4 h-4" /> },
  { id: "contract", label: "Contract", icon: <FileText className="w-4 h-4" /> },
];

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

const COLORS = [
  "#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c",
  "#0891b2", "#4f46e5", "#c026d3", "#059669", "#d97706",
];

function formatNumber(num: number): string {
  if (num >= 1000000) return `${(num / 1000000).toFixed(2)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(0)}K`;
  return num.toLocaleString();
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

interface FourPlusTimeseriesData {
  years: number[];
  series: Record<string, (number | null)[]>;
  filters: {
    plan_types: string | null;
    product_types: string | null;
    group_types: string | null;
    snp_types: string | null;
  };
  error?: string;
}

// Cutpoints time series measure type
interface CutpointYearData {
  measure_id: string;
  cut_5: string | null;
  cut_4: string | null;
  cut_3: string | null;
  cut_2: string | null;
  cut_5_num: number | null;
  cut_4_num: number | null;
  cut_3_num: number | null;
  cut_2_num: number | null;
}

interface CutpointsMeasure {
  measure_key: string;
  measure_name: string;
  part: string;
  domain: string;
  weight: number;
  lower_is_better: boolean;
  data_source: 'CAHPS' | 'HOS' | 'HEDIS' | 'Admin';
  cutpoint_method: 'Survey' | 'Clustering' | 'Admin';
  first_year: number;
  years_active: number[];
  yearly: Record<string, CutpointYearData>;  // Keys are string (from JSON)
}

// Measure enrollment response type
interface MeasureEnrollmentTimeseries {
  measure_key: string;
  parent_org: string;
  years: number[];
  distribution: Record<string, number[]>;
  fourplus_pct: number[];
  fourplus_enrollment: number[];
  total_enrollment: number[];
  error?: string;
}

// Contract audit drill-down types
interface ContractAuditItem {
  contract_id: string;
  parent_org: string | null;
  star_rating: number;
  enrollment: number;
}

interface ContractAuditResponse {
  measure_key: string;
  year: number;
  parent_org: string;
  contracts: ContractAuditItem[];
  summary: {
    total_contracts: number;
    fourplus_contracts: number;
    total_enrollment: number;
    fourplus_enrollment: number;
    fourplus_pct: number;
  };
  error?: string;
}

// Measure performance table types
interface MeasurePerformanceYearData {
  value: number | null;
  contract_count: number;
  enrollment: number;
}

interface MeasurePerformanceRow {
  measure_id: string;
  measure_key: string;
  measure_name: string;
  part: string;
  lower_is_better: boolean;
  in_2026: boolean;
  yearly: Record<string, MeasurePerformanceYearData | null>;
  weights: Record<string, number>;  // year -> weight
}

interface MeasurePerformanceData {
  parent_org: string;
  avg_type: string;
  years: number[];
  measures: MeasurePerformanceRow[];
  validation: {
    total_measures: number;
    measures_in_2026: number;
    discontinued_measures: number;
  };
  error?: string;
}

interface MeasurePerformanceDetailContract {
  contract_id: string;
  parent_org: string | null;
  performance_pct: number | null;
  enrollment: number | null;
}

interface MeasurePerformanceDetail {
  measure_key: string;
  measure_id: string;
  measure_name: string;
  year: number;
  parent_org: string;
  contract_count: number;
  contracts: MeasurePerformanceDetailContract[];
  error?: string;
}

// Contract performance types
interface ContractYearlyData {
  value: number | null;
  measure_id: string;
  star_rating: number | null;
  weight?: number;
}

interface ContractBandPosition {
  year: number;
  star_rating: number;
  position: 'top' | 'middle' | 'bottom' | null;
}

interface ContractCutpoints {
  cut_2: number | null;
  cut_3: number | null;
  cut_4: number | null;
  cut_5: number | null;
}

interface ContractMeasureRow {
  measure_id: string;
  measure_key: string;
  measure_name: string;
  part: string;
  lower_is_better: boolean;
  weight: number;
  in_2026: boolean;
  yearly: Record<string, ContractYearlyData | null>;
  latest_band: ContractBandPosition | null;
  cutpoints: ContractCutpoints | null;
}

interface ContractPerformanceData {
  contract_id: string;
  parent_org: string | null;
  years: number[];
  measures: ContractMeasureRow[];
  weighted_avg_star: number | null;
  total_weight: number;
  yearly_weighted_avgs?: Record<number, number | null>;
  error?: string;
}

interface ContractListItem {
  contract_id: string;
  parent_org: string | null;
  enrollment: number;
}

function formatEnrollment(num: number): string {
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(0)}K`;
  return num.toLocaleString();
}

// Full-width line chart for a single measure (one row per measure)
function MeasureRowChart({
  measure,
  starLevel,
  years,
  selectedPayer
}: {
  measure: CutpointsMeasure;
  starLevel: 2 | 3 | 4 | 5;
  years: number[];
  selectedPayer: string;
}) {
  // State for contract audit modal
  const [auditYear, setAuditYear] = useState<number | null>(null);

  // Fetch enrollment data for this measure
  // Pass part (C/D) to get correct ratings for dual-rated measures like Call Center
  const { data: enrollmentData } = useQuery<MeasureEnrollmentTimeseries>({
    queryKey: ["measure-enrollment", measure.measure_key, measure.part, selectedPayer],
    queryFn: async () => {
      const params = new URLSearchParams({ measure_key: measure.measure_key });
      if (selectedPayer !== "Industry") params.set("parent_org", selectedPayer);
      if (measure.part) params.set("part", measure.part);
      const res = await fetch(`${API_BASE}/api/stars/measure-enrollment-timeseries?${params}`);
      if (!res.ok) return { error: "Failed to load", years: [], distribution: {}, fourplus_pct: [], measure_key: measure.measure_key, parent_org: selectedPayer };
      return res.json();
    },
    staleTime: 60000,
  });

  // Fetch contract audit data when a year is clicked
  const { data: auditData, isLoading: loadingAudit } = useQuery<ContractAuditResponse>({
    queryKey: ["measure-enrollment-contracts", measure.measure_key, measure.part, selectedPayer, auditYear],
    queryFn: async () => {
      const params = new URLSearchParams({
        measure_key: measure.measure_key,
        year: String(auditYear),
      });
      if (selectedPayer !== "Industry") params.set("parent_org", selectedPayer);
      if (measure.part) params.set("part", measure.part);
      const res = await fetch(`${API_BASE}/api/stars/measure-enrollment-contracts?${params}`);
      if (!res.ok) return { error: "Failed to load", measure_key: measure.measure_key, year: auditYear!, parent_org: selectedPayer, contracts: [], summary: { total_contracts: 0, fourplus_contracts: 0, total_enrollment: 0, fourplus_enrollment: 0, fourplus_pct: 0 } };
      return res.json();
    },
    enabled: auditYear !== null,
    staleTime: 60000,
  });

  const cutKey = `cut_${starLevel}_num` as const;

  // Build chart data - API returns string keys, so convert year to string
  const chartData = years.map(year => {
    const yearData = measure.yearly[String(year)];
    return {
      year,
      value: yearData?.[cutKey] ?? null,
      displayValue: yearData?.[`cut_${starLevel}` as keyof CutpointYearData] ?? null,
    };
  });

  // Check if we have any data
  const hasData = chartData.some(d => d.value !== null);

  // Get min/max for scale
  const values = chartData.filter(d => d.value !== null).map(d => d.value as number);
  const minVal = values.length > 0 ? Math.min(...values) : 0;
  const maxVal = values.length > 0 ? Math.max(...values) : 100;
  const range = maxVal - minVal || 10;
  const padding = range * 0.15;

  const starColors: Record<number, string> = {
    5: "#16a34a",
    4: "#2563eb",
    3: "#ca8a04",
    2: "#ea580c",
  };

  const latestYear = Math.max(...measure.years_active);
  const latestData = measure.yearly[String(latestYear)];

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5 mb-4">
      {/* Header Row */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            {/* Data Source Badge */}
            {measure.data_source === 'CAHPS' && (
              <span className="text-xs bg-blue-100 text-blue-700 px-2 py-0.5 rounded" title="Consumer Assessment of Healthcare Providers and Systems - patient survey">
                CAHPS
              </span>
            )}
            {measure.data_source === 'HOS' && (
              <span className="text-xs bg-teal-100 text-teal-700 px-2 py-0.5 rounded" title="Health Outcomes Survey">
                HOS
              </span>
            )}
            {measure.data_source === 'HEDIS' && (
              <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded" title="Healthcare Effectiveness Data and Information Set - claims/clinical data">
                HEDIS
              </span>
            )}
            {measure.data_source === 'Admin' && (
              <span className="text-xs bg-amber-100 text-amber-700 px-2 py-0.5 rounded" title="Administrative data (complaints, appeals, etc.)">
                Admin
              </span>
            )}
            {/* Cutpoint Method Badge */}
            {measure.cutpoint_method === 'Survey' && (
              <span className="text-xs bg-indigo-50 text-indigo-600 px-2 py-0.5 rounded" title="Cutpoints set using relative distribution with significance testing">
                Survey Method
              </span>
            )}
            {measure.cutpoint_method === 'Clustering' && (
              <span className="text-xs bg-slate-50 text-slate-600 px-2 py-0.5 rounded" title="Cutpoints set using hierarchical clustering algorithm">
                Clustering
              </span>
            )}
            {/* Lower is Better Badge */}
            {measure.lower_is_better && (
              <span className="text-xs bg-red-100 text-red-600 px-2 py-0.5 rounded">↓ lower better</span>
            )}
          </div>
          <h3 className="font-semibold text-gray-900 text-lg">{measure.measure_name}</h3>
          <p className="text-sm text-gray-500">{measure.domain}</p>
        </div>
      </div>

      {/* Chart */}
      <div className="h-48 mb-4">
        {!hasData ? (
          <div className="h-full flex items-center justify-center text-gray-400">No cutpoint data available</div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 10, right: 30, bottom: 10, left: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="year"
                tick={{ fontSize: 11, fill: '#6b7280' }}
                tickLine={false}
              />
              <YAxis
                domain={
                  measure.lower_is_better
                    ? [Math.ceil(maxVal + padding), Math.floor(minVal - padding)]  // Inverted for lower-is-better
                    : [Math.floor(minVal - padding), Math.ceil(maxVal + padding)]
                }
                tick={{ fontSize: 11, fill: '#6b7280' }}
                tickLine={false}
                tickFormatter={(v) => `${v}%`}
                width={45}
                reversed={measure.lower_is_better}
              />
              <Tooltip
                formatter={(value) => [`${value}%`, `${starLevel}★ Cutpoint`]}
                labelFormatter={(year) => `${year}`}
                contentStyle={{ fontSize: 12, borderRadius: 8 }}
              />
              <Line
                type="monotone"
                dataKey="value"
                stroke={starColors[starLevel]}
                strokeWidth={2.5}
                dot={{ fill: starColors[starLevel], r: 4 }}
                activeDot={{ r: 6 }}
                connectNulls={false}
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* 4★+ Enrollment by Year */}
      <div className="border-t border-gray-100 pt-4">
        <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
          4★+ Enrollment by Year
          {selectedPayer !== "Industry" && (
            <span className="ml-2 text-blue-600 font-normal">({selectedPayer})</span>
          )}
        </h4>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-green-50">
                {years.map(year => (
                  <th key={year} className="py-2 px-3 text-center text-green-700 font-medium min-w-[70px]">
                    {year}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              <tr className="bg-green-50">
                {years.map(year => {
                  const yearIdx = enrollmentData?.years?.indexOf(year);
                  const pct = yearIdx !== undefined && yearIdx >= 0
                    ? enrollmentData?.fourplus_pct?.[yearIdx]
                    : null;
                  const count = yearIdx !== undefined && yearIdx >= 0
                    ? enrollmentData?.fourplus_enrollment?.[yearIdx]
                    : null;
                  return (
                    <td key={year} className="py-2 px-3 text-center">
                      {pct !== null && pct !== undefined ? (
                        <button
                          onClick={() => setAuditYear(year)}
                          className="hover:bg-green-100 rounded px-1 py-0.5 transition-colors cursor-pointer"
                          title="Click to see contract breakdown"
                        >
                          <span className="font-bold text-green-700">{pct}%</span>
                          {count !== null && count !== undefined && (
                            <span className="text-green-600 text-xs ml-1">({formatEnrollment(count)})</span>
                          )}
                        </button>
                      ) : (
                        <span className="text-gray-300">-</span>
                      )}
                    </td>
                  );
                })}
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {/* Contract Audit Modal */}
      {auditYear !== null && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={() => setAuditYear(null)}>
          <div
            className="bg-white rounded-xl shadow-2xl max-w-4xl w-full mx-4 max-h-[80vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            {/* Header */}
            <div className="p-4 border-b border-gray-200 flex items-center justify-between bg-gray-50">
              <div>
                <h3 className="font-semibold text-lg">{measure.measure_name}</h3>
                <p className="text-sm text-gray-600">
                  4★+ Enrollment Breakdown for {auditYear}
                  {selectedPayer !== "Industry" && <span className="text-blue-600 ml-1">({selectedPayer})</span>}
                </p>
              </div>
              <button
                onClick={() => setAuditYear(null)}
                className="text-gray-500 hover:text-gray-700 text-2xl leading-none"
              >
                &times;
              </button>
            </div>

            {/* Content */}
            <div className="flex-1 overflow-auto p-4">
              {loadingAudit ? (
                <div className="flex items-center justify-center py-8">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                </div>
              ) : auditData?.error ? (
                <div className="text-red-600 text-center py-8">{auditData.error}</div>
              ) : auditData?.contracts ? (
                <>
                  {/* Summary */}
                  <div className="bg-green-50 rounded-lg p-4 mb-4">
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-center">
                      <div>
                        <div className="text-2xl font-bold text-green-700">{auditData.summary.fourplus_pct}%</div>
                        <div className="text-xs text-green-600">4★+ Enrollment</div>
                      </div>
                      <div>
                        <div className="text-2xl font-bold text-green-700">{formatEnrollment(auditData.summary.fourplus_enrollment)}</div>
                        <div className="text-xs text-green-600">4★+ Members</div>
                      </div>
                      <div>
                        <div className="text-2xl font-bold text-gray-700">{formatEnrollment(auditData.summary.total_enrollment)}</div>
                        <div className="text-xs text-gray-600">Total Members</div>
                      </div>
                      <div>
                        <div className="text-2xl font-bold text-gray-700">{auditData.summary.fourplus_contracts} / {auditData.summary.total_contracts}</div>
                        <div className="text-xs text-gray-600">4★+ Contracts</div>
                      </div>
                    </div>
                  </div>

                  {/* Contract Table */}
                  <table className="w-full text-sm">
                    <thead className="bg-gray-100 sticky top-0">
                      <tr>
                        <th className="text-left py-2 px-3">Contract</th>
                        <th className="text-left py-2 px-3">Parent Org</th>
                        <th className="text-center py-2 px-3">Star Rating</th>
                        <th className="text-right py-2 px-3">Enrollment</th>
                      </tr>
                    </thead>
                    <tbody>
                      {auditData.contracts.map((contract, idx) => (
                        <tr
                          key={contract.contract_id}
                          className={`${idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'} ${contract.star_rating >= 4 ? 'text-green-700' : ''}`}
                        >
                          <td className="py-2 px-3 font-mono">{contract.contract_id}</td>
                          <td className="py-2 px-3 text-gray-600 truncate max-w-[200px]" title={contract.parent_org || ''}>{contract.parent_org || '-'}</td>
                          <td className="py-2 px-3 text-center">
                            <span className={`font-bold ${contract.star_rating >= 4 ? 'text-green-600' : contract.star_rating === 3 ? 'text-yellow-600' : 'text-red-600'}`}>
                              {contract.star_rating}★
                            </span>
                          </td>
                          <td className="py-2 px-3 text-right font-mono">{contract.enrollment.toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </>
              ) : null}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// Measure Performance Tab Component
function MeasurePerformanceTab({ parentOrgs }: { parentOrgs: string[] }) {
  const [selectedPayer, setSelectedPayer] = useState<string>("Industry");
  const [avgType, setAvgType] = useState<"simple" | "weighted">("weighted");
  const [showPayerDropdown, setShowPayerDropdown] = useState(false);
  const [payerSearch, setPayerSearch] = useState("");

  // State for detail modal
  const [detailSelection, setDetailSelection] = useState<{ measureKey: string; measureName: string; year: number } | null>(null);

  // State for weight year selector
  const [weightYear, setWeightYear] = useState<number>(2026);

  // Fetch measure performance data
  const { data: perfData, isLoading } = useQuery<MeasurePerformanceData>({
    queryKey: ["measure-performance", selectedPayer, avgType],
    queryFn: async () => {
      const params = new URLSearchParams({ avg_type: avgType });
      if (selectedPayer !== "Industry") params.set("parent_org", selectedPayer);
      const res = await fetch(`${API_BASE}/api/stars/measure-performance?${params}`);
      if (!res.ok) return { error: "Failed to load", years: [], measures: [], parent_org: selectedPayer, avg_type: avgType, validation: { total_measures: 0, measures_in_2026: 0, discontinued_measures: 0 } };
      return res.json();
    },
    staleTime: 60000,
  });

  const years = perfData?.years || [];
  const measures = perfData?.measures || [];

  // Fetch contract-level detail when a cell is clicked
  const { data: detailData, isLoading: detailLoading } = useQuery<MeasurePerformanceDetail>({
    queryKey: ["measure-performance-detail", detailSelection?.measureKey, detailSelection?.year, selectedPayer],
    queryFn: async () => {
      if (!detailSelection) return null;
      const params = new URLSearchParams({
        measure_key: detailSelection.measureKey,
        year: String(detailSelection.year),
      });
      if (selectedPayer !== "Industry") params.set("parent_org", selectedPayer);
      const res = await fetch(`${API_BASE}/api/stars/measure-performance/detail?${params}`);
      if (!res.ok) return { error: "Failed to load detail" } as MeasurePerformanceDetail;
      return res.json();
    },
    enabled: !!detailSelection,
    staleTime: 60000,
  });


  return (
    <div className="space-y-4">
      {/* Header with controls */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
        <div className="flex items-center justify-between flex-wrap gap-4">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Measure Performance Over Time</h2>
            <p className="text-sm text-gray-500 mt-1">
              Average performance % by measure (2020-2026)
            </p>
          </div>

          <div className="flex items-center gap-4 flex-wrap">
            {/* Payer Selector */}
            <div className="relative">
              <button
                onClick={() => setShowPayerDropdown(!showPayerDropdown)}
                className="flex items-center gap-2 px-4 py-2 rounded-lg border-2 border-gray-300 bg-white text-gray-900 text-sm font-semibold transition-all hover:border-blue-500 hover:bg-blue-50 min-w-[200px]"
              >
                <span className="truncate">
                  {selectedPayer === "Industry" ? "Industry (All Payers)" : selectedPayer.length > 25 ? selectedPayer.substring(0, 25) + "..." : selectedPayer}
                </span>
                <ChevronDown className="w-4 h-4 text-gray-500 flex-shrink-0" />
              </button>

              {showPayerDropdown && (
                <>
                  <div className="fixed inset-0 z-40" onClick={() => { setShowPayerDropdown(false); setPayerSearch(""); }} />
                  <div className="absolute top-full left-0 mt-2 w-[380px] bg-white rounded-xl shadow-2xl border border-gray-300 p-4 z-50">
                    <input
                      type="text"
                      placeholder="Search payers..."
                      value={payerSearch}
                      onChange={(e) => setPayerSearch(e.target.value)}
                      className="w-full px-3 py-2.5 border border-gray-300 rounded-lg mb-3 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
                      autoFocus
                    />
                    <div className="max-h-72 overflow-y-auto space-y-1">
                      <button
                        onClick={() => { setSelectedPayer("Industry"); setShowPayerDropdown(false); setPayerSearch(""); }}
                        className={`w-full text-left px-4 py-3 rounded-lg text-sm font-medium transition-colors ${
                          selectedPayer === "Industry"
                            ? "bg-blue-600 text-white"
                            : "bg-gray-50 text-gray-900 hover:bg-blue-50"
                        }`}
                      >
                        Industry (All Payers)
                      </button>
                      {parentOrgs?.filter(p => p.toLowerCase().includes(payerSearch.toLowerCase())).slice(0, 20).map((org) => (
                        <button
                          key={org}
                          onClick={() => { setSelectedPayer(org); setShowPayerDropdown(false); setPayerSearch(""); }}
                          className={`w-full text-left px-4 py-3 rounded-lg text-sm font-medium transition-colors ${
                            selectedPayer === org
                              ? "bg-blue-600 text-white"
                              : "text-gray-800 hover:bg-blue-50"
                          }`}
                        >
                          {org}
                        </button>
                      ))}
                    </div>
                  </div>
                </>
              )}
            </div>

            {/* Average Type Toggle */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">Average:</span>
              <div className="flex bg-gray-100 rounded-lg p-1">
                <button
                  onClick={() => setAvgType("weighted")}
                  className={`px-3 py-1.5 rounded-md text-sm font-semibold transition-all ${
                    avgType === "weighted"
                      ? "bg-blue-600 text-white"
                      : "text-gray-500 hover:text-gray-700"
                  }`}
                >
                  Weighted
                </button>
                <button
                  onClick={() => setAvgType("simple")}
                  className={`px-3 py-1.5 rounded-md text-sm font-semibold transition-all ${
                    avgType === "simple"
                      ? "bg-blue-600 text-white"
                      : "text-gray-500 hover:text-gray-700"
                  }`}
                >
                  Simple
                </button>
              </div>
            </div>
          </div>
        </div>

        {/* Validation info */}
        {perfData?.validation && (
          <div className="mt-3 text-xs text-gray-500">
            {perfData.validation.total_measures} measures ({perfData.validation.measures_in_2026} active in 2026, {perfData.validation.discontinued_measures} discontinued)
            {avgType === "weighted" && " • Weighted by enrollment"}
          </div>
        )}
      </div>

      {/* Performance Table */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        {isLoading ? (
          <div className="flex items-center justify-center h-64">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
          </div>
        ) : perfData?.error ? (
          <div className="p-8 text-center text-gray-500">{perfData.error}</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full border-collapse text-sm">
              <thead>
                <tr className="bg-gray-50">
                  <th className="py-3 px-4 font-semibold text-gray-700 text-left border-b border-gray-200 sticky left-0 bg-gray-50 z-10 min-w-[280px]">
                    Measure
                  </th>
                  <th className="py-3 px-2 font-semibold text-gray-700 text-center border-b border-l border-gray-200 min-w-[70px]">
                    <button
                      onClick={() => {
                        // Cycle through available years
                        const idx = years.indexOf(weightYear);
                        const nextIdx = (idx + 1) % years.length;
                        setWeightYear(years[nextIdx]);
                      }}
                      className="hover:bg-gray-200 px-2 py-1 rounded transition-colors"
                      title="Click to change weight year"
                    >
                      {weightYear} Wt
                    </button>
                  </th>
                  {years.map(year => (
                    <th key={year} className="py-3 px-3 font-semibold text-gray-700 text-center border-b border-l border-gray-200 min-w-[70px]">
                      {year}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {measures.map((measure, idx) => (
                  <tr
                    key={measure.measure_key}
                    className={`hover:bg-gray-50 ${!measure.in_2026 ? 'bg-gray-50 opacity-70' : ''} ${idx > 0 && measures[idx - 1].in_2026 && !measure.in_2026 ? 'border-t-2 border-gray-300' : ''}`}
                  >
                    <td className="py-2.5 px-4 border-b border-gray-100 sticky left-0 bg-white z-10">
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-gray-400 font-mono w-8">{measure.measure_id}</span>
                        <div>
                          <div className="font-medium text-gray-900 text-sm leading-tight">
                            {measure.measure_name.length > 40 ? measure.measure_name.substring(0, 40) + "..." : measure.measure_name}
                          </div>
                          <div className="flex items-center gap-1 mt-0.5">
                            {measure.lower_is_better && (
                              <span className="text-[10px] bg-red-100 text-red-600 px-1 py-0.5 rounded">↓ lower better</span>
                            )}
                            {!measure.in_2026 && (
                              <span className="text-[10px] bg-gray-200 text-gray-600 px-1 py-0.5 rounded">discontinued</span>
                            )}
                          </div>
                        </div>
                      </div>
                    </td>
                    <td className="py-2 px-2 text-center border-b border-l border-gray-100">
                      {measure.weights && measure.weights[String(weightYear)] ? (
                        <span className={`font-medium ${measure.weights[String(weightYear)] > 1 ? 'text-blue-600' : 'text-gray-500'}`}>
                          {measure.weights[String(weightYear)] > 1 ? `${measure.weights[String(weightYear)]}x` : '1x'}
                        </span>
                      ) : (
                        <span className="text-gray-300">-</span>
                      )}
                    </td>
                    {years.map((year, yearIdx) => {
                      const yearData = measure.yearly[String(year)];
                      const value = yearData?.value;
                      const hasData = value !== null && value !== undefined;

                      // Calculate YoY change
                      const prevYear = years[yearIdx - 1];
                      const prevYearData = prevYear ? measure.yearly[String(prevYear)] : null;
                      const prevValue = prevYearData?.value;
                      const hasPrevData = prevValue !== null && prevValue !== undefined;

                      let yoyChange: number | null = null;
                      let isImprovement: boolean | null = null;

                      if (hasData && hasPrevData) {
                        yoyChange = value - prevValue;
                        // For lower-is-better, a decrease is an improvement
                        isImprovement = measure.lower_is_better ? yoyChange < 0 : yoyChange > 0;
                      }

                      // Format value - preserve decimal precision from source
                      const formatValue = (v: number) => {
                        // Check if value has meaningful decimals
                        const str = v.toString();
                        const decimalPart = str.includes('.') ? str.split('.')[1] : '';
                        if (decimalPart.length === 0) return `${v}%`;
                        if (decimalPart.length === 1 || decimalPart[1] === '0') return `${v.toFixed(1)}%`;
                        return `${v.toFixed(2)}%`;
                      };

                      return (
                        <td
                          key={year}
                          className={`py-2 px-2 text-center border-b border-l border-gray-100 ${hasData ? 'cursor-pointer hover:bg-blue-50' : ''}`}
                          title={yearData ? `N=${yearData.contract_count}, Enrollment=${yearData.enrollment.toLocaleString()} - Click for details` : 'No data'}
                          onClick={() => {
                            if (hasData) {
                              setDetailSelection({ measureKey: measure.measure_key, measureName: measure.measure_name, year });
                            }
                          }}
                        >
                          {hasData ? (
                            <div className="flex flex-col items-center">
                              <span className="font-medium text-gray-900">{formatValue(value)}</span>
                              {yoyChange !== null && Math.abs(yoyChange) >= 0.05 && (
                                <span className={`text-[10px] font-medium ${
                                  Math.abs(yoyChange) < 0.1 ? 'text-gray-400' :
                                  isImprovement ? 'text-green-600' : 'text-red-600'
                                }`}>
                                  {yoyChange > 0 ? '+' : ''}{yoyChange.toFixed(1)}
                                </span>
                              )}
                            </div>
                          ) : (
                            <span className="text-gray-300">-</span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Detail Modal */}
      {detailSelection && (
        <>
          <div
            className="fixed inset-0 bg-black/50 z-50"
            onClick={() => setDetailSelection(null)}
          />
          <div className="fixed inset-4 md:inset-10 lg:inset-20 bg-white rounded-xl shadow-2xl z-50 flex flex-col overflow-hidden">
            {/* Modal Header */}
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200 bg-gray-50">
              <div>
                <h3 className="text-lg font-semibold text-gray-900">
                  {detailSelection.measureName}
                </h3>
                <p className="text-sm text-gray-500">
                  {detailSelection.year} &middot; {selectedPayer === "Industry" ? "All Payers" : selectedPayer}
                </p>
              </div>
              <button
                onClick={() => setDetailSelection(null)}
                className="p-2 hover:bg-gray-200 rounded-lg transition-colors"
              >
                <X className="w-5 h-5 text-gray-500" />
              </button>
            </div>

            {/* Modal Body */}
            <div className="flex-1 overflow-auto p-6">
              {detailLoading ? (
                <div className="flex items-center justify-center h-64">
                  <div className="text-gray-500">Loading contract details...</div>
                </div>
              ) : detailData?.error ? (
                <div className="flex items-center justify-center h-64">
                  <div className="text-red-500">{detailData.error}</div>
                </div>
              ) : detailData?.contracts ? (
                <div>
                  <div className="mb-4 text-sm text-gray-600">
                    {detailData.contract_count} contracts
                  </div>
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left text-gray-500 border-b border-gray-200">
                        <th className="pb-2 font-medium">Contract ID</th>
                        <th className="pb-2 font-medium">Parent Organization</th>
                        <th className="pb-2 font-medium text-right">Performance</th>
                        <th className="pb-2 font-medium text-right">Enrollment</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detailData.contracts.map((contract) => (
                        <tr key={contract.contract_id} className="border-b border-gray-100 hover:bg-gray-50">
                          <td className="py-2 font-mono text-gray-900">{contract.contract_id}</td>
                          <td className="py-2 text-gray-700">{contract.parent_org || '-'}</td>
                          <td className="py-2 text-right font-medium text-gray-900">
                            {contract.performance_pct !== null ? `${contract.performance_pct.toFixed(1)}%` : '-'}
                          </td>
                          <td className="py-2 text-right text-gray-600">
                            {contract.enrollment !== null ? contract.enrollment.toLocaleString() : '-'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : null}
            </div>
          </div>
        </>
      )}
    </div>
  );
}

// Cutpoints Tab Component
function CutpointsTab({ parentOrgs }: { parentOrgs: string[] }) {
  const [starLevel, setStarLevel] = useState<2 | 3 | 4 | 5>(4);
  const [domainFilter, setDomainFilter] = useState<string>("all");
  const [selectedPayer, setSelectedPayer] = useState<string>("Industry");
  const [showPayerDropdown, setShowPayerDropdown] = useState(false);
  const [payerSearch, setPayerSearch] = useState("");
  const [sortYear, setSortYear] = useState<number>(2026);  // Year to use for sorting by measure_id

  // Fetch all cutpoints timeseries
  const { data: cutpointsData, isLoading } = useQuery<{
    years: number[];
    measures: CutpointsMeasure[];
    error?: string;
  }>({
    queryKey: ["cutpoints-timeseries"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/stars/cutpoints-timeseries`);
      if (!res.ok) return { years: [], measures: [], error: "Data not available" };
      return res.json();
    },
  });

  const years = cutpointsData?.years || [];
  const measures = cutpointsData?.measures || [];

  // Get unique domains
  const domains = [...new Set(measures.map(m => m.domain))].sort();

  // Filter measures by domain
  const filteredMeasures = domainFilter === "all"
    ? measures
    : measures.filter(m => m.domain === domainFilter);

  // Sort by measure_id for the selected sort year (C01, C02, D01, etc.)
  const sortedMeasures = [...filteredMeasures].sort((a, b) => {
    const aYearData = a.yearly[String(sortYear)];
    const bYearData = b.yearly[String(sortYear)];
    const aId = aYearData?.measure_id || 'ZZZ';  // Put measures without data at end
    const bId = bYearData?.measure_id || 'ZZZ';

    // Sort by part (C before D) then by number
    const aPart = aId.charAt(0);
    const bPart = bId.charAt(0);
    if (aPart !== bPart) return aPart.localeCompare(bPart);

    // Extract number and compare numerically
    const aNum = parseInt(aId.slice(1)) || 999;
    const bNum = parseInt(bId.slice(1)) || 999;
    return aNum - bNum;
  });

  const starColors: Record<number, string> = {
    5: "text-green-600 bg-green-100",
    4: "text-blue-600 bg-blue-100",
    3: "text-yellow-600 bg-yellow-100",
    2: "text-orange-600 bg-orange-100",
  };

  return (
    <div className="space-y-4">
      {/* Header with star level toggle */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
        <div className="flex items-center justify-between flex-wrap gap-4">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Cutpoint Trends Over Time</h2>
            <p className="text-sm text-gray-500 mt-1">
              How CMS performance thresholds have changed (2019-2026)
            </p>
          </div>

          <div className="flex items-center gap-4 flex-wrap">
            {/* Payer Selector */}
            <div className="relative">
              <button
                onClick={() => setShowPayerDropdown(!showPayerDropdown)}
                className="flex items-center gap-2 px-4 py-2 rounded-lg border-2 border-gray-300 bg-white text-gray-900 text-sm font-semibold transition-all hover:border-blue-500 hover:bg-blue-50 min-w-[200px]"
              >
                <span className="truncate">
                  {selectedPayer === "Industry" ? "Industry (All Payers)" : selectedPayer.length > 25 ? selectedPayer.substring(0, 25) + "..." : selectedPayer}
                </span>
                <ChevronDown className="w-4 h-4 text-gray-500 flex-shrink-0" />
              </button>

              {showPayerDropdown && (
                <>
                  <div className="fixed inset-0 z-40" onClick={() => { setShowPayerDropdown(false); setPayerSearch(""); }} />
                  <div className="absolute top-full left-0 mt-2 w-[380px] bg-white rounded-xl shadow-2xl border border-gray-300 p-4 z-50">
                    <input
                      type="text"
                      placeholder="Search payers..."
                      value={payerSearch}
                      onChange={(e) => setPayerSearch(e.target.value)}
                      className="w-full px-3 py-2.5 border border-gray-300 rounded-lg mb-3 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
                      autoFocus
                    />
                    <div className="max-h-72 overflow-y-auto space-y-1">
                      <button
                        onClick={() => { setSelectedPayer("Industry"); setShowPayerDropdown(false); setPayerSearch(""); }}
                        className={`w-full text-left px-4 py-3 rounded-lg text-sm font-medium transition-colors ${
                          selectedPayer === "Industry"
                            ? "bg-blue-600 text-white"
                            : "bg-gray-50 text-gray-900 hover:bg-blue-50"
                        }`}
                      >
                        Industry (All Payers)
                      </button>
                      {parentOrgs?.filter(p => p.toLowerCase().includes(payerSearch.toLowerCase())).slice(0, 20).map((org) => (
                        <button
                          key={org}
                          onClick={() => { setSelectedPayer(org); setShowPayerDropdown(false); setPayerSearch(""); }}
                          className={`w-full text-left px-4 py-3 rounded-lg text-sm font-medium transition-colors ${
                            selectedPayer === org
                              ? "bg-blue-600 text-white"
                              : "text-gray-800 hover:bg-blue-50"
                          }`}
                        >
                          {org}
                        </button>
                      ))}
                    </div>
                  </div>
                </>
              )}
            </div>

            {/* Sort by Year */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">Sort by:</span>
              <select
                value={sortYear}
                onChange={(e) => setSortYear(Number(e.target.value))}
                className="px-3 py-1.5 rounded-lg border border-gray-300 text-sm font-medium bg-white"
              >
                {years.map(y => (
                  <option key={y} value={y}>{y}</option>
                ))}
              </select>
            </div>

            {/* Domain Filter */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">Domain:</span>
              <select
                value={domainFilter}
                onChange={(e) => setDomainFilter(e.target.value)}
                className="px-3 py-1.5 rounded-lg border border-gray-300 text-sm font-medium bg-white"
              >
                <option value="all">All ({measures.length})</option>
                {domains.map(d => (
                  <option key={d} value={d}>
                    {d.replace('Part C - ', 'C: ').replace('Part D - ', 'D: ')} ({measures.filter(m => m.domain === d).length})
                  </option>
                ))}
              </select>
            </div>

            {/* Star Level Toggle */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">Show:</span>
              <div className="flex bg-gray-100 rounded-lg p-1">
                {([5, 4, 3, 2] as const).map((level) => (
                  <button
                    key={level}
                    onClick={() => setStarLevel(level)}
                    className={`px-3 py-1.5 rounded-md text-sm font-semibold transition-all ${
                      starLevel === level
                        ? starColors[level]
                        : "text-gray-500 hover:text-gray-700"
                    }`}
                  >
                    {level}★
                  </button>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Measure Rows (Full Width) */}
      {isLoading ? (
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
        </div>
      ) : cutpointsData?.error ? (
        <div className="bg-amber-50 rounded-lg border border-amber-200 p-6">
          <h3 className="font-semibold text-amber-900">Error loading cutpoints data</h3>
          <p className="text-amber-800 text-sm mt-2">{cutpointsData.error}</p>
        </div>
      ) : (
        <div className="space-y-0">
          {sortedMeasures.map((measure) => (
            <MeasureRowChart
              key={measure.measure_key}
              measure={measure}
              starLevel={starLevel}
              years={years}
              selectedPayer={selectedPayer}
            />
          ))}
        </div>
      )}
    </div>
  );
}

// Contract Tab - Shows measure performance for a single contract
function ContractTab() {
  const [selectedContract, setSelectedContract] = useState<string>("H5216");
  const [showContractDropdown, setShowContractDropdown] = useState(false);
  const [contractSearch, setContractSearch] = useState("");
  const [simValues, setSimValues] = useState<Record<string, number | null>>({});

  // Fetch list of contracts
  const { data: contractList, isLoading: loadingContracts } = useQuery<{ contracts: ContractListItem[] }>({
    queryKey: ["contract-list"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/stars/measure-performance/contracts`);
      if (!res.ok) return { contracts: [] };
      return res.json();
    },
    staleTime: 300000, // 5 minutes
  });

  // Fetch contract performance data
  const { data: contractData, isLoading: loadingData } = useQuery<ContractPerformanceData>({
    queryKey: ["contract-performance", selectedContract],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/stars/measure-performance/contract/${selectedContract}`);
      if (!res.ok) return { error: "Failed to load", contract_id: selectedContract, parent_org: null, years: [], measures: [], weighted_avg_star: null, total_weight: 0 };
      return res.json();
    },
    enabled: !!selectedContract,
    staleTime: 60000,
  });

  const contracts = contractList?.contracts || [];
  // Filter out S contracts (standalone PDP) and apply search
  const filteredContracts = contracts
    .filter(c => !c.contract_id.startsWith('S'))
    .filter(c =>
      c.contract_id.toLowerCase().includes(contractSearch.toLowerCase()) ||
      (c.parent_org?.toLowerCase().includes(contractSearch.toLowerCase()) ?? false)
    );

  // Calculate star rating from performance value using cutpoints
  // Cutpoints represent the THRESHOLD to achieve each star level:
  // - For lower-is-better: cut_X is the max value to get X stars (value <= cut_X)
  // - For higher-is-better: cut_X is the min value to get X stars (value >= cut_X)
  const calcStarFromPerformance = (value: number, cutpoints: ContractCutpoints | null, lowerIsBetter: boolean): number => {
    if (!cutpoints) return 3; // default
    const { cut_2, cut_3, cut_4, cut_5 } = cutpoints;

    if (lowerIsBetter) {
      // Lower is better: <= cut_5 = 5 stars, etc.
      // e.g., readmissions: ≤7% = 5★, ≤9% = 4★, ≤10% = 3★, ≤12% = 2★, >12% = 1★
      if (cut_5 !== null && value <= cut_5) return 5;
      if (cut_4 !== null && value <= cut_4) return 4;
      if (cut_3 !== null && value <= cut_3) return 3;
      if (cut_2 !== null && value <= cut_2) return 2;
      return 1;
    } else {
      // Higher is better: >= cut_5 = 5 stars
      if (cut_5 !== null && value >= cut_5) return 5;
      if (cut_4 !== null && value >= cut_4) return 4;
      if (cut_3 !== null && value >= cut_3) return 3;
      if (cut_2 !== null && value >= cut_2) return 2;
      return 1;
    }
  };

  // Calculate simulated weighted average (uses simValues if entered, otherwise actual)
  const calcSimulatedWeightedAvg = (): number | null => {
    if (!contractData || !contractData.measures) return null;

    let totalWeight = 0;
    let weightedSum = 0;
    let hasAnySimValue = false;

    for (const m of contractData.measures) {
      if (!m.in_2026) continue;

      const simVal = simValues[m.measure_key];
      let star: number | null = null;

      if (simVal !== undefined && simVal !== null) {
        // Use simulated value
        star = calcStarFromPerformance(simVal, m.cutpoints, m.lower_is_better);
        hasAnySimValue = true;
      } else if (m.latest_band?.star_rating) {
        // Use actual star rating
        star = m.latest_band.star_rating;
      }

      if (star !== null) {
        totalWeight += m.weight;
        weightedSum += star * m.weight;
      }
    }

    // Only return simulated avg if user has entered at least one value
    if (!hasAnySimValue) return null;
    return totalWeight > 0 ? Math.round((weightedSum / totalWeight) * 100) / 100 : null;
  };

  // Get the band range for a star rating based on cutpoints
  const getBandRange = (starRating: number, cutpoints: ContractCutpoints | null, lowerIsBetter: boolean): string => {
    if (!cutpoints) return '';
    const { cut_2, cut_3, cut_4, cut_5 } = cutpoints;

    if (lowerIsBetter) {
      // Lower is better: lower values = higher stars
      // Bands use <= for upper bound to match calcStarFromPerformance
      if (starRating === 5) return cut_5 !== null ? `≤${cut_5}%` : '';
      if (starRating === 4) return cut_5 !== null && cut_4 !== null ? `>${cut_5}% to ≤${cut_4}%` : '';
      if (starRating === 3) return cut_4 !== null && cut_3 !== null ? `>${cut_4}% to ≤${cut_3}%` : '';
      if (starRating === 2) return cut_3 !== null && cut_2 !== null ? `>${cut_3}% to ≤${cut_2}%` : '';
      return cut_2 !== null ? `>${cut_2}%` : '';
    } else {
      // Higher is better: higher values = higher stars
      if (starRating === 5) return cut_5 !== null ? `≥${cut_5}%` : '';
      if (starRating === 4) return cut_4 !== null && cut_5 !== null ? `≥${cut_4}% to <${cut_5}%` : '';
      if (starRating === 3) return cut_3 !== null && cut_4 !== null ? `≥${cut_3}% to <${cut_4}%` : '';
      if (starRating === 2) return cut_2 !== null && cut_3 !== null ? `≥${cut_2}% to <${cut_3}%` : '';
      return cut_2 !== null ? `<${cut_2}%` : '';
    }
  };

  // Check if value is outside expected band (CAHPS statistical adjustment)
  const isOutsideBand = (value: number, starRating: number, cutpoints: ContractCutpoints | null, lowerIsBetter: boolean): boolean => {
    if (!cutpoints) return false;
    const expectedStar = calcStarFromPerformance(value, cutpoints, lowerIsBetter);
    return expectedStar !== starRating;
  };

  // Star rating colors
  const starColor = (rating: number | null) => {
    if (rating === null) return "text-gray-400";
    if (rating === 5) return "text-green-600";
    if (rating === 4) return "text-blue-600";
    if (rating === 3) return "text-yellow-600";
    if (rating === 2) return "text-orange-600";
    return "text-red-600";
  };

  // Star rating background colors
  const starBgColor = (rating: number | null) => {
    if (rating === null) return "bg-gray-100";
    if (rating === 5) return "bg-green-100";
    if (rating === 4) return "bg-blue-100";
    if (rating === 3) return "bg-yellow-100";
    if (rating === 2) return "bg-orange-100";
    return "bg-red-100";
  };

  // Band position styling
  const bandPositionStyle = (position: string | null) => {
    if (position === 'top') return { bg: 'bg-green-100', text: 'text-green-800', label: 'Top' };
    if (position === 'middle') return { bg: 'bg-yellow-100', text: 'text-yellow-800', label: 'Mid' };
    if (position === 'bottom') return { bg: 'bg-red-100', text: 'text-red-800', label: 'Bot' };
    return { bg: 'bg-gray-100', text: 'text-gray-600', label: '-' };
  };

  const simulatedAvg = calcSimulatedWeightedAvg();

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-gray-900">Contract Performance</h2>

        <div className="flex items-center gap-4">
          {/* Contract Selector */}
          <div className="relative">
          <button
            onClick={() => setShowContractDropdown(!showContractDropdown)}
            className="flex items-center gap-2 px-4 py-2 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 min-w-[280px] justify-between"
          >
            <span className={selectedContract ? "text-gray-900" : "text-gray-500"}>
              {selectedContract ? `${selectedContract}${contractData?.parent_org ? ` - ${contractData.parent_org}` : ''}` : "Select a contract..."}
            </span>
            <ChevronDown className="w-4 h-4 text-gray-400" />
          </button>

          {showContractDropdown && (
            <div className="absolute z-50 mt-1 w-[400px] bg-white border border-gray-200 rounded-lg shadow-lg max-h-80 overflow-hidden right-0">
              <div className="p-2 border-b border-gray-100">
                <input
                  type="text"
                  placeholder="Search contracts..."
                  value={contractSearch}
                  onChange={(e) => setContractSearch(e.target.value)}
                  className="w-full px-3 py-2 text-sm border border-gray-200 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
                  autoFocus
                />
              </div>
              <div className="overflow-y-auto max-h-60">
                {loadingContracts ? (
                  <div className="p-4 text-center text-gray-500">Loading...</div>
                ) : filteredContracts.length === 0 ? (
                  <div className="p-4 text-center text-gray-500">No contracts found</div>
                ) : (
                  filteredContracts.slice(0, 100).map((contract) => (
                    <button
                      key={contract.contract_id}
                      onClick={() => {
                        setSelectedContract(contract.contract_id);
                        setShowContractDropdown(false);
                        setContractSearch("");
                      }}
                      className={`w-full px-3 py-2 text-left hover:bg-blue-50 flex justify-between items-center ${
                        selectedContract === contract.contract_id ? "bg-blue-100" : ""
                      }`}
                    >
                      <div>
                        <span className="font-medium text-gray-900">{contract.contract_id}</span>
                        {contract.parent_org && (
                          <span className="text-gray-500 ml-2 text-sm">{contract.parent_org}</span>
                        )}
                      </div>
                      <span className="text-xs text-gray-400">{formatEnrollment(contract.enrollment)}</span>
                    </button>
                  ))
                )}
              </div>
            </div>
          )}
        </div>
        </div>
      </div>

      {/* No contract selected */}
      {!selectedContract && (
        <div className="text-center py-12 text-gray-500">
          Select a contract to view measure performance
        </div>
      )}

      {/* Loading */}
      {selectedContract && loadingData && (
        <div className="text-center py-12 text-gray-500">Loading contract data...</div>
      )}

      {/* Error */}
      {contractData?.error && (
        <div className="text-center py-12 text-red-500">{contractData.error}</div>
      )}

      {/* Contract Performance Table */}
      {selectedContract && contractData && !contractData.error && contractData.measures.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200">
                <th className="text-left py-3 px-2 font-medium text-gray-600 sticky left-0 bg-white w-64">Measure</th>
                {contractData.years.map((year) => (
                  <th key={year} className="text-center py-3 px-2 font-medium text-gray-600 min-w-[80px]">
                    {year}
                  </th>
                ))}
                <th className="text-center py-3 px-2 font-medium text-gray-600 min-w-[100px]">
                  Band
                </th>
                <th className="text-center py-3 px-2 font-medium text-purple-600 min-w-[80px] bg-purple-50">
                  2027 Input
                </th>
                <th className="text-center py-3 px-2 font-medium text-purple-600 min-w-[70px] bg-purple-50">
                  2027 ★
                </th>
              </tr>
            </thead>
            <tbody>
              {/* Overall Star Rating Row (rounded to half-star per CMS methodology) */}
              <tr className="bg-gradient-to-r from-blue-50 to-blue-100 border-b-2 border-blue-200 font-semibold">
                <td className="py-3 px-2 sticky left-0 bg-gradient-to-r from-blue-50 to-blue-100 text-blue-900">
                  Overall Star Rating
                </td>
                {contractData.years.map((year) => {
                  const avg = contractData.yearly_weighted_avgs?.[year] ?? null;
                  // CMS rounds to nearest half-star
                  const halfStar = avg !== null ? Math.round(avg * 2) / 2 : null;
                  return (
                    <td key={year} className="text-center py-3 px-2">
                      {halfStar !== null ? (
                        <span className={`text-lg font-bold ${starColor(Math.ceil(halfStar))}`}>
                          {halfStar % 1 === 0 ? halfStar : halfStar.toFixed(1)}★
                        </span>
                      ) : (
                        <span className="text-gray-400">-</span>
                      )}
                    </td>
                  );
                })}
                <td className="text-center py-3 px-2"></td>
                <td className="text-center py-3 px-2 bg-purple-50"></td>
                <td className="text-center py-3 px-2 bg-purple-50">
                  {simulatedAvg !== null && (() => {
                    const halfStar = Math.round(simulatedAvg * 2) / 2;
                    return (
                      <span className={`text-lg font-bold ${starColor(Math.ceil(halfStar))}`}>
                        {halfStar % 1 === 0 ? halfStar : halfStar.toFixed(1)}★
                      </span>
                    );
                  })()}
                </td>
              </tr>
              {/* Raw Weighted Average Row */}
              <tr className="bg-gray-50 border-b-2 border-gray-300">
                <td className="py-2 px-2 sticky left-0 bg-gray-50 text-gray-700 font-medium">
                  Weighted Average
                </td>
                {contractData.years.map((year) => {
                  const avg = contractData.yearly_weighted_avgs?.[year] ?? null;
                  return (
                    <td key={year} className="text-center py-2 px-2">
                      {avg !== null ? (
                        <span className="text-gray-700 font-medium">
                          {avg.toFixed(2)}
                        </span>
                      ) : (
                        <span className="text-gray-400">-</span>
                      )}
                    </td>
                  );
                })}
                <td className="text-center py-2 px-2"></td>
                <td className="text-center py-2 px-2 bg-purple-50"></td>
                <td className="text-center py-2 px-2 bg-purple-50">
                  {simulatedAvg !== null && (
                    <span className="text-purple-700 font-medium">
                      {simulatedAvg.toFixed(2)}
                    </span>
                  )}
                </td>
              </tr>
              {/* Measure Rows */}
              {contractData.measures.map((measure) => {
                const latestBand = measure.latest_band;

                return (
                  <tr
                    key={measure.measure_key}
                    className={`border-b border-gray-100 hover:bg-gray-50 ${!measure.in_2026 ? 'bg-gray-50' : ''}`}
                  >
                    {/* Measure Name */}
                    <td className="py-2 px-2 sticky left-0 bg-white">
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-gray-400 font-mono w-8">{measure.measure_id}</span>
                        <span className={`text-gray-900 ${!measure.in_2026 ? 'text-gray-500' : ''}`}>
                          {measure.measure_name}
                        </span>
                        {measure.lower_is_better && (
                          <span className="text-[10px] px-1 py-0.5 bg-blue-100 text-blue-700 rounded">↓</span>
                        )}
                        {measure.weight > 1 && (
                          <span className="text-[10px] px-1 py-0.5 bg-purple-100 text-purple-700 rounded">{measure.weight}x</span>
                        )}
                        {!measure.in_2026 && (
                          <span className="text-[10px] px-1 py-0.5 bg-gray-200 text-gray-600 rounded">Disc.</span>
                        )}
                      </div>
                    </td>

                    {/* Year columns */}
                    {contractData.years.map((year) => {
                      const yearData = measure.yearly[String(year)];
                      const value = yearData?.value;
                      const starRating = yearData?.star_rating;

                      return (
                        <td key={year} className="py-2 px-2 text-center">
                          {value !== null && value !== undefined ? (
                            <div className="flex flex-col items-center">
                              <span className="font-medium text-gray-900">
                                {value < 1 ? value.toFixed(2) : Math.round(value)}
                                {value >= 1 ? '%' : ''}
                              </span>
                              {starRating && (
                                <span className={`text-xs ${starColor(starRating)}`}>
                                  {starRating}★
                                </span>
                              )}
                            </div>
                          ) : (
                            <span className="text-gray-300">-</span>
                          )}
                        </td>
                      );
                    })}

                    {/* Band Column - shows the % range for current star rating */}
                    <td className="py-2 px-2 text-center">
                      {latestBand && measure.cutpoints ? (
                        (() => {
                          const latestYear = Math.max(...contractData.years);
                          const latestValue = measure.yearly[String(latestYear)]?.value;
                          const outsideBand = latestValue !== null && latestValue !== undefined &&
                            isOutsideBand(latestValue, latestBand.star_rating, measure.cutpoints, measure.lower_is_better);
                          const bandRange = getBandRange(latestBand.star_rating, measure.cutpoints, measure.lower_is_better);

                          return (
                            <div className="flex flex-col items-center gap-0.5">
                              <span className="text-xs text-gray-700 font-medium">
                                {bandRange}
                              </span>
                              {outsideBand && (
                                <span className="text-[9px] px-1 py-0.5 bg-amber-100 text-amber-700 rounded" title="Value outside expected band (CAHPS statistical adjustment)">
                                  adj
                                </span>
                              )}
                            </div>
                          );
                        })()
                      ) : (
                        <span className="text-gray-300">-</span>
                      )}
                    </td>

                    {/* 2027 Input Column */}
                    <td className="py-2 px-2 text-center bg-purple-50">
                      {measure.in_2026 && measure.cutpoints ? (
                        <input
                          type="number"
                          min={0}
                          max={100}
                          step={0.1}
                          placeholder="%"
                          value={simValues[measure.measure_key] ?? ''}
                          onChange={(e) => {
                            const val = e.target.value === '' ? null : parseFloat(e.target.value);
                            setSimValues(prev => ({
                              ...prev,
                              [measure.measure_key]: val
                            }));
                          }}
                          className="w-16 px-1 py-0.5 text-center text-sm border border-purple-200 rounded focus:outline-none focus:ring-1 focus:ring-purple-500 bg-white"
                        />
                      ) : (
                        <span className="text-gray-300">-</span>
                      )}
                    </td>

                    {/* 2027 Output Column - shows star rating based on input */}
                    <td className="py-2 px-2 text-center bg-purple-50">
                      {measure.in_2026 && measure.cutpoints ? (
                        (() => {
                          const simVal = simValues[measure.measure_key];
                          if (simVal === undefined || simVal === null) {
                            return <span className="text-gray-300">-</span>;
                          }
                          const simStar = calcStarFromPerformance(simVal, measure.cutpoints, measure.lower_is_better);
                          const origStar = latestBand?.star_rating;
                          const changed = origStar && simStar !== origStar;

                          return (
                            <div className="flex flex-col items-center">
                              <span className={`text-sm font-bold ${starColor(simStar)}`}>
                                {simStar}★
                              </span>
                              {changed && (
                                <span className={`text-[10px] font-medium ${simStar > origStar! ? 'text-green-600' : 'text-red-600'}`}>
                                  {simStar > origStar! ? '↑' : '↓'} from {origStar}★
                                </span>
                              )}
                            </div>
                          );
                        })()
                      ) : (
                        <span className="text-gray-300">-</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* No measures */}
      {selectedContract && contractData && !contractData.error && contractData.measures.length === 0 && (
        <div className="text-center py-12 text-gray-500">No measures found for this contract</div>
      )}
    </div>
  );
}

export default function StarsPage() {
  // Tab state
  const [activeTab, setActiveTab] = useState<StarsTab>("home");

  // Distribution table state
  const [selectedPayers, setSelectedPayers] = useState<string[]>([]);
  const [showPayerPopup, setShowPayerPopup] = useState(false);
  const [payerSearch, setPayerSearch] = useState("");
  const [starYear, setStarYear] = useState(2025);
  const [showYearPopup, setShowYearPopup] = useState(false);

  // Graph state
  const [graphPayers, setGraphPayers] = useState<string[]>([]);
  const [showGraphPayerPopup, setShowGraphPayerPopup] = useState(false);
  const [graphPayerSearch, setGraphPayerSearch] = useState("");
  const [showIndustryLine, setShowIndustryLine] = useState(true);
  const [yearRange, setYearRange] = useState<number | null>(null);

  // Filter state for graph
  const [selectedPlanTypes, setSelectedPlanTypes] = useState<string[]>([]);
  const [selectedGroupTypes, setSelectedGroupTypes] = useState<string[]>([]);
  const [selectedSnpTypes, setSelectedSnpTypes] = useState<string[]>([]);
  const [showFilterPopup, setShowFilterPopup] = useState(false);

  // Distribution over time state
  const [distTimeParentOrg, setDistTimeParentOrg] = useState<string>("Industry");
  const [distTimePlanTypes, setDistTimePlanTypes] = useState<string[]>([]);
  const [distTimeGroupTypes, setDistTimeGroupTypes] = useState<string[]>([]);
  const [distTimeSnpTypes, setDistTimeSnpTypes] = useState<string[]>([]);
  const [distTimeYearRange, setDistTimeYearRange] = useState<number | null>(null);
  const [showDistTimePayerPopup, setShowDistTimePayerPopup] = useState(false);
  const [showDistTimeFilterPopup, setShowDistTimeFilterPopup] = useState(false);
  const [distTimePayerSearch, setDistTimePayerSearch] = useState("");

  // Available star years (2014-2026)
  const availableStarYears = Array.from({ length: 13 }, (_, i) => 2026 - i);

  // Fetch filter options for parent orgs
  const { data: filterOptions } = useQuery<FilterOptions>({
    queryKey: ["enrollment-filters"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/v2/enrollment/filters`);
      return res.json();
    },
  });

  // Fetch stars distribution
  const { data: distributionData, isLoading } = useQuery({
    queryKey: ["stars-distribution", selectedPayers, starYear],
    queryFn: () => starsAPI.getDistribution(selectedPayers, starYear),
  });

  // Build query params for 4+ timeseries
  const buildTimeseriesParams = () => {
    const params = new URLSearchParams();
    if (graphPayers.length > 0) params.set("parent_orgs", graphPayers.join("|"));
    if (selectedPlanTypes.length > 0) params.set("plan_types", selectedPlanTypes.join(","));
    if (selectedGroupTypes.length > 0) params.set("group_types", selectedGroupTypes.join(","));
    if (selectedSnpTypes.length > 0) params.set("snp_types", selectedSnpTypes.join(","));
    params.set("include_total", "true");
    return params.toString();
  };

  // Fetch 4+ star time series
  const { data: rawTimeseriesData, isLoading: timeseriesLoading } = useQuery<FourPlusTimeseriesData>({
    queryKey: ["stars-fourplus-timeseries", graphPayers, selectedPlanTypes, selectedGroupTypes, selectedSnpTypes],
    queryFn: async () => {
      const params = buildTimeseriesParams();
      const res = await fetch(`${API_BASE}/api/stars/fourplus-timeseries?${params}`);
      return res.json();
    },
  });

  // Fetch distribution over time
  const { data: distTimeData, isLoading: distTimeLoading } = useQuery<{
    years: number[];
    distribution: Record<string, number[]>;
    totals: number[];
    error?: string;
  }>({
    queryKey: ["stars-distribution-timeseries", distTimeParentOrg, distTimePlanTypes, distTimeGroupTypes, distTimeSnpTypes],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (distTimeParentOrg !== "Industry") params.set("parent_org", distTimeParentOrg);
      if (distTimePlanTypes.length > 0) params.set("plan_types", distTimePlanTypes.join(","));
      if (distTimeGroupTypes.length > 0) params.set("group_types", distTimeGroupTypes.join(","));
      if (distTimeSnpTypes.length > 0) params.set("snp_types", distTimeSnpTypes.join(","));
      const res = await fetch(`${API_BASE}/api/stars/distribution-timeseries?${params}`);
      return res.json();
    },
  });

  // Filter distribution years by range
  const distTimeYears = distTimeData?.years || [];
  const filteredDistTimeYears = distTimeYearRange
    ? distTimeYears.slice(-distTimeYearRange)
    : distTimeYears;
  const distTimeStartIdx = distTimeYearRange
    ? Math.max(0, distTimeYears.length - distTimeYearRange)
    : 0;

  // Filter out Industry if not showing it
  const timeseriesData = rawTimeseriesData ? {
    ...rawTimeseriesData,
    series: rawTimeseriesData.series
      ? Object.fromEntries(
          Object.entries(rawTimeseriesData.series).filter(([key]) =>
            showIndustryLine || key !== 'Industry'
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

  // Filter payers for search (distribution table)
  const filteredPayers = filterOptions?.parent_orgs?.filter(
    (p) =>
      p.toLowerCase().includes(payerSearch.toLowerCase()) &&
      !selectedPayers.includes(p)
  ).slice(0, payerSearch ? 30 : 15) || [];

  // Filter payers for graph
  const filteredGraphPayers = filterOptions?.parent_orgs?.filter(
    (p) =>
      p.toLowerCase().includes(graphPayerSearch.toLowerCase()) &&
      !graphPayers.includes(p)
  ).slice(0, graphPayerSearch ? 30 : 15) || [];

  const addPayer = (payer: string) => {
    setSelectedPayers([...selectedPayers, payer]);
    setShowPayerPopup(false);
    setPayerSearch("");
  };

  const removePayer = (payer: string) => {
    setSelectedPayers(selectedPayers.filter((p) => p !== payer));
  };

  const addGraphPayer = (payer: string) => {
    setGraphPayers([...graphPayers, payer]);
    setShowGraphPayerPopup(false);
    setGraphPayerSearch("");
  };

  const removeGraphPayer = (payer: string) => {
    setGraphPayers(graphPayers.filter((p) => p !== payer));
  };

  // Get column order (Industry first, then selected payers)
  const columns = distributionData?.columns
    ? ["Industry", ...selectedPayers.filter((p) => distributionData.columns[p])]
    : ["Industry"];

  // Star bands from high to low (2.5 is lowest with meaningful data)
  const starBands = [5, 4.5, 4, 3.5, 3, 2.5];

  const hasFilters = selectedPlanTypes.length > 0 || selectedGroupTypes.length > 0 || selectedSnpTypes.length > 0;
  const filterCount = selectedPlanTypes.length + selectedGroupTypes.length + selectedSnpTypes.length;

  const clearFilters = () => {
    setSelectedPlanTypes([]);
    setSelectedGroupTypes([]);
    setSelectedSnpTypes([]);
  };

  return (
    <div className="space-y-4">
      {/* Tab Navigation */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-1.5 flex gap-1">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-all ${
              activeTab === tab.id
                ? "bg-gray-900 text-white shadow-md"
                : "text-gray-600 hover:bg-gray-100 hover:text-gray-900"
            }`}
          >
            {tab.icon}
            <span>{tab.label}</span>
          </button>
        ))}
      </div>

      {/* HOME TAB */}
      {activeTab === "home" && (
        <>
      {/* 4+ Star Time Series Graph */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-gray-900">
            4+ Star Enrollment Over Time
            {hasFilters && <span className="text-sm font-normal text-gray-500 ml-2">(filtered)</span>}
          </h2>
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

        {/* Graph Control Bar */}
        <div className="flex items-center gap-3 flex-wrap mb-4">
          {/* Filter by Type Button */}
          <div className="relative">
            <button
              onClick={() => setShowFilterPopup(!showFilterPopup)}
              className={`flex items-center gap-2 px-4 py-2.5 rounded-lg font-medium transition-all shadow-sm ${
                hasFilters
                  ? "bg-blue-600 text-white hover:bg-blue-700"
                  : "bg-gray-100 text-gray-700 hover:bg-gray-200"
              }`}
            >
              <Filter className="w-4 h-4" />
              <span>Filter by Type</span>
              {filterCount > 0 && (
                <span className="bg-white text-blue-600 text-xs px-1.5 py-0.5 rounded-full font-bold">{filterCount}</span>
              )}
              <ChevronDown className={`w-4 h-4 transition-transform ${showFilterPopup ? 'rotate-180' : ''}`} />
            </button>

            {showFilterPopup && (
              <div className="absolute top-full left-0 mt-2 w-80 bg-white rounded-xl shadow-xl border border-gray-200 p-5 z-50">
                <div className="flex justify-between items-center mb-4">
                  <span className="font-semibold text-gray-900">Filter by Type</span>
                  {hasFilters && (
                    <button onClick={clearFilters} className="text-sm text-blue-600 hover:text-blue-800 font-medium">
                      Clear all
                    </button>
                  )}
                </div>

                {/* Group Type */}
                <div className="mb-4">
                  <label className="block text-xs font-medium text-gray-500 mb-2 uppercase tracking-wide">Market Segment</label>
                  <div className="flex gap-2">
                    {['Individual', 'Group'].map((type) => (
                      <button
                        key={type}
                        onClick={() => setSelectedGroupTypes(prev =>
                          prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type]
                        )}
                        className={`flex-1 px-3 py-2 rounded-lg text-sm font-medium transition-all ${
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
                <div className="mb-4">
                  <label className="block text-xs font-medium text-gray-500 mb-2 uppercase tracking-wide">Plan Type</label>
                  <div className="flex flex-wrap gap-2">
                    {['HMO/HMOPOS', 'PPO', 'PFFS'].map((type) => (
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
                        {type === 'HMO/HMOPOS' ? 'HMO' : type}
                      </button>
                    ))}
                  </div>
                </div>

                {/* SNP Type */}
                <div className="mb-4">
                  <label className="block text-xs font-medium text-gray-500 mb-2 uppercase tracking-wide">Special Needs</label>
                  <div className="flex gap-2">
                    {['SNP', 'Non-SNP'].map((type) => (
                      <button
                        key={type}
                        onClick={() => setSelectedSnpTypes(prev =>
                          prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type]
                        )}
                        className={`flex-1 px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                          selectedSnpTypes.includes(type)
                            ? "bg-orange-600 text-white shadow-md"
                            : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                        }`}
                      >
                        {type}
                      </button>
                    ))}
                  </div>
                </div>

                <button
                  onClick={() => setShowFilterPopup(false)}
                  className="w-full py-2.5 bg-gray-900 hover:bg-gray-800 text-white rounded-lg text-sm font-medium transition-colors"
                >
                  Apply
                </button>
              </div>
            )}
          </div>

          {/* Add Payer Button for Graph */}
          <div className="relative">
            <button
              onClick={() => setShowGraphPayerPopup(!showGraphPayerPopup)}
              className="flex items-center gap-2 px-4 py-2.5 rounded-lg bg-gray-100 text-gray-700 hover:bg-gray-200 font-medium transition-all shadow-sm"
            >
              <Plus className="w-4 h-4" />
              <span>Add Payer</span>
            </button>

            {showGraphPayerPopup && (
              <div className="absolute top-full left-0 mt-2 w-[420px] bg-white rounded-xl shadow-xl border border-gray-200 p-4 z-50">
                <input
                  type="text"
                  placeholder="Search payers (e.g., United, Humana, CVS)..."
                  value={graphPayerSearch}
                  onChange={(e) => setGraphPayerSearch(e.target.value)}
                  className="w-full px-4 py-3 border border-gray-300 rounded-lg mb-3 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
                  autoFocus
                />
                <div className="max-h-72 overflow-y-auto">
                  {filteredGraphPayers.map((org, idx) => (
                    <button
                      key={org}
                      onClick={() => addGraphPayer(org)}
                      className="w-full text-left px-4 py-3 hover:bg-blue-50 rounded-lg text-sm transition-colors flex items-center justify-between group"
                    >
                      <span className="font-medium text-gray-800">{org}</span>
                      <span className="text-xs text-gray-400 group-hover:text-blue-600">#{idx + 1}</span>
                    </button>
                  ))}
                  {filteredGraphPayers.length === 0 && graphPayerSearch && (
                    <div className="text-gray-500 text-sm px-4 py-3">No payers found matching "{graphPayerSearch}"</div>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* Industry Chip - show when payers are selected */}
          {graphPayers.length > 0 && showIndustryLine && (
            <span className="inline-flex items-center gap-1.5 px-3 py-2 rounded-lg bg-gray-200 text-gray-800 text-sm font-medium">
              Industry
              <button onClick={() => setShowIndustryLine(false)} className="hover:text-gray-600 ml-1">
                <X className="w-4 h-4" />
              </button>
            </span>
          )}

          {/* Add Industry back button */}
          {graphPayers.length > 0 && !showIndustryLine && (
            <button
              onClick={() => setShowIndustryLine(true)}
              className="inline-flex items-center gap-1.5 px-3 py-2 rounded-lg bg-gray-100 text-gray-600 text-sm font-medium hover:bg-gray-200 transition-colors"
            >
              <Plus className="w-4 h-4" />
              Industry
            </button>
          )}

          {/* Selected Graph Payers */}
          {graphPayers.map((payer) => (
            <span
              key={payer}
              className="inline-flex items-center gap-1.5 px-3 py-2 rounded-lg bg-purple-100 text-purple-800 text-sm font-medium"
            >
              {payer.length > 30 ? payer.substring(0, 30) + "..." : payer}
              <button onClick={() => removeGraphPayer(payer)} className="hover:text-purple-600 ml-1">
                <X className="w-4 h-4" />
              </button>
            </span>
          ))}
        </div>

        {/* Chart */}
        <div className="h-80" style={{ minHeight: '320px', minWidth: '0' }}>
          {timeseriesLoading ? (
            <div className="flex items-center justify-center h-full">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            </div>
          ) : timeseriesData?.error ? (
            <div className="flex items-center justify-center h-full text-gray-500">{timeseriesData.error}</div>
          ) : chartData.length > 0 ? (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="year" tick={{ fill: '#6b7280' }} />
                <YAxis
                  tickFormatter={formatPercent}
                  tick={{ fill: '#6b7280' }}
                  domain={[0, 100]}
                  width={50}
                />
                <Tooltip
                  formatter={(value) => [formatPercent(value as number), ""]}
                  labelFormatter={(year) => `Star Year: ${year}`}
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

      {/* Click outside to close graph popups */}
      {(showFilterPopup || showGraphPayerPopup) && (
        <div
          className="fixed inset-0 z-40"
          onClick={() => {
            setShowFilterPopup(false);
            setShowGraphPayerPopup(false);
            setGraphPayerSearch("");
          }}
        />
      )}

      {/* Click outside to close table popups */}
      {(showPayerPopup || showYearPopup) && (
        <div
          className="fixed inset-0 z-40"
          onClick={() => {
            setShowPayerPopup(false);
            setShowYearPopup(false);
            setPayerSearch("");
          }}
        />
      )}

      {/* Star Rating Distribution */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        {/* Header with controls */}
        <div className="px-5 py-4 border-b border-gray-200 flex items-center gap-3 flex-wrap">
          <h2 className="text-lg font-semibold text-gray-900 mr-2">Star Rating Distribution</h2>

          {/* Year Selector */}
          <div className="relative">
            <button
              onClick={() => setShowYearPopup(!showYearPopup)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-gray-100 text-gray-700 text-sm font-medium transition-all hover:bg-gray-200"
            >
              <span>{starYear}</span>
              <ChevronDown className="w-3.5 h-3.5" />
            </button>

            {showYearPopup && (
              <div className="absolute top-full left-0 mt-2 w-48 bg-white rounded-xl shadow-xl border border-gray-200 py-2 z-50 max-h-72 overflow-y-auto">
                {availableStarYears.map((yr) => (
                  <button
                    key={yr}
                    onClick={() => {
                      setStarYear(yr);
                      setShowYearPopup(false);
                    }}
                    className={`w-full text-left px-4 py-2 text-sm transition-colors flex justify-between items-center ${
                      starYear === yr
                        ? "bg-gray-100 font-semibold text-gray-900"
                        : "hover:bg-gray-50 text-gray-700"
                    }`}
                  >
                    <span>{yr} Stars</span>
                    <span className="text-xs text-gray-400">{yr + 1} payment</span>
                  </button>
                ))}
              </div>
            )}
          </div>

          <span className="text-gray-400">|</span>

          {/* Add Payer Button */}
          <div className="relative">
            <button
              onClick={() => setShowPayerPopup(!showPayerPopup)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-gray-100 text-gray-700 text-sm font-medium transition-all hover:bg-gray-200"
            >
              <Plus className="w-3.5 h-3.5" />
              <span>Add Payer</span>
            </button>

            {showPayerPopup && (
              <div className="absolute top-full left-0 mt-2 w-[380px] bg-white rounded-xl shadow-xl border border-gray-200 p-4 z-50">
                <input
                  type="text"
                  placeholder="Search payers..."
                  value={payerSearch}
                  onChange={(e) => setPayerSearch(e.target.value)}
                  className="w-full px-3 py-2.5 border border-gray-300 rounded-lg mb-3 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
                  autoFocus
                />
                <div className="max-h-64 overflow-y-auto">
                  {filteredPayers.map((org) => (
                    <button
                      key={org}
                      onClick={() => addPayer(org)}
                      className="w-full text-left px-3 py-2.5 hover:bg-blue-50 rounded-lg text-sm transition-colors"
                    >
                      <span className="font-medium text-gray-800">{org}</span>
                    </button>
                  ))}
                  {filteredPayers.length === 0 && payerSearch && (
                    <div className="text-gray-500 text-sm px-3 py-2">No payers found</div>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* Selected Payers as chips */}
          {selectedPayers.map((payer) => (
            <span
              key={payer}
              className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full bg-purple-100 text-purple-800 text-sm font-medium"
            >
              {payer.length > 25 ? payer.substring(0, 25) + "..." : payer}
              <button onClick={() => removePayer(payer)} className="hover:text-purple-600">
                <X className="w-3.5 h-3.5" />
              </button>
            </span>
          ))}
        </div>
        {isLoading ? (
          <div className="flex items-center justify-center h-64">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
          </div>
        ) : distributionData?.error ? (
          <div className="p-8 text-center text-gray-500">{distributionData.error}</div>
        ) : (
          <table className="w-full border-collapse">
            <thead>
              <tr className="bg-gray-50">
                <th className="py-2.5 px-4 font-medium text-gray-500 text-sm text-left border-b border-gray-200 w-28">
                  Rating
                </th>
                {columns.map((col) => (
                  <th
                    key={col}
                    className="py-2.5 px-5 font-semibold text-gray-900 text-sm text-center border-l border-b border-gray-200"
                    style={{ minWidth: '160px' }}
                  >
                    {col.length > 25 ? `${col.substring(0, 25)}...` : col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {starBands.map((band) => (
                <tr key={band} className="hover:bg-gray-50">
                  <td className="py-3 px-4 font-medium text-gray-700 whitespace-nowrap border-b border-gray-100">
                    {band % 1 === 0 ? band : band.toFixed(1)} Star{band !== 1 ? "s" : ""}
                  </td>
                  {columns.map((col) => {
                    const colData = distributionData?.columns?.[col];
                    // JSON keys are strings like "5.0", "4.5", etc.
                    const bandKey = band % 1 === 0 ? `${band}.0` : band.toString();
                    const distribution = colData?.distribution as Record<string, { pct: number; enrollment: number }> | undefined;
                    const bandData = distribution?.[bandKey];
                    return (
                      <td key={col} className="py-3 px-6 text-center whitespace-nowrap border-l border-b border-gray-100">
                        {bandData ? (
                          <>
                            <span className="font-semibold text-gray-900">{bandData.pct.toFixed(1)}%</span>
                            <span className="text-gray-400 text-sm ml-2">({formatNumber(bandData.enrollment)})</span>
                          </>
                        ) : (
                          <span className="text-gray-300">-</span>
                        )}
                      </td>
                    );
                  })}
                </tr>
              ))}
              {/* 4+ Stars Row */}
              <tr className="bg-green-50">
                <td className="py-3 px-4 font-semibold text-green-800 whitespace-nowrap border-b border-gray-200">
                  4+ Stars
                </td>
                {columns.map((col) => {
                  const colData = distributionData?.columns?.[col];
                  if (!colData) return <td key={col} className="py-3 px-6 text-center border-l border-b border-gray-200 text-gray-300">-</td>;
                  // Sum all ratings >= 4 (4, 4.5, 5)
                  const fourPlus = Object.entries(colData.distribution)
                    .filter(([band]) => parseFloat(band) >= 4)
                    .reduce((sum, [, data]) => sum + (data?.enrollment || 0), 0);
                  const pct = colData.total_enrollment > 0 ? (fourPlus / colData.total_enrollment * 100) : 0;
                  return (
                    <td key={col} className="py-3 px-6 text-center whitespace-nowrap border-l border-b border-gray-200">
                      <span className="font-bold text-green-700">{pct.toFixed(1)}%</span>
                      <span className="text-green-600 text-sm ml-2">({formatNumber(fourPlus)})</span>
                    </td>
                  );
                })}
              </tr>
              {/* Total Row */}
              <tr className="bg-gray-50">
                <td className="py-3 px-4 font-semibold text-gray-700 whitespace-nowrap">Total</td>
                {columns.map((col) => {
                  const colData = distributionData?.columns?.[col];
                  return (
                    <td key={col} className="py-3 px-6 text-center whitespace-nowrap border-l border-gray-200">
                      {colData ? (
                        <span className="font-semibold text-gray-900">{formatNumber(colData.total_enrollment)}</span>
                      ) : (
                        <span className="text-gray-300">-</span>
                      )}
                    </td>
                  );
                })}
              </tr>
            </tbody>
          </table>
        )}
      </div>

      {/* Distribution Over Time */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
        {/* Header */}
        <div className="px-5 py-4 border-b border-gray-200 flex items-center gap-3 flex-wrap">
          <h2 className="text-lg font-semibold text-gray-900 mr-2">Distribution Over Time</h2>

          {/* Year Range */}
          <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-1">
            {[
              { value: null, label: "All" },
              { value: 10, label: "10Y" },
              { value: 5, label: "5Y" },
              { value: 3, label: "3Y" },
            ].map((opt) => (
              <button
                key={opt.label}
                onClick={() => setDistTimeYearRange(opt.value)}
                className={`px-2.5 py-1 rounded text-sm font-medium transition-all ${
                  distTimeYearRange === opt.value
                    ? "bg-white text-gray-900 shadow-sm"
                    : "text-gray-600 hover:text-gray-900"
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>

          <span className="text-gray-300">|</span>

          {/* Payer Selector */}
          <div className="relative">
            <button
              onClick={() => setShowDistTimePayerPopup(!showDistTimePayerPopup)}
              className="flex items-center gap-2 px-4 py-2 rounded-lg border-2 border-gray-300 bg-white text-gray-900 text-sm font-semibold transition-all hover:border-blue-500 hover:bg-blue-50"
            >
              <span>{distTimeParentOrg === "Industry" ? "Industry" : distTimeParentOrg.length > 25 ? distTimeParentOrg.substring(0, 25) + "..." : distTimeParentOrg}</span>
              <ChevronDown className="w-4 h-4 text-gray-500" />
            </button>

            {showDistTimePayerPopup && (
              <div className="absolute top-full left-0 mt-2 w-[400px] bg-white rounded-xl shadow-2xl border border-gray-300 p-4 z-50">
                <input
                  type="text"
                  placeholder="Search payers..."
                  value={distTimePayerSearch}
                  onChange={(e) => setDistTimePayerSearch(e.target.value)}
                  className="w-full px-3 py-2.5 border border-gray-300 rounded-lg mb-3 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
                  autoFocus
                />
                <div className="max-h-72 overflow-y-auto space-y-1">
                  <button
                    onClick={() => { setDistTimeParentOrg("Industry"); setShowDistTimePayerPopup(false); setDistTimePayerSearch(""); }}
                    className={`w-full text-left px-4 py-3 rounded-lg text-sm font-medium transition-colors ${
                      distTimeParentOrg === "Industry"
                        ? "bg-blue-600 text-white"
                        : "bg-gray-50 text-gray-900 hover:bg-blue-50"
                    }`}
                  >
                    Industry (All Payers)
                  </button>
                  {filterOptions?.parent_orgs?.filter(p => p.toLowerCase().includes(distTimePayerSearch.toLowerCase())).slice(0, 20).map((org) => (
                    <button
                      key={org}
                      onClick={() => { setDistTimeParentOrg(org); setShowDistTimePayerPopup(false); setDistTimePayerSearch(""); }}
                      className={`w-full text-left px-4 py-3 rounded-lg text-sm font-medium transition-colors ${
                        distTimeParentOrg === org
                          ? "bg-blue-600 text-white"
                          : "text-gray-800 hover:bg-blue-50"
                      }`}
                    >
                      {org}
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Filter Button */}
          <div className="relative">
            <button
              onClick={() => setShowDistTimeFilterPopup(!showDistTimeFilterPopup)}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-semibold transition-all ${
                distTimePlanTypes.length + distTimeGroupTypes.length + distTimeSnpTypes.length > 0
                  ? "bg-blue-600 text-white hover:bg-blue-700 shadow-md"
                  : "border-2 border-gray-300 bg-white text-gray-700 hover:border-blue-500 hover:bg-blue-50"
              }`}
            >
              <Filter className="w-4 h-4" />
              <span>Filter</span>
              {(distTimePlanTypes.length + distTimeGroupTypes.length + distTimeSnpTypes.length > 0) && (
                <span className="bg-white text-blue-600 text-xs px-2 py-0.5 rounded-full font-bold">
                  {distTimePlanTypes.length + distTimeGroupTypes.length + distTimeSnpTypes.length}
                </span>
              )}
            </button>

            {showDistTimeFilterPopup && (
              <div className="absolute top-full left-0 mt-2 w-80 bg-white rounded-xl shadow-2xl border border-gray-300 p-5 z-50">
                <div className="flex justify-between items-center mb-4">
                  <span className="font-bold text-gray-900">Filter by Type</span>
                  {(distTimePlanTypes.length + distTimeGroupTypes.length + distTimeSnpTypes.length > 0) && (
                    <button
                      onClick={() => { setDistTimePlanTypes([]); setDistTimeGroupTypes([]); setDistTimeSnpTypes([]); }}
                      className="text-sm text-blue-600 hover:text-blue-800 font-semibold"
                    >
                      Clear all
                    </button>
                  )}
                </div>

                <div className="mb-4">
                  <label className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">Market Segment</label>
                  <div className="flex gap-2">
                    {['Individual', 'Group'].map((type) => (
                      <button
                        key={type}
                        onClick={() => setDistTimeGroupTypes(prev => prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type])}
                        className={`flex-1 px-3 py-2.5 rounded-lg text-sm font-semibold transition-all ${
                          distTimeGroupTypes.includes(type) ? "bg-indigo-600 text-white shadow-md" : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                        }`}
                      >
                        {type}
                      </button>
                    ))}
                  </div>
                </div>

                <div className="mb-4">
                  <label className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">Plan Type</label>
                  <div className="flex flex-wrap gap-2">
                    {['HMO', 'PPO', 'PFFS'].map((type) => (
                      <button
                        key={type}
                        onClick={() => setDistTimePlanTypes(prev => prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type])}
                        className={`px-4 py-2.5 rounded-lg text-sm font-semibold transition-all ${
                          distTimePlanTypes.includes(type) ? "bg-blue-600 text-white shadow-md" : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                        }`}
                      >
                        {type}
                      </button>
                    ))}
                  </div>
                </div>

                <div className="mb-4">
                  <label className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">Special Needs</label>
                  <div className="flex gap-2">
                    {['SNP', 'Non-SNP'].map((type) => (
                      <button
                        key={type}
                        onClick={() => setDistTimeSnpTypes(prev => prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type])}
                        className={`flex-1 px-3 py-2.5 rounded-lg text-sm font-semibold transition-all ${
                          distTimeSnpTypes.includes(type) ? "bg-orange-600 text-white shadow-md" : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                        }`}
                      >
                        {type}
                      </button>
                    ))}
                  </div>
                </div>

                <button
                  onClick={() => setShowDistTimeFilterPopup(false)}
                  className="w-full py-3 bg-gray-900 hover:bg-gray-800 text-white rounded-lg text-sm font-semibold shadow-md"
                >
                  Apply Filters
                </button>
              </div>
            )}
          </div>
        </div>

        {/* Click outside handlers */}
        {(showDistTimePayerPopup || showDistTimeFilterPopup) && (
          <div
            className="fixed inset-0 z-40"
            onClick={() => { setShowDistTimePayerPopup(false); setShowDistTimeFilterPopup(false); setDistTimePayerSearch(""); }}
          />
        )}

        {/* Table */}
        {distTimeLoading ? (
          <div className="flex items-center justify-center h-48">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
          </div>
        ) : distTimeData?.error ? (
          <div className="p-8 text-center text-gray-500">{distTimeData.error}</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full border-collapse">
              <thead>
                <tr className="bg-gray-50">
                  <th className="py-2.5 px-4 font-medium text-gray-500 text-sm text-left border-b border-gray-200 sticky left-0 bg-gray-50 z-10 w-24">
                    Rating
                  </th>
                  {filteredDistTimeYears.map((year) => (
                    <th key={year} className="py-2.5 px-4 font-semibold text-gray-900 text-sm text-center border-l border-b border-gray-200 min-w-[70px]">
                      {year}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {[5.0, 4.5, 4.0, 3.5, 3.0, 2.5].map((band) => (
                  <tr key={band} className="hover:bg-gray-50">
                    <td className="py-2.5 px-4 font-medium text-gray-700 whitespace-nowrap border-b border-gray-100 sticky left-0 bg-white z-10">
                      {band % 1 === 0 ? band : band.toFixed(1)} Stars
                    </td>
                    {filteredDistTimeYears.map((year, i) => {
                      const idx = distTimeStartIdx + i;
                      const pct = distTimeData?.distribution?.[band.toFixed(1)]?.[idx];
                      return (
                        <td key={year} className="py-2.5 px-4 text-center whitespace-nowrap border-l border-b border-gray-100">
                          {pct !== undefined ? (
                            <span className="font-medium text-gray-900">{pct.toFixed(1)}%</span>
                          ) : (
                            <span className="text-gray-300">-</span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
                {/* 4+ Row */}
                <tr className="bg-green-50">
                  <td className="py-2.5 px-4 font-semibold text-green-800 whitespace-nowrap border-b border-gray-200 sticky left-0 bg-green-50 z-10">
                    4+ Stars
                  </td>
                  {filteredDistTimeYears.map((year, i) => {
                    const idx = distTimeStartIdx + i;
                    const pct = distTimeData?.distribution?.['4+']?.[idx];
                    return (
                      <td key={year} className="py-2.5 px-4 text-center whitespace-nowrap border-l border-b border-gray-200">
                        {pct !== undefined ? (
                          <span className="font-bold text-green-700">{pct.toFixed(1)}%</span>
                        ) : (
                          <span className="text-gray-300">-</span>
                        )}
                      </td>
                    );
                  })}
                </tr>
              </tbody>
            </table>
          </div>
        )}
      </div>
        </>
      )}

      {/* CUTPOINTS TAB */}
      {activeTab === "cutpoints" && <CutpointsTab parentOrgs={filterOptions?.parent_orgs || []} />}

      {/* MEASURES TAB */}
      {activeTab === "measures" && <MeasurePerformanceTab parentOrgs={filterOptions?.parent_orgs || []} />}

      {/* CONTRACT TAB */}
      {activeTab === "contract" && <ContractTab />}
    </div>
  );
}
