'use client';

import { useState, useCallback, useRef, useEffect } from 'react';
import { 
  Send, Sparkles, User, ChevronDown, ChevronRight, 
  DollarSign, Clock, Database, Brain, CheckCircle, 
  XCircle, Activity, Loader2, Zap, Table, BarChart3, Download,
  Search, FileText, Calculator, TrendingUp, Plus, X, BookOpen, ScrollText, Layers
} from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
  AreaChart,
  Area
} from 'recharts';
import { SharedChart, SharedTable } from '@/components/charts';

const API_BASE = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000').replace(/\/$/, '');

// Document context types
interface CMSDocument {
  year: number;
  name: string;
  type: string;
  key: string;
  size_mb: number;
}

interface YearDetail {
  year: number;
  month: number | null;
}

interface DataSource {
  id: string;
  name: string;
  description: string;
  years: number[];
  years_detail?: YearDetail[];
  has_month?: boolean;
  key_columns: string[];
  join_keys: string[];
}

interface SelectedDocument {
  type: string;
  year: number;
  name: string;
  isDataSource?: boolean;  // true for raw data, false for documents
}

// Format large numbers with abbreviations (1.4M, 2.5B, etc.) - v2
function formatLargeNumber(value: number | string): string {
  if (typeof value !== 'number') return String(value);
  
  const absValue = Math.abs(value);
  
  if (absValue >= 1_000_000_000) {
    return (value / 1_000_000_000).toFixed(1).replace(/\.0$/, '') + 'B';
  }
  if (absValue >= 1_000_000) {
    return (value / 1_000_000).toFixed(1).replace(/\.0$/, '') + 'M';
  }
  if (absValue >= 1_000) {
    return (value / 1_000).toFixed(1).replace(/\.0$/, '') + 'K';
  }
  return value.toLocaleString();
}

// Format numbers for display (full precision with commas)
function formatDisplayNumber(value: number | string): string {
  if (typeof value !== 'number') return String(value);
  return value.toLocaleString();
}

interface LLMCall {
  call_id: string;
  phase: string;
  model: string;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  cost_usd: number;
  latency_ms: number;
}

interface ToolCall {
  call_id: string;
  tool_name: string;
  arguments: Record<string, unknown>;
  result_preview: string;
  success: boolean;
  error?: string;
  latency_ms: number;
  rows_returned?: number;
}

interface AgentStep {
  step_id: string;
  step_number: number;
  phase: string;
  description: string;
  llm_calls: LLMCall[];
  tool_calls: ToolCall[];
  decision?: string;
  reasoning?: string;
  sql_generated?: string;
  sql_validation?: string;
  timestamp: string;
}

interface ThoughtStep {
  step: string;
  reasoning: string;
  conclusion: string;
  confidence: number;
}

interface SQLQuery {
  sql: string;
  description: string;
  rows_returned: number;
  success: boolean;
  error?: string;
}

// V3 Thinking interfaces
interface ThinkingStepV3 {
  id: string;
  phase: string;
  title: string;
  content: string;
  status: string;
  duration_ms: number;
  tool_name?: string;
  tool_params?: Record<string, unknown>;
  service_called?: string;
  row_count?: number;
  validations?: { check: string; passed: boolean; message: string }[];
  confidence?: string;
}

interface ThinkingProcessV3 {
  query_id: string;
  question: string;
  steps: ThinkingStepV3[];
  total_duration_ms: number;
  total_tokens: number;
  tools_called: number;
  status: string;
}

interface AgentResponseV3 {
  status: string;
  response: string;
  charts: ChartSpecV3[];
  tables: DataTableV3[];
  thinking: ThinkingProcessV3 | null;
  sources: string[];
  confidence: string;
  error?: string;
}

interface ChartSpecV3 {
  type: 'bar' | 'line' | 'area' | 'table';
  title: string;
  data: Record<string, unknown>[];
  xKey?: string;
  yKeys?: string[];
  colors?: string[];
}

interface DataTableV3 {
  type: string;
  title: string;
  data: Record<string, unknown>[];
  columns?: string[];
}

interface ChartSpec {
  chart_type: 'bar' | 'line' | 'area' | 'pie';
  title: string;
  subtitle?: string;
  x_axis: string;
  x_label?: string;
  y_axis: string;
  y_label?: string;
  y_domain?: [number, number];
  data: Record<string, unknown>[];
  series?: { key: string; label: string; color?: string }[];
  orientation?: 'vertical' | 'horizontal';
  color_field?: string;
  show_legend?: boolean;
}

interface DataTable {
  title: string;
  columns: string[];
  rows: Record<string, unknown>[];
  summary?: string;
}

interface AgentResponse {
  answer: string;
  run_id: string;
  llm_calls: number;
  tool_calls: number;
  total_tokens: number;
  cost_usd: number;
  latency_ms: number;
  confidence: number;
  data_tables: DataTable[];
  charts: ChartSpec[];
  sources: string[];
  thought_process?: ThoughtStep[];
  sql_queries?: SQLQuery[];
  audit?: {
    steps: AgentStep[];
  };
}

// Unified response interface that handles both V2 and V3
interface UnifiedResponse {
  version: 'v2' | 'v3';
  answer: string;
  charts: ChartSpec[];
  tables: DataTable[];
  sources: string[];
  confidence: number | string;
  thinking?: ThinkingProcessV3;
  total_tokens?: number;
  tools_called?: number;
  latency_ms?: number;
}

interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  isLoading?: boolean;
  loadingPhase?: string;
  loadingStep?: string;
  response?: AgentResponse;
  responseV3?: AgentResponseV3;
}

const PHASE_ICONS: Record<string, React.ReactNode> = {
  planning: <Brain className="w-4 h-4" />,
  executing: <Database className="w-4 h-4" />,
  analyzing: <Activity className="w-4 h-4" />,
  validating: <CheckCircle className="w-4 h-4" />,
  synthesizing: <Sparkles className="w-4 h-4" />,
};

const PHASE_LABELS: Record<string, string> = {
  planning: 'Planning what data to gather...',
  executing: 'Querying databases...',
  analyzing: 'Analyzing results...',
  validating: 'Validating findings...',
  synthesizing: 'Writing response...',
};

const PHASE_COLORS: Record<string, string> = {
  planning: 'text-purple-500 bg-purple-500/10',
  executing: 'text-blue-500 bg-blue-500/10',
  analyzing: 'text-green-500 bg-green-500/10',
  validating: 'text-yellow-500 bg-yellow-500/10',
  synthesizing: 'text-pink-500 bg-pink-500/10',
};

const CHART_COLORS = [
  '#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', 
  '#EC4899', '#06B6D4', '#84CC16', '#F97316', '#6366F1'
];

function formatCost(cost: number): string {
  if (cost < 0.01) return `$${(cost * 100).toFixed(2)}¢`;
  return `$${cost.toFixed(4)}`;
}

function formatLatency(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function formatCellValue(val: unknown, colName?: string): { text: string; className?: string } {
  if (val === null || val === undefined) return { text: '-', className: 'text-gray-400' };
  if (typeof val === 'object') {
    if (Array.isArray(val)) {
      return { text: val.length > 0 ? `[${val.length} items]` : '[]' };
    }
    const obj = val as Record<string, unknown>;
    if ('value' in obj) return formatCellValue(obj.value, colName);
    if ('name' in obj) return { text: String(obj.name) };
    const json = JSON.stringify(val);
    return { text: json.length > 50 ? json.slice(0, 47) + '...' : json };
  }
  if (typeof val === 'number') {
    const formatted = Number.isInteger(val) && Math.abs(val) > 1000 
      ? val.toLocaleString() 
      : !Number.isInteger(val) ? val.toFixed(1) : val.toString();
    
    // Color coding for change/growth columns
    const isChangeCol = colName?.toLowerCase().includes('change') || 
                        colName?.toLowerCase().includes('growth') ||
                        colName?.toLowerCase().includes('pct_change');
    if (isChangeCol) {
      if (val > 0) return { text: `+${formatted}`, className: 'text-green-600 font-semibold' };
      if (val < 0) return { text: formatted, className: 'text-red-600 font-semibold' };
    }
    
    // Large numbers get special formatting
    if (Math.abs(val) > 100000) return { text: formatted, className: 'font-medium' };
    
    return { text: formatted };
  }
  
  // String values - check for special patterns
  const str = String(val);
  if (str === 'Yes' || str === 'RECOVERED') return { text: str, className: 'text-green-600 font-medium' };
  if (str === 'No' || str === 'NOT_RECOVERED') return { text: str, className: 'text-red-600 font-medium' };
  if (str === 'Ongoing' || str === 'PARTIAL') return { text: str, className: 'text-yellow-600 font-medium' };
  if (str === 'NEW in V28') return { text: str, className: 'text-blue-600 font-medium' };
  if (str === 'DROPPED in V28') return { text: str, className: 'text-red-600 font-medium' };
  
  return { text: str };
}

function DataTableDisplay({ table }: { table: DataTable }) {
  const [collapsed, setCollapsed] = useState(false);
  
  // Validate and fix table structure
  const validColumns = table.columns?.filter(c => typeof c === 'string' && c !== 'rows') || [];
  const validRows = Array.isArray(table.rows) ? table.rows.slice(0, 50) : []; // Limit to 50 rows
  
  // If columns are wrong, try to infer from first row
  const columns = validColumns.length > 0 ? validColumns : 
    (validRows.length > 0 && typeof validRows[0] === 'object' ? Object.keys(validRows[0] as object) : []);
  
  const downloadCSV = () => {
    const headers = columns.join(',');
    const rows = validRows.map(row => 
      columns.map(col => {
        const val = (row as Record<string, unknown>)[col];
        const { text } = formatCellValue(val, col);
        return text.includes(',') ? `"${text}"` : text;
      }).join(',')
    );
    const csv = [headers, ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${(table.title || 'data').replace(/\s+/g, '_')}.csv`;
    a.click();
  };

  if (columns.length === 0 || validRows.length === 0) {
    return (
      <div className="my-4 p-4 border border-gray-200 dark:border-gray-700 rounded-lg bg-gray-50 dark:bg-gray-800/50">
        <div className="flex items-center gap-2 text-gray-500">
          <Table className="w-4 h-4" />
          <span>{table.title || 'Data'}</span>
          <span className="text-xs">(No displayable data)</span>
        </div>
      </div>
    );
  }

  return (
    <div className="my-4 border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
      <div className="flex items-center justify-between px-4 py-2 bg-gray-50 dark:bg-gray-800/50">
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="flex items-center gap-2 font-medium text-gray-700 dark:text-gray-300"
        >
          {collapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
          <Table className="w-4 h-4 text-blue-500" />
          {table.title || 'Data Table'}
          <span className="text-xs text-gray-400 ml-2">({validRows.length} rows)</span>
        </button>
        <button
          onClick={downloadCSV}
          className="flex items-center gap-1 text-xs text-gray-500 hover:text-blue-500"
        >
          <Download className="w-3 h-3" />
          CSV
        </button>
      </div>
      
      {!collapsed && (
        <>
          {table.summary && (
            <div className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 bg-blue-50 dark:bg-blue-900/20">
              {table.summary}
            </div>
          )}
          <div className="overflow-x-auto max-h-80">
            <table className="w-full text-sm">
              <thead className="bg-gray-100 dark:bg-gray-800 sticky top-0">
                <tr>
                  {columns.map((col, i) => (
                    <th key={i} className="px-3 py-2 text-left font-medium text-gray-600 dark:text-gray-400 whitespace-nowrap">
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                {validRows.map((row, i) => (
                  <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-800/30">
                    {columns.map((col, j) => {
                      const { text, className } = formatCellValue((row as Record<string, unknown>)[col], col);
                      return (
                        <td key={j} className={`px-3 py-2 whitespace-nowrap ${className || 'text-gray-700 dark:text-gray-300'}`}>
                          {text}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

function ChartDisplay({ chart }: { chart: ChartSpec }) {
  const [collapsed, setCollapsed] = useState(false);
  
  // Validate chart data
  const validData = Array.isArray(chart.data) ? chart.data.filter(d => typeof d === 'object') : [];
  
  if (validData.length === 0) {
    return (
      <div className="my-4 p-4 border border-gray-200 dark:border-gray-700 rounded-lg bg-gray-50 dark:bg-gray-800/50">
        <div className="flex items-center gap-2 text-gray-500">
          <BarChart3 className="w-4 h-4" />
          <span>{chart.title || 'Chart'}</span>
          <span className="text-xs">(No data)</span>
        </div>
      </div>
    );
  }
  
  const series = chart.series || [{ key: chart.y_axis, label: chart.y_label || chart.y_axis, color: CHART_COLORS[0] }];
  const isHorizontal = chart.orientation === 'horizontal';
  
  // Determine colors based on chart context
  const isLosersChart = chart.title?.toLowerCase().includes('loser') || 
                        chart.title?.toLowerCase().includes('drop') ||
                        chart.title?.toLowerCase().includes('decrease') ||
                        chart.title?.toLowerCase().includes('loss');
  const isGainersChart = chart.title?.toLowerCase().includes('gainer') ||
                         chart.title?.toLowerCase().includes('growth') ||
                         chart.title?.toLowerCase().includes('increase');
  const defaultBarColor = isLosersChart ? '#EF4444' : isGainersChart ? '#10B981' : '#3B82F6';
  const chartIcon = chart.chart_type === 'line' ? 'text-blue-500' : isLosersChart ? 'text-red-500' : 'text-green-500';

  // Calculate chart height based on data
  const chartHeight = isHorizontal ? Math.max(300, Math.min(validData.length * 35, 500)) : 350;

  return (
    <div className="my-6 border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden shadow-sm">
      {/* Chart header */}
      <div className="px-4 py-3 bg-gradient-to-r from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-800/80 border-b border-gray-200 dark:border-gray-700">
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="w-full flex items-center gap-3 font-semibold text-gray-800 dark:text-gray-200 text-left"
        >
          {collapsed ? <ChevronRight className="w-5 h-5 text-gray-400" /> : <ChevronDown className="w-5 h-5 text-gray-400" />}
          <BarChart3 className={`w-5 h-5 ${chartIcon}`} />
          <div className="flex-1">
            <span className="text-base">{chart.title}</span>
            {chart.subtitle && (
              <span className="block text-sm text-gray-500 font-normal">{chart.subtitle}</span>
            )}
          </div>
        </button>
      </div>
      
      {!collapsed && (
        <div className="p-6 bg-white dark:bg-gray-900">
          <ResponsiveContainer width="100%" height={chartHeight}>
            {chart.chart_type === 'bar' && isHorizontal ? (
              // Horizontal bar chart (for rankings)
              <BarChart data={validData} layout="vertical" margin={{ left: 10, right: 30, top: 10, bottom: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.2} horizontal={true} vertical={false} />
                <XAxis 
                  type="number" 
                  tick={{ fill: '#9CA3AF', fontSize: 11 }} 
                  tickFormatter={formatLargeNumber}
                  domain={chart.y_domain || ['auto', 'auto']}
                  label={chart.x_label ? { value: chart.x_label, position: 'bottom', fill: '#6B7280', fontSize: 12 } : undefined}
                />
                <YAxis 
                  type="category" 
                  dataKey={chart.y_axis} 
                  tick={{ fill: '#6B7280', fontSize: 11 }} 
                  width={180}
                  tickLine={false}
                  tickFormatter={(v) => typeof v === 'string' && v.length > 25 ? v.slice(0, 22) + '...' : v}
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1F2937', border: 'none', borderRadius: '8px', color: '#F3F4F6', padding: '12px' }}
                  formatter={(value) => [formatDisplayNumber(value as number), '']}
                  labelFormatter={(label) => label}
                />
                {series.map((s) => (
                  <Bar 
                    key={s.key} 
                    dataKey={s.key} 
                    name={s.label} 
                    fill={s.color || defaultBarColor}
                    radius={[0, 4, 4, 0]}
                  />
                ))}
              </BarChart>
            ) : chart.chart_type === 'bar' ? (
              // Vertical bar chart
              <BarChart data={validData} margin={{ left: 10, right: 30, top: 10, bottom: 30 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.2} />
                <XAxis 
                  dataKey={chart.x_axis} 
                  tick={{ fill: '#6B7280', fontSize: 11 }} 
                  tickFormatter={(v) => typeof v === 'string' && v.length > 15 ? v.slice(0, 12) + '...' : v}
                />
                <YAxis 
                  tick={{ fill: '#9CA3AF', fontSize: 11 }} 
                  domain={chart.y_domain || ['auto', 'auto']}
                  tickFormatter={formatLargeNumber}
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1F2937', border: 'none', borderRadius: '8px', color: '#F3F4F6', padding: '12px' }}
                  formatter={(value) => [formatDisplayNumber(value as number), '']}
                />
                {chart.show_legend !== false && <Legend />}
                {series.map((s, i) => (
                  <Bar 
                    key={s.key} 
                    dataKey={s.key} 
                    name={s.label} 
                    fill={s.color || CHART_COLORS[i % CHART_COLORS.length]}
                    radius={[4, 4, 0, 0]}
                  />
                ))}
              </BarChart>
            ) : chart.chart_type === 'area' ? (
              <AreaChart data={validData} margin={{ left: 10, right: 30, top: 10, bottom: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                <XAxis dataKey={chart.x_axis} tick={{ fill: '#9CA3AF', fontSize: 12 }} />
                <YAxis 
                  tick={{ fill: '#9CA3AF', fontSize: 12 }}
                  domain={chart.y_domain || ['auto', 'auto']}
                  tickFormatter={formatLargeNumber}
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1F2937', border: 'none', borderRadius: '8px', color: '#F3F4F6' }}
                  formatter={(value) => [formatDisplayNumber(value as number), '']}
                />
                {chart.show_legend !== false && <Legend />}
                {series.map((s, i) => (
                  <Area 
                    key={s.key} type="monotone" dataKey={s.key} name={s.label}
                    stroke={s.color || CHART_COLORS[i % CHART_COLORS.length]}
                    fill={s.color || CHART_COLORS[i % CHART_COLORS.length]}
                    fillOpacity={0.3}
                  />
                ))}
              </AreaChart>
            ) : (
              // Line chart
              <LineChart data={validData} margin={{ left: 10, right: 30, top: 10, bottom: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                <XAxis 
                  dataKey={chart.x_axis} 
                  tick={{ fill: '#9CA3AF', fontSize: 12 }}
                  label={chart.x_label ? { value: chart.x_label, position: 'bottom', fill: '#6B7280', fontSize: 12, dy: 15 } : undefined}
                />
                <YAxis 
                  tick={{ fill: '#9CA3AF', fontSize: 12 }}
                  domain={chart.y_domain || ['auto', 'auto']}
                  tickFormatter={formatLargeNumber}
                  label={chart.y_label ? { value: chart.y_label, angle: -90, position: 'insideLeft', fill: '#6B7280', fontSize: 12 } : undefined}
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1F2937', border: 'none', borderRadius: '8px', color: '#F3F4F6', padding: '12px' }}
                  formatter={(value) => [formatDisplayNumber(value as number), '']}
                />
                {chart.show_legend !== false && series.length > 1 && <Legend />}
                {series.map((s, i) => (
                  <Line 
                    key={s.key} type="monotone" dataKey={s.key} name={s.label}
                    stroke={s.color || CHART_COLORS[i % CHART_COLORS.length]} 
                    strokeWidth={2}
                    dot={{ fill: s.color || CHART_COLORS[i % CHART_COLORS.length], r: 4 }}
                    activeDot={{ r: 6 }}
                  />
                ))}
              </LineChart>
            )}
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}

function LoadingIndicator({ phase, step }: { phase?: string; step?: string }) {
  const phaseKey = phase || 'planning';
  const icon = PHASE_ICONS[phaseKey] || <Loader2 className="w-4 h-4 animate-spin" />;
  const label = step || PHASE_LABELS[phaseKey] || 'Processing...';
  const color = PHASE_COLORS[phaseKey] || 'text-blue-500 bg-blue-500/10';
  
  return (
    <div className="flex items-start gap-3">
      <div className={`p-2 rounded-lg ${color}`}>
        {icon}
      </div>
      <div className="flex-1">
        <div className="flex items-center gap-2">
          <Loader2 className="w-3 h-3 animate-spin text-gray-400" />
          <span className="text-sm text-gray-600 dark:text-gray-400">{label}</span>
        </div>
        <div className="mt-2 flex gap-1">
          {['planning', 'executing', 'analyzing', 'validating', 'synthesizing'].map((p) => (
            <div 
              key={p}
              className={`h-1 flex-1 rounded-full transition-all duration-500 ${
                p === phaseKey ? 'bg-blue-500' : 
                ['planning', 'executing', 'analyzing', 'validating', 'synthesizing'].indexOf(p) < 
                ['planning', 'executing', 'analyzing', 'validating', 'synthesizing'].indexOf(phaseKey)
                  ? 'bg-blue-500/50' : 'bg-gray-200 dark:bg-gray-700'
              }`}
            />
          ))}
        </div>
        <div className="mt-1 flex justify-between text-[10px] text-gray-400">
          <span>Plan</span>
          <span>Query</span>
          <span>Analyze</span>
          <span>Validate</span>
          <span>Write</span>
        </div>
      </div>
    </div>
  );
}

function StepDetails({ step }: { step: AgentStep }) {
  const [expanded, setExpanded] = useState(false);
  const phaseColor = PHASE_COLORS[step.phase] || 'text-gray-500 bg-gray-500/10';
  const phaseIcon = PHASE_ICONS[step.phase] || <Zap className="w-4 h-4" />;

  return (
    <div className="border-l-2 border-gray-200 dark:border-gray-700 pl-3 py-1">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full text-left hover:bg-gray-50 dark:hover:bg-gray-800/50 rounded p-1 -ml-1"
      >
        {expanded ? <ChevronDown className="w-3 h-3 text-gray-400" /> : <ChevronRight className="w-3 h-3 text-gray-400" />}
        <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium ${phaseColor}`}>
          {phaseIcon}
          {step.phase}
        </span>
        <span className="text-xs text-gray-500 truncate flex-1">{step.description}</span>
        {step.llm_calls.length > 0 && (
          <span className="text-xs text-gray-400">
            {step.llm_calls.reduce((sum, c) => sum + c.total_tokens, 0).toLocaleString()} tokens
          </span>
        )}
      </button>

      {expanded && (
        <div className="mt-2 ml-5 space-y-2 text-xs">
          {step.llm_calls.map((call) => (
            <div key={call.call_id} className="p-2 bg-gray-50 dark:bg-gray-800 rounded">
              <div className="flex items-center gap-3 text-gray-500">
                <Brain className="w-3 h-3" />
                <span>LLM Call</span>
                <span className="text-gray-400">|</span>
                <span>{call.prompt_tokens.toLocaleString()} in</span>
                <span>{call.completion_tokens.toLocaleString()} out</span>
                <span className="text-gray-400">|</span>
                <span className="text-green-600">{formatCost(call.cost_usd)}</span>
                <span className="text-gray-400">|</span>
                <span>{formatLatency(call.latency_ms)}</span>
              </div>
            </div>
          ))}

          {step.tool_calls.map((call) => (
            <div key={call.call_id} className="p-2 bg-gray-50 dark:bg-gray-800 rounded">
              <div className="flex items-center gap-3 text-gray-500">
                <Database className="w-3 h-3" />
                <span className="font-medium text-blue-600">{call.tool_name}</span>
                {call.success ? (
                  <CheckCircle className="w-3 h-3 text-green-500" />
                ) : (
                  <XCircle className="w-3 h-3 text-red-500" />
                )}
                {call.rows_returned !== undefined && (
                  <span className="text-gray-400">{call.rows_returned} rows</span>
                )}
                <span className="text-gray-400">{formatLatency(call.latency_ms)}</span>
              </div>
              {call.result_preview && (
                <pre className="mt-1 text-gray-600 dark:text-gray-400 truncate max-w-full text-[10px]">
                  {call.result_preview.slice(0, 100)}...
                </pre>
              )}
            </div>
          ))}

          {step.decision && (
            <div className="text-gray-500 italic">
              Decision: {step.decision}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function TokenUsageBar({ response }: { response: AgentResponse }) {
  return (
    <div className="flex items-center gap-4 px-3 py-2 bg-gradient-to-r from-blue-500/10 to-purple-500/10 rounded-lg text-xs">
      <div className="flex items-center gap-1.5">
        <Brain className="w-3.5 h-3.5 text-purple-500" />
        <span className="font-medium text-gray-700 dark:text-gray-300">{response.llm_calls} LLM</span>
      </div>
      <div className="flex items-center gap-1.5">
        <Database className="w-3.5 h-3.5 text-blue-500" />
        <span className="font-medium text-gray-700 dark:text-gray-300">{response.tool_calls} tools</span>
      </div>
      <div className="flex items-center gap-1.5">
        <Zap className="w-3.5 h-3.5 text-yellow-500" />
        <span className="font-medium text-gray-700 dark:text-gray-300">{response.total_tokens.toLocaleString()} tokens</span>
      </div>
      <div className="flex items-center gap-1.5">
        <DollarSign className="w-3.5 h-3.5 text-green-500" />
        <span className="font-medium text-green-600">{formatCost(response.cost_usd)}</span>
      </div>
      <div className="flex items-center gap-1.5">
        <Clock className="w-3.5 h-3.5 text-gray-500" />
        <span className="text-gray-500">{formatLatency(response.latency_ms)}</span>
      </div>
      <div className={`ml-auto px-2 py-0.5 rounded text-xs font-medium ${
        response.confidence >= 0.8 ? 'bg-green-500/20 text-green-700' : 
        response.confidence >= 0.6 ? 'bg-yellow-500/20 text-yellow-700' : 
        'bg-red-500/20 text-red-700'
      }`}>
        {(response.confidence * 100).toFixed(0)}% confidence
      </div>
    </div>
  );
}

function ThoughtProcessDisplay({ thoughts }: { thoughts: ThoughtStep[] }) {
  return (
    <div className="space-y-2">
      {thoughts.map((thought, i) => (
        <div key={i} className="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-3 text-sm">
          <div className="flex items-center gap-2 mb-1">
            <Brain className="w-4 h-4 text-blue-500" />
            <span className="font-medium text-blue-700 dark:text-blue-300 capitalize">
              {thought.step.replace(/_/g, ' ')}
            </span>
            <span className={`text-xs px-2 py-0.5 rounded-full ${
              thought.confidence >= 0.7 ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' :
              thought.confidence >= 0.4 ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400' :
              'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400'
            }`}>
              {Math.round(thought.confidence * 100)}% confidence
            </span>
          </div>
          <p className="text-gray-600 dark:text-gray-300 text-xs mb-1">{thought.reasoning}</p>
          <p className="text-blue-600 dark:text-blue-400 text-xs font-medium">→ {thought.conclusion}</p>
        </div>
      ))}
    </div>
  );
}

function SQLQueriesDisplay({ queries }: { queries: SQLQuery[] }) {
  const [expanded, setExpanded] = useState<number | null>(null);
  
  return (
    <div className="space-y-2">
      {queries.map((query, i) => (
        <div key={i} className={`rounded-lg p-3 text-sm ${
          query.success ? 'bg-green-50 dark:bg-green-900/20' : 'bg-red-50 dark:bg-red-900/20'
        }`}>
          <div className="flex items-center justify-between mb-1">
            <div className="flex items-center gap-2">
              <Database className="w-4 h-4 text-gray-500" />
              <span className="font-medium text-gray-700 dark:text-gray-300">
                {query.description}
              </span>
            </div>
            <div className="flex items-center gap-2">
              {query.success ? (
                <span className="text-xs text-green-600 dark:text-green-400">
                  ✓ {query.rows_returned} rows
                </span>
              ) : (
                <span className="text-xs text-red-600 dark:text-red-400">
                  ✗ Failed
                </span>
              )}
              <button
                onClick={() => setExpanded(expanded === i ? null : i)}
                className="text-xs text-blue-500 hover:text-blue-700"
              >
                {expanded === i ? 'Hide SQL' : 'Show SQL'}
              </button>
            </div>
          </div>
          {expanded === i && (
            <pre className="mt-2 p-2 bg-gray-900 text-gray-100 rounded text-xs overflow-x-auto">
              {query.sql}
            </pre>
          )}
          {query.error && (
            <p className="mt-1 text-xs text-red-600">{query.error}</p>
          )}
        </div>
      ))}
    </div>
  );
}

// V3 Thinking Display - Shows transparent AI reasoning
function ThinkingDisplayV3({ thinking }: { thinking: ThinkingProcessV3 }) {
  const [expanded, setExpanded] = useState(true);
  
  const PHASE_ICONS_V3: Record<string, React.ReactNode> = {
    plan: <Brain className="w-4 h-4" />,
    query: <Database className="w-4 h-4" />,
    analyze: <Activity className="w-4 h-4" />,
    validate: <CheckCircle className="w-4 h-4" />,
    synthesize: <Sparkles className="w-4 h-4" />,
    error: <XCircle className="w-4 h-4" />,
  };
  
  const PHASE_COLORS_V3: Record<string, string> = {
    plan: 'text-purple-600 bg-purple-100 dark:bg-purple-900/30',
    query: 'text-blue-600 bg-blue-100 dark:bg-blue-900/30',
    analyze: 'text-green-600 bg-green-100 dark:bg-green-900/30',
    validate: 'text-yellow-600 bg-yellow-100 dark:bg-yellow-900/30',
    synthesize: 'text-pink-600 bg-pink-100 dark:bg-pink-900/30',
    error: 'text-red-600 bg-red-100 dark:bg-red-900/30',
  };
  
  const STATUS_ICONS: Record<string, React.ReactNode> = {
    running: <Loader2 className="w-3 h-3 animate-spin" />,
    complete: <CheckCircle className="w-3 h-3 text-green-500" />,
    error: <XCircle className="w-3 h-3 text-red-500" />,
  };

  return (
    <div className="border border-gray-200 dark:border-gray-700 rounded-xl overflow-hidden bg-gradient-to-br from-gray-50 to-blue-50/30 dark:from-gray-900 dark:to-blue-900/10">
      {/* Header */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-4 py-3 flex items-center justify-between bg-white/50 dark:bg-gray-800/50 border-b border-gray-200 dark:border-gray-700"
      >
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5">
            <Brain className="w-5 h-5 text-purple-500" />
            <span className="font-semibold text-gray-800 dark:text-gray-200">AI Thinking Process</span>
          </div>
          <span className="text-xs px-2 py-0.5 rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400">
            {thinking.steps.length} steps
          </span>
          <span className="text-xs text-gray-500">
            {(thinking.total_duration_ms / 1000).toFixed(1)}s • {thinking.total_tokens.toLocaleString()} tokens • {thinking.tools_called} tools
          </span>
        </div>
        {expanded ? <ChevronDown className="w-4 h-4 text-gray-400" /> : <ChevronRight className="w-4 h-4 text-gray-400" />}
      </button>
      
      {/* Steps */}
      {expanded && (
        <div className="p-4 space-y-3">
          {thinking.steps.map((step, i) => (
            <div key={step.id} className="flex gap-3">
              {/* Phase badge */}
              <div className={`flex-shrink-0 w-20 flex items-center gap-1.5 px-2 py-1 rounded-lg text-xs font-medium ${PHASE_COLORS_V3[step.phase] || 'text-gray-600 bg-gray-100'}`}>
                {PHASE_ICONS_V3[step.phase]}
                <span className="capitalize">{step.phase}</span>
              </div>
              
              {/* Content */}
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  {STATUS_ICONS[step.status]}
                  <span className="font-medium text-gray-800 dark:text-gray-200 text-sm">
                    {step.title}
                  </span>
                  {step.duration_ms > 0 && (
                    <span className="text-xs text-gray-400">
                      {step.duration_ms}ms
                    </span>
                  )}
                </div>
                
                <p className="text-sm text-gray-600 dark:text-gray-400 mt-0.5">
                  {step.content}
                </p>
                
                {/* Tool params if present */}
                {step.tool_name && (
                  <div className="mt-2 flex items-center gap-2 flex-wrap">
                    <span className="text-xs px-2 py-0.5 rounded bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 font-mono">
                      {step.tool_name}
                    </span>
                    {step.tool_params && Object.entries(step.tool_params).slice(0, 3).map(([key, val]) => (
                      <span key={key} className="text-xs text-gray-500">
                        {key}={JSON.stringify(val).slice(0, 30)}
                      </span>
                    ))}
                    {step.row_count !== undefined && (
                      <span className="text-xs text-green-600">
                        → {step.row_count} rows
                      </span>
                    )}
                  </div>
                )}
                
                {/* Validations */}
                {step.validations && step.validations.length > 0 && (
                  <div className="mt-2 flex flex-wrap gap-2">
                    {step.validations.map((v, j) => (
                      <span 
                        key={j} 
                        className={`text-xs px-2 py-0.5 rounded ${
                          v.passed ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400'
                        }`}
                      >
                        {v.passed ? '✓' : '✗'} {v.check}
                      </span>
                    ))}
                  </div>
                )}
                
                {/* Confidence */}
                {step.confidence && (
                  <span className={`inline-block mt-2 text-xs px-2 py-0.5 rounded-full ${
                    step.confidence === 'high' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' :
                    step.confidence === 'medium' ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400' :
                    'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400'
                  }`}>
                    Confidence: {step.confidence}
                  </span>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// V3 Chart Display - Uses SharedChart for consistent styling
function ChartDisplayV3({ chart }: { chart: ChartSpecV3 }) {
  if (!chart.data || chart.data.length === 0) {
    return null;
  }
  
  const xKey = chart.xKey || 'year';
  const yKeys = chart.yKeys || Object.keys(chart.data[0] as Record<string, unknown>).filter(k => k !== xKey);
  
  // Determine if horizontal bar chart based on data (ranking = horizontal)
  const isRanking = chart.title?.toLowerCase().includes('ranking') ||
                    chart.title?.toLowerCase().includes('top') ||
                    chart.title?.toLowerCase().includes('by payer') ||
                    chart.title?.toLowerCase().includes('market share');
  
  return (
    <SharedChart
      title={chart.title}
      data={chart.data as Record<string, string | number | null | undefined>[]}
      type={chart.type === 'bar' ? 'bar' : 'line'}
      xKey={xKey}
      yKeys={yKeys}
      colors={chart.colors}
      orientation={chart.type === 'bar' && isRanking ? 'horizontal' : 'vertical'}
      collapsible={true}
      defaultCollapsed={false}
    />
  );
}

// V3 Table Display - Uses SharedTable for consistent styling
function TableDisplayV3({ table }: { table: DataTableV3 }) {
  if (!table.data || table.data.length === 0) {
    return null;
  }
  
  const columns = table.columns || Object.keys(table.data[0] as Record<string, unknown>);
  
  // Determine if first column should have color indicators
  const hasOrgColumn = columns[0]?.toLowerCase().includes('org') ||
                       columns[0]?.toLowerCase().includes('payer') ||
                       columns[0]?.toLowerCase().includes('company');
  
  return (
    <SharedTable
      title={table.title}
      data={table.data as Record<string, string | number | null | undefined>[]}
      columns={columns}
      showRowNumbers={false}
      collapsible={true}
      defaultCollapsed={false}
      colorColumn={hasOrgColumn ? columns[0] : undefined}
    />
  );
}

function AuditPanel({ response }: { response: AgentResponse }) {
  const [showSteps, setShowSteps] = useState(false);
  const [showThoughts, setShowThoughts] = useState(false);
  const [showSQL, setShowSQL] = useState(false);

  const hasThoughts = response.thought_process && response.thought_process.length > 0;
  const hasSQL = response.sql_queries && response.sql_queries.length > 0;

  return (
    <div className="mt-4 border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
      {/* Token usage bar - prominent */}
      <TokenUsageBar response={response} />

      {/* Sources */}
      {response.sources && response.sources.length > 0 && (
        <div className="px-3 py-2 border-t border-gray-200 dark:border-gray-700 text-xs text-gray-500">
          <span className="font-medium">Sources: </span>
          {response.sources.join(' • ')}
        </div>
      )}

      {/* Thought Process toggle - NEW */}
      {hasThoughts && (
        <>
          <button
            onClick={() => setShowThoughts(!showThoughts)}
            className="w-full px-3 py-2 flex items-center gap-2 text-xs text-blue-600 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 border-t border-gray-200 dark:border-gray-700"
          >
            {showThoughts ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
            <Brain className="w-3 h-3" />
            {showThoughts ? 'Hide' : 'Show'} reasoning chain ({response.thought_process!.length} thoughts)
          </button>

          {showThoughts && (
            <div className="p-3 border-t border-gray-200 dark:border-gray-700 max-h-96 overflow-y-auto">
              <ThoughtProcessDisplay thoughts={response.thought_process!} />
            </div>
          )}
        </>
      )}

      {/* SQL Queries toggle - NEW */}
      {hasSQL && (
        <>
          <button
            onClick={() => setShowSQL(!showSQL)}
            className="w-full px-3 py-2 flex items-center gap-2 text-xs text-green-600 dark:text-green-400 hover:bg-green-50 dark:hover:bg-green-900/20 border-t border-gray-200 dark:border-gray-700"
          >
            {showSQL ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
            <Database className="w-3 h-3" />
            {showSQL ? 'Hide' : 'Show'} SQL queries ({response.sql_queries!.length})
          </button>

          {showSQL && (
            <div className="p-3 border-t border-gray-200 dark:border-gray-700 max-h-96 overflow-y-auto">
              <SQLQueriesDisplay queries={response.sql_queries!} />
            </div>
          )}
        </>
      )}

      {/* Steps toggle */}
      {response.audit?.steps && response.audit.steps.length > 0 && (
        <>
          <button
            onClick={() => setShowSteps(!showSteps)}
            className="w-full px-3 py-2 flex items-center gap-2 text-xs text-gray-500 hover:bg-gray-50 dark:hover:bg-gray-800/30 border-t border-gray-200 dark:border-gray-700"
          >
            {showSteps ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
            {showSteps ? 'Hide' : 'Show'} agent steps ({response.audit.steps.length})
          </button>

          {showSteps && (
            <div className="p-3 border-t border-gray-200 dark:border-gray-700 space-y-1 max-h-96 overflow-y-auto">
              {response.audit.steps.map((step) => (
                <StepDetails key={step.step_id} step={step} />
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function AssistantMessage({ message }: { message: ChatMessage }) {
  // Handle V3 response
  const v3Response = message.responseV3;
  const v2Response = message.response;
  
  // V3 charts and tables
  const v3Charts = v3Response?.charts || [];
  const v3Tables = v3Response?.tables || [];
  
  // V2 charts and tables
  const v2Charts = v2Response?.charts || [];
  const v2Tables = v2Response?.data_tables || [];
  
  const hasV3Content = v3Response && (v3Response.response || v3Charts.length > 0 || v3Tables.length > 0);
  const hasV2Content = v2Response && (v2Response.answer || v2Charts.length > 0 || v2Tables.length > 0);
  
  return (
    <div className="flex items-start gap-4">
      <div className="w-10 h-10 rounded-full bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center flex-shrink-0 shadow-lg">
        <Sparkles className="w-5 h-5 text-white" />
      </div>
      <div className="flex-1 min-w-0">
        {message.isLoading ? (
          <LoadingIndicator phase={message.loadingPhase} step={message.loadingStep} />
        ) : hasV3Content ? (
          // V3 Response Layout
          <div className="space-y-6">
            {/* Thinking Display - Show the AI's reasoning */}
            {v3Response.thinking && (
              <ThinkingDisplayV3 thinking={v3Response.thinking} />
            )}
            
            {/* Main narrative */}
            <div className="prose prose-base dark:prose-invert max-w-none 
                          prose-p:my-3 prose-p:leading-relaxed
                          prose-strong:text-blue-600 dark:prose-strong:text-blue-400
                          prose-headings:mt-5 prose-headings:mb-3
                          prose-li:my-1
                          prose-ul:my-3">
              <ReactMarkdown>{v3Response.response}</ReactMarkdown>
            </div>

            {/* V3 Charts */}
            {v3Charts.length > 0 && (
              <div className={v3Charts.length >= 2 ? 'grid grid-cols-1 lg:grid-cols-2 gap-4' : ''}>
                {v3Charts.map((chart, i) => (
                  <ChartDisplayV3 key={i} chart={chart} />
                ))}
              </div>
            )}

            {/* V3 Tables */}
            {v3Tables.length > 0 && (
              <div className="space-y-4">
                {v3Tables.map((table, i) => (
                  <TableDisplayV3 key={i} table={table} />
                ))}
              </div>
            )}

            {/* Sources and confidence */}
            {(v3Response.sources?.length > 0 || v3Response.confidence) && (
              <div className="flex items-center gap-4 px-3 py-2 bg-gray-50 dark:bg-gray-800/50 rounded-lg text-xs">
                {v3Response.sources?.length > 0 && (
                  <div className="flex items-center gap-1.5">
                    <FileText className="w-3.5 h-3.5 text-gray-500" />
                    <span className="text-gray-600 dark:text-gray-400">
                      Sources: {v3Response.sources.join(' • ')}
                    </span>
                  </div>
                )}
                {v3Response.confidence && (
                  <span className={`ml-auto px-2 py-0.5 rounded text-xs font-medium ${
                    v3Response.confidence === 'high' ? 'bg-green-500/20 text-green-700' : 
                    v3Response.confidence === 'medium' ? 'bg-yellow-500/20 text-yellow-700' : 
                    'bg-red-500/20 text-red-700'
                  }`}>
                    {v3Response.confidence} confidence
                  </span>
                )}
              </div>
            )}
          </div>
        ) : hasV2Content ? (
          // V2 Response Layout (legacy)
          <div className="space-y-6">
            {/* Main narrative */}
            <div className="prose prose-base dark:prose-invert max-w-none 
                          prose-p:my-3 prose-p:leading-relaxed
                          prose-strong:text-blue-600 dark:prose-strong:text-blue-400
                          prose-headings:mt-5 prose-headings:mb-3
                          prose-li:my-1
                          prose-ul:my-3">
              <ReactMarkdown>{message.content}</ReactMarkdown>
            </div>

            {/* Visualizations section */}
            {(v2Charts.length > 0 || v2Tables.length > 0) && (
              <div className="border-t border-gray-200 dark:border-gray-700 pt-6">
                {v2Charts.length > 0 && (
                  <div className={v2Charts.length >= 2 ? 'grid grid-cols-1 lg:grid-cols-2 gap-4' : ''}>
                    {v2Charts.map((chart, i) => (
                      <ChartDisplay key={i} chart={chart} />
                    ))}
                  </div>
                )}

                {v2Tables.length > 0 && (
                  <div className="mt-6 space-y-4">
                    {v2Tables.map((table, i) => (
                      <DataTableDisplay key={i} table={table} />
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* Audit panel */}
            {v2Response && (
              <AuditPanel response={v2Response} />
            )}
          </div>
        ) : (
          // Fallback - just show content
          <div className="prose prose-base dark:prose-invert max-w-none">
            <ReactMarkdown>{message.content}</ReactMarkdown>
          </div>
        )}
      </div>
    </div>
  );
}

function UserMessage({ message }: { message: ChatMessage }) {
  return (
    <div className="flex items-start gap-3">
      <div className="w-8 h-8 rounded-full bg-gray-200 dark:bg-gray-700 flex items-center justify-center flex-shrink-0">
        <User className="w-4 h-4 text-gray-600 dark:text-gray-300" />
      </div>
      <div className="flex-1">
        <p className="text-gray-900 dark:text-white">{message.content}</p>
      </div>
    </div>
  );
}

export function ChatV2() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [showDocSelector, setShowDocSelector] = useState(false);
  const [selectedDocs, setSelectedDocs] = useState<SelectedDocument[]>([]);
  const [availableDocs, setAvailableDocs] = useState<{
    rate_notices: { advance: CMSDocument[]; final: CMSDocument[] };
    technical_notes: { stars: CMSDocument[] };
    stars_docs?: { rate_announcements: CMSDocument[]; cai_supplements: CMSDocument[]; fact_sheets: CMSDocument[] };
    hcc_docs?: { model: CMSDocument[] };
  } | null>(null);
  const [availableDataSources, setAvailableDataSources] = useState<DataSource[]>([]);
  const [contextTab, setContextTab] = useState<'documents' | 'data'>('documents');
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Fetch available documents and data sources on mount
  useEffect(() => {
    fetch(`${API_BASE}/api/documents/list`)
      .then(res => res.json())
      .then(data => setAvailableDocs(data.documents))
      .catch(err => console.error('Failed to fetch documents:', err));
    
    fetch(`${API_BASE}/api/data-schema/list`)
      .then(res => res.json())
      .then(data => setAvailableDataSources(data.data_sources))
      .catch(err => console.error('Failed to fetch data sources:', err));
  }, []);

  const addDocument = (type: string, year: number, name: string, isDataSource: boolean = false) => {
    const exists = selectedDocs.some(d => d.type === type && d.year === year);
    if (!exists) {
      setSelectedDocs(prev => [...prev, { type, year, name, isDataSource }]);
    }
  };

  const removeDocument = (type: string, year: number) => {
    setSelectedDocs(prev => prev.filter(d => !(d.type === type && d.year === year)));
  };

  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages, scrollToBottom]);

  const updateLoadingPhase = useCallback((messageId: string, phase: string, step: string) => {
    setMessages(prev => prev.map(msg => 
      msg.id === messageId ? { ...msg, loadingPhase: phase, loadingStep: step } : msg
    ));
  }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    const userMessage: ChatMessage = {
      id: crypto.randomUUID(),
      role: 'user',
      content: input.trim(),
      timestamp: new Date().toISOString(),
    };

    const loadingId = crypto.randomUUID();
    const loadingMessage: ChatMessage = {
      id: loadingId,
      role: 'assistant',
      content: '',
      timestamp: new Date().toISOString(),
      isLoading: true,
      loadingPhase: 'planning',
      loadingStep: 'Planning what data to gather...',
    };

    setMessages((prev) => [...prev, userMessage, loadingMessage]);
    setInput('');
    setIsLoading(true);

    // Simulate phase progression while waiting
    const phases = [
      { phase: 'planning', step: 'Determining data requirements...', delay: 2000 },
      { phase: 'executing', step: 'Querying star ratings database...', delay: 5000 },
      { phase: 'executing', step: 'Fetching enrollment data...', delay: 8000 },
      { phase: 'analyzing', step: 'Analyzing patterns and trends...', delay: 12000 },
      { phase: 'validating', step: 'Validating findings...', delay: 18000 },
      { phase: 'synthesizing', step: 'Writing response...', delay: 25000 },
    ];
    
    const phaseTimeouts: NodeJS.Timeout[] = [];
    phases.forEach(({ phase, step, delay }) => {
      const timeout = setTimeout(() => {
        updateLoadingPhase(loadingId, phase, step);
      }, delay);
      phaseTimeouts.push(timeout);
    });

    try {
      // Use V3 agent API with optional document context
      const response = await fetch(`${API_BASE}/api/v3/agent/ask`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question: userMessage.content,
          include_thinking: true,
          document_context: selectedDocs.length > 0 ? selectedDocs : undefined,
        }),
      });

      // Clear phase simulation
      phaseTimeouts.forEach(t => clearTimeout(t));

      if (!response.ok) {
        throw new Error(`Request failed: ${response.status}`);
      }

      const data: AgentResponseV3 = await response.json();

      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === loadingId
            ? {
                ...msg,
                content: data.response,
                isLoading: false,
                loadingPhase: undefined,
                loadingStep: undefined,
                responseV3: data,
              }
            : msg
        )
      );
    } catch (error) {
      // Clear phase simulation
      phaseTimeouts.forEach(t => clearTimeout(t));
      
      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === loadingId
            ? {
                ...msg,
                content: `Sorry, something went wrong: ${error instanceof Error ? error.message : 'Unknown error'}`,
                isLoading: false,
                loadingPhase: undefined,
                loadingStep: undefined,
              }
            : msg
        )
      );
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Messages area */}
      <div className="flex-1 overflow-y-auto p-4 space-y-6">
        {messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-center text-gray-500">
            <Sparkles className="w-12 h-12 mb-4 text-blue-500 opacity-50" />
            <h3 className="text-lg font-medium text-gray-700 dark:text-gray-300">
              MA Intelligence Agent
            </h3>
            <p className="text-sm mt-1 max-w-md">
              Multi-step reasoning with charts, tables, and full audit trail. 
              Ask about enrollment, star ratings, risk adjustment, rate notices.
            </p>
            <div className="mt-6 grid grid-cols-2 gap-3 text-xs max-w-lg">
              <button
                onClick={() => setInput('Show companies with major 4+ star drops and their recovery patterns')}
                className="px-4 py-3 bg-gray-100 dark:bg-gray-800 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-700 text-left"
              >
                <span className="font-medium block mb-1">Star Rating Drops</span>
                <span className="text-gray-500">Major 4+ star losses & recoveries</span>
              </button>
              <button
                onClick={() => setInput('Compare V24 vs V28 risk models')}
                className="px-4 py-3 bg-gray-100 dark:bg-gray-800 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-700 text-left"
              >
                <span className="font-medium block mb-1">V24 vs V28 Models</span>
                <span className="text-gray-500">Risk model comparison</span>
              </button>
              <button
                onClick={() => setInput('Show D-SNP enrollment growth by payer from 2018-2026')}
                className="px-4 py-3 bg-gray-100 dark:bg-gray-800 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-700 text-left"
              >
                <span className="font-medium block mb-1">D-SNP Growth</span>
                <span className="text-gray-500">Dual eligible trends</span>
              </button>
              <button
                onClick={() => setInput('Top payers by enrollment with market share')}
                className="px-4 py-3 bg-gray-100 dark:bg-gray-800 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-700 text-left"
              >
                <span className="font-medium block mb-1">Market Leaders</span>
                <span className="text-gray-500">Enrollment & market share</span>
              </button>
            </div>
          </div>
        ) : (
          messages.map((message) => (
            message.role === 'user' ? (
              <UserMessage key={message.id} message={message} />
            ) : (
              <AssistantMessage key={message.id} message={message} />
            )
          ))
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input area */}
      <div className="border-t border-gray-200 dark:border-gray-700 p-4">
        {/* Selected documents/data chips */}
        {selectedDocs.length > 0 && (
          <div className="flex flex-wrap gap-2 mb-3">
            {selectedDocs.map((doc) => (
              <span
                key={`${doc.type}-${doc.year}`}
                className={`inline-flex items-center gap-1 px-2 py-1 text-xs rounded-full ${
                  doc.isDataSource 
                    ? 'bg-purple-100 dark:bg-purple-900 text-purple-800 dark:text-purple-200'
                    : 'bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200'
                }`}
              >
                {doc.isDataSource ? <Database className="w-3 h-3" /> : <FileText className="w-3 h-3" />}
                {doc.name}
                <button
                  type="button"
                  onClick={() => removeDocument(doc.type, doc.year)}
                  className={`ml-1 ${doc.isDataSource ? 'hover:text-purple-600' : 'hover:text-blue-600'}`}
                >
                  <X className="w-3 h-3" />
                </button>
              </span>
            ))}
          </div>
        )}
        
        <form onSubmit={handleSubmit} className="flex gap-3">
          <button
            type="button"
            onClick={() => setShowDocSelector(true)}
            className="px-3 py-2 border border-gray-200 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-800 text-gray-500 hover:text-gray-700 hover:border-gray-300 focus:outline-none focus:ring-2 focus:ring-blue-500"
            title="Add document context"
          >
            <Plus className="w-5 h-5" />
          </button>
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder={selectedDocs.length > 0 
              ? selectedDocs.some(d => d.isDataSource)
                ? "Ask how to calculate metrics, join tables, analyze the data..."
                : "Ask about the selected documents..."
              : "Ask about Medicare Advantage data..."}
            disabled={isLoading}
            className="flex-1 px-4 py-2 border border-gray-200 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50"
          />
          <button
            type="submit"
            disabled={isLoading || !input.trim()}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
          >
            {isLoading ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Send className="w-4 h-4" />
            )}
          </button>
        </form>
      </div>

      {/* Document Selector Modal */}
      {showDocSelector && (
        <div 
          className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4"
          onClick={() => setShowDocSelector(false)}
        >
          <div 
            className="bg-white dark:bg-gray-800 rounded-2xl shadow-2xl max-w-2xl w-full max-h-[80vh] overflow-hidden"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
              <div className="flex items-center justify-between mb-4">
                <div>
                  <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Add Context</h3>
                  <p className="text-sm text-gray-500 dark:text-gray-400">Select documents or data sources</p>
                </div>
                <button 
                  onClick={() => setShowDocSelector(false)}
                  className="p-2 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg"
                >
                  <X className="w-5 h-5 text-gray-400" />
                </button>
              </div>
              
              {/* Tabs */}
              <div className="flex gap-2">
                <button
                  onClick={() => setContextTab('documents')}
                  className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors ${
                    contextTab === 'documents'
                      ? 'bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300'
                      : 'text-gray-600 hover:bg-gray-100 dark:text-gray-400 dark:hover:bg-gray-700'
                  }`}
                >
                  <FileText className="w-4 h-4 inline mr-2" />
                  Policy Documents
                </button>
                <button
                  onClick={() => setContextTab('data')}
                  className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors ${
                    contextTab === 'data'
                      ? 'bg-purple-100 text-purple-700 dark:bg-purple-900 dark:text-purple-300'
                      : 'text-gray-600 hover:bg-gray-100 dark:text-gray-400 dark:hover:bg-gray-700'
                  }`}
                >
                  <Layers className="w-4 h-4 inline mr-2" />
                  Raw Data (Tutorials)
                </button>
              </div>
            </div>

            <div className="p-6 overflow-y-auto max-h-[60vh] space-y-6">
              {/* Data Sources Tab */}
              {contextTab === 'data' && (
                <div className="space-y-4">
                  <div className="bg-purple-50 dark:bg-purple-900/20 rounded-lg p-4 mb-4">
                    <p className="text-sm text-purple-800 dark:text-purple-200">
                      <strong>Tutorial Mode:</strong> Select data sources to learn how to join tables, calculate metrics, and work with raw CMS data.
                    </p>
                  </div>
                  
                  {availableDataSources.map((source) => (
                    <div key={source.id} className="border border-gray-200 dark:border-gray-700 rounded-lg p-4">
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-2">
                          <Database className="w-5 h-5 text-purple-500" />
                          <h4 className="font-medium text-gray-900 dark:text-white">{source.name}</h4>
                        </div>
                      </div>
                      <p className="text-sm text-gray-500 dark:text-gray-400 mb-2">{source.description}</p>
                      <p className="text-xs text-gray-400 dark:text-gray-500 mb-3">
                        Key columns: {source.key_columns.slice(0, 4).join(', ')}
                      </p>
                      <div className="flex flex-wrap gap-2">
                        {(() => {
                          const monthNames = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
                          const yearsToShow = [...source.years].reverse().slice(0, 12);
                          
                          return yearsToShow.map((year) => {
                            const isSelected = selectedDocs.some(d => d.type === source.id && d.year === year);
                            const yearDetail = source.years_detail?.find(d => d.year === year);
                            const monthLabel = source.has_month && yearDetail?.month 
                              ? ` (${monthNames[yearDetail.month]})`
                              : '';
                            
                            return (
                              <button
                                key={year}
                                onClick={() => isSelected 
                                  ? removeDocument(source.id, year)
                                  : addDocument(source.id, year, `${source.name} ${year}${monthLabel}`, true)
                                }
                                className={`px-2 py-1 text-xs rounded border transition-all ${
                                  isSelected
                                    ? 'bg-purple-100 border-purple-300 text-purple-700 dark:bg-purple-900 dark:border-purple-700 dark:text-purple-300'
                                    : 'bg-gray-50 border-gray-200 text-gray-600 hover:border-gray-300 dark:bg-gray-700 dark:border-gray-600 dark:text-gray-300'
                                }`}
                                title={source.has_month && yearDetail?.month 
                                  ? `${monthNames[yearDetail.month]} ${year} data` 
                                  : `${year} data`}
                              >
                                {year}{source.has_month && yearDetail?.month ? ` ${monthNames[yearDetail.month]}` : ''}
                              </button>
                            );
                          });
                        })()}
                      </div>
                    </div>
                  ))}
                  
                  {/* Example queries */}
                  <div className="pt-4 border-t border-gray-200 dark:border-gray-700">
                    <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Example Questions</h4>
                    <div className="space-y-2 text-sm text-gray-500 dark:text-gray-400">
                      <p>• "How do I join enrollment data with star ratings?"</p>
                      <p>• "Show me how to calculate % of enrollment in 5-star plans"</p>
                      <p>• "What columns do I need to link CPSC to Stars data?"</p>
                    </div>
                  </div>
                </div>
              )}
              
              {/* Documents Tab */}
              {contextTab === 'documents' && (
                <>
              {/* Rate Notices - Advance */}
              {availableDocs?.rate_notices.advance && availableDocs.rate_notices.advance.length > 0 && (
                <div>
                  <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2 flex items-center gap-2">
                    <ScrollText className="w-4 h-4 text-emerald-500" />
                    Advance Rate Notices
                  </h4>
                  <div className="flex flex-wrap gap-2">
                    {availableDocs.rate_notices.advance.slice(0, 8).map((doc) => {
                      const isSelected = selectedDocs.some(d => d.type === doc.type && d.year === doc.year);
                      return (
                        <button
                          key={doc.year}
                          onClick={() => isSelected 
                            ? removeDocument(doc.type, doc.year)
                            : addDocument(doc.type, doc.year, `${doc.year} Advance Rate Notice`)
                          }
                          className={`px-3 py-1.5 text-sm rounded-lg border transition-all ${
                            isSelected
                              ? 'bg-emerald-100 border-emerald-300 text-emerald-700 dark:bg-emerald-900 dark:border-emerald-700 dark:text-emerald-300'
                              : 'bg-gray-50 border-gray-200 text-gray-700 hover:border-gray-300 dark:bg-gray-700 dark:border-gray-600 dark:text-gray-300'
                          }`}
                        >
                          {doc.year}
                        </button>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* Rate Notices - Final */}
              {availableDocs?.rate_notices.final && availableDocs.rate_notices.final.length > 0 && (
                <div>
                  <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2 flex items-center gap-2">
                    <FileText className="w-4 h-4 text-blue-500" />
                    Final Rate Notices
                  </h4>
                  <div className="flex flex-wrap gap-2">
                    {availableDocs.rate_notices.final.slice(0, 8).map((doc) => {
                      const isSelected = selectedDocs.some(d => d.type === doc.type && d.year === doc.year);
                      return (
                        <button
                          key={doc.year}
                          onClick={() => isSelected 
                            ? removeDocument(doc.type, doc.year)
                            : addDocument(doc.type, doc.year, `${doc.year} Final Rate Notice`)
                          }
                          className={`px-3 py-1.5 text-sm rounded-lg border transition-all ${
                            isSelected
                              ? 'bg-blue-100 border-blue-300 text-blue-700 dark:bg-blue-900 dark:border-blue-700 dark:text-blue-300'
                              : 'bg-gray-50 border-gray-200 text-gray-700 hover:border-gray-300 dark:bg-gray-700 dark:border-gray-600 dark:text-gray-300'
                          }`}
                        >
                          {doc.year}
                        </button>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* Technical Notes - Stars */}
              {availableDocs?.technical_notes.stars && availableDocs.technical_notes.stars.length > 0 && (
                <div>
                  <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2 flex items-center gap-2">
                    <BookOpen className="w-4 h-4 text-amber-500" />
                    Star Ratings Technical Notes
                  </h4>
                  <div className="flex flex-wrap gap-2">
                    {availableDocs.technical_notes.stars.slice(0, 8).map((doc) => {
                      const isSelected = selectedDocs.some(d => d.type === doc.type && d.year === doc.year);
                      return (
                        <button
                          key={doc.year}
                          onClick={() => isSelected 
                            ? removeDocument(doc.type, doc.year)
                            : addDocument(doc.type, doc.year, `${doc.year} Stars Technical Notes`)
                          }
                          className={`px-3 py-1.5 text-sm rounded-lg border transition-all ${
                            isSelected
                              ? 'bg-amber-100 border-amber-300 text-amber-700 dark:bg-amber-900 dark:border-amber-700 dark:text-amber-300'
                              : 'bg-gray-50 border-gray-200 text-gray-700 hover:border-gray-300 dark:bg-gray-700 dark:border-gray-600 dark:text-gray-300'
                          }`}
                        >
                          {doc.year}
                        </button>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* Quick actions */}
              <div className="pt-4 border-t border-gray-200 dark:border-gray-700">
                <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Quick Select</h4>
                <div className="flex flex-wrap gap-2">
                  <button
                    onClick={() => {
                      if (availableDocs?.technical_notes.stars[0]) {
                        const latest = availableDocs.technical_notes.stars[0];
                        addDocument(latest.type, latest.year, `${latest.year} Stars Technical Notes`);
                      }
                    }}
                    className="px-3 py-1.5 text-sm bg-gray-100 dark:bg-gray-700 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300"
                  >
                    Latest Technical Notes
                  </button>
                  <button
                    onClick={() => {
                      if (availableDocs?.rate_notices.advance[0]) {
                        const latest = availableDocs.rate_notices.advance[0];
                        addDocument(latest.type, latest.year, `${latest.year} Advance Rate Notice`);
                      }
                    }}
                    className="px-3 py-1.5 text-sm bg-gray-100 dark:bg-gray-700 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300"
                  >
                    Latest Advance Notice
                  </button>
                  <button
                    onClick={() => {
                      // Add last 2 years of tech notes for comparison
                      availableDocs?.technical_notes.stars.slice(0, 2).forEach(doc => {
                        addDocument(doc.type, doc.year, `${doc.year} Stars Technical Notes`);
                      });
                    }}
                    className="px-3 py-1.5 text-sm bg-gray-100 dark:bg-gray-700 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300"
                  >
                    Compare Last 2 Years Tech Notes
                  </button>
                </div>
              </div>
              </>
              )}
            </div>

            <div className="px-6 py-4 border-t border-gray-200 dark:border-gray-700 flex items-center justify-between">
              <p className="text-xs text-gray-500 dark:text-gray-400">
                {selectedDocs.length} document{selectedDocs.length !== 1 ? 's' : ''} selected
              </p>
              <button
                onClick={() => setShowDocSelector(false)}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
              >
                Done
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default ChatV2;
