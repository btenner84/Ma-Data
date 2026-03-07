'use client';

import { useState, useCallback, useRef, useEffect } from 'react';
import { 
  Send, Sparkles, User, ChevronDown, ChevronRight, 
  DollarSign, Clock, Database, Brain, CheckCircle, 
  XCircle, Activity, Loader2, Zap, Table, BarChart3, Download,
  Search, FileText, Calculator
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

const API_BASE = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000').replace(/\/$/, '');

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

interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  isLoading?: boolean;
  loadingPhase?: string;
  loadingStep?: string;
  response?: AgentResponse;
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
                  tickFormatter={(v) => typeof v === 'number' ? v.toLocaleString() : v}
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
                  formatter={(value) => [(typeof value === 'number' ? value.toLocaleString() : value), '']}
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
                  tickFormatter={(v) => typeof v === 'number' ? v.toLocaleString() : v}
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1F2937', border: 'none', borderRadius: '8px', color: '#F3F4F6', padding: '12px' }}
                  formatter={(value) => [(typeof value === 'number' ? value.toLocaleString() : value), '']}
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
                />
                <Tooltip contentStyle={{ backgroundColor: '#1F2937', border: 'none', borderRadius: '8px', color: '#F3F4F6' }} />
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
                  label={chart.y_label ? { value: chart.y_label, angle: -90, position: 'insideLeft', fill: '#6B7280', fontSize: 12 } : undefined}
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1F2937', border: 'none', borderRadius: '8px', color: '#F3F4F6', padding: '12px' }}
                  formatter={(value) => [(typeof value === 'number' ? value.toFixed(1) : value), '']}
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
  const chartCount = message.response?.charts?.length || 0;
  const tableCount = message.response?.data_tables?.length || 0;
  
  return (
    <div className="flex items-start gap-4">
      <div className="w-10 h-10 rounded-full bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center flex-shrink-0 shadow-lg">
        <Sparkles className="w-5 h-5 text-white" />
      </div>
      <div className="flex-1 min-w-0">
        {message.isLoading ? (
          <LoadingIndicator phase={message.loadingPhase} step={message.loadingStep} />
        ) : (
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
            {(chartCount > 0 || tableCount > 0) && (
              <div className="border-t border-gray-200 dark:border-gray-700 pt-6">
                {/* Charts - use grid for multiple */}
                {chartCount > 0 && (
                  <div className={chartCount >= 2 ? 'grid grid-cols-1 lg:grid-cols-2 gap-4' : ''}>
                    {message.response!.charts!.map((chart, i) => (
                      <ChartDisplay key={i} chart={chart} />
                    ))}
                  </div>
                )}

                {/* Tables */}
                {tableCount > 0 && (
                  <div className="mt-6 space-y-4">
                    {message.response!.data_tables!.map((table, i) => (
                      <DataTableDisplay key={i} table={table} />
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* Audit panel - more subtle */}
            {message.response && (
              <AuditPanel response={message.response} />
            )}
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
  const messagesEndRef = useRef<HTMLDivElement>(null);

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
      const response = await fetch(`${API_BASE}/api/v2/agent/ask`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question: userMessage.content,
          include_full_audit: true,
        }),
      });

      // Clear phase simulation
      phaseTimeouts.forEach(t => clearTimeout(t));

      if (!response.ok) {
        throw new Error(`Request failed: ${response.status}`);
      }

      const data: AgentResponse = await response.json();

      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === loadingId
            ? {
                ...msg,
                content: data.answer,
                isLoading: false,
                loadingPhase: undefined,
                loadingStep: undefined,
                response: data,
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
        <form onSubmit={handleSubmit} className="flex gap-3">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about Medicare Advantage data..."
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
    </div>
  );
}

export default ChatV2;
