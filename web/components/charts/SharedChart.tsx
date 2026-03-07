'use client';

import { useState } from 'react';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import { ChevronDown, ChevronRight, Download, BarChart3, TrendingUp } from 'lucide-react';

const COLORS = [
  "#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c",
  "#0891b2", "#4f46e5", "#c026d3", "#059669", "#d97706",
];

function formatNumber(num: number | undefined | null): string {
  if (num === undefined || num === null || isNaN(num)) return "0";
  if (num >= 1000000000) return `${(num / 1000000000).toFixed(1)}B`;
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(0)}K`;
  return num.toLocaleString();
}

function formatFullNumber(num: number | undefined | null): string {
  if (num === undefined || num === null || isNaN(num)) return "0";
  return Math.round(num).toLocaleString();
}

export interface ChartData {
  [key: string]: string | number | null | undefined;
}

export interface SharedChartProps {
  title: string;
  subtitle?: string;
  data: ChartData[];
  type?: 'line' | 'bar';
  xKey: string;
  yKeys: string[];
  colors?: string[];
  height?: number;
  showLegend?: boolean;
  showYoY?: boolean;
  collapsible?: boolean;
  defaultCollapsed?: boolean;
  orientation?: 'vertical' | 'horizontal';
}

export function SharedChart({
  title,
  subtitle,
  data,
  type = 'line',
  xKey,
  yKeys,
  colors = COLORS,
  height = 384,
  showLegend = true,
  showYoY = false,
  collapsible = true,
  defaultCollapsed = false,
  orientation = 'vertical',
}: SharedChartProps) {
  const [collapsed, setCollapsed] = useState(defaultCollapsed);
  
  if (!data || data.length === 0) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-center gap-2 text-gray-500">
          <BarChart3 className="w-5 h-5" />
          <span>{title}</span>
          <span className="text-sm">(No data)</span>
        </div>
      </div>
    );
  }

  // Calculate max value for Y domain
  const maxValue = Math.max(
    ...data.flatMap(d => yKeys.map(k => (typeof d[k] === 'number' ? d[k] : 0) as number))
  );

  const isHorizontal = orientation === 'horizontal' && type === 'bar';
  const chartHeight = isHorizontal ? Math.max(300, Math.min(data.length * 35, 500)) : height;

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-gray-50 to-white">
        {collapsible ? (
          <button
            onClick={() => setCollapsed(!collapsed)}
            className="w-full flex items-center gap-3 text-left"
          >
            {collapsed ? (
              <ChevronRight className="w-5 h-5 text-gray-400" />
            ) : (
              <ChevronDown className="w-5 h-5 text-gray-400" />
            )}
            <div className="flex-1">
              <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
                {type === 'line' ? (
                  <TrendingUp className="w-5 h-5 text-blue-500" />
                ) : (
                  <BarChart3 className="w-5 h-5 text-blue-500" />
                )}
                {title}
              </h3>
              {subtitle && (
                <p className="text-sm text-gray-500 mt-0.5">{subtitle}</p>
              )}
            </div>
          </button>
        ) : (
          <div>
            <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
              {type === 'line' ? (
                <TrendingUp className="w-5 h-5 text-blue-500" />
              ) : (
                <BarChart3 className="w-5 h-5 text-blue-500" />
              )}
              {title}
            </h3>
            {subtitle && (
              <p className="text-sm text-gray-500 mt-0.5">{subtitle}</p>
            )}
          </div>
        )}
      </div>

      {/* Chart */}
      {!collapsed && (
        <div className="p-6">
          <div style={{ height: chartHeight }}>
            <ResponsiveContainer width="100%" height="100%">
              {type === 'bar' && isHorizontal ? (
                <BarChart data={data} layout="vertical" margin={{ left: 10, right: 30, top: 10, bottom: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" horizontal={true} vertical={false} />
                  <XAxis 
                    type="number" 
                    tick={{ fill: '#6b7280', fontSize: 12 }} 
                    tickFormatter={formatNumber}
                  />
                  <YAxis 
                    type="category" 
                    dataKey={xKey} 
                    tick={{ fill: '#374151', fontSize: 12 }} 
                    width={180}
                    tickLine={false}
                    tickFormatter={(v) => typeof v === 'string' && v.length > 28 ? v.slice(0, 25) + '...' : v}
                  />
                  <Tooltip
                    formatter={(value) => [formatFullNumber(value as number), ""]}
                    contentStyle={{ borderRadius: '8px', border: '1px solid #e5e7eb', backgroundColor: 'white' }}
                  />
                  {showLegend && yKeys.length > 1 && <Legend />}
                  {yKeys.map((key, i) => (
                    <Bar
                      key={key}
                      dataKey={key}
                      fill={colors[i % colors.length]}
                      radius={[0, 4, 4, 0]}
                      name={key}
                    />
                  ))}
                </BarChart>
              ) : type === 'bar' ? (
                <BarChart data={data} margin={{ left: 10, right: 30, top: 10, bottom: 30 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis 
                    dataKey={xKey} 
                    tick={{ fill: '#6b7280', fontSize: 12 }}
                    tickFormatter={(v) => typeof v === 'string' && v.length > 15 ? v.slice(0, 12) + '...' : v}
                  />
                  <YAxis 
                    tickFormatter={formatNumber} 
                    tick={{ fill: '#6b7280', fontSize: 12 }}
                    domain={[0, Math.ceil(maxValue * 1.1)]}
                  />
                  <Tooltip
                    formatter={(value) => [formatFullNumber(value as number), ""]}
                    contentStyle={{ borderRadius: '8px', border: '1px solid #e5e7eb', backgroundColor: 'white' }}
                  />
                  {showLegend && yKeys.length > 1 && <Legend />}
                  {yKeys.map((key, i) => (
                    <Bar
                      key={key}
                      dataKey={key}
                      fill={colors[i % colors.length]}
                      radius={[4, 4, 0, 0]}
                      name={key}
                    />
                  ))}
                </BarChart>
              ) : (
                <LineChart data={data} margin={{ left: 10, right: 30, top: 10, bottom: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis 
                    dataKey={xKey} 
                    tick={{ fill: '#6b7280', fontSize: 12 }}
                  />
                  <YAxis
                    tickFormatter={formatNumber}
                    tick={{ fill: '#6b7280', fontSize: 12 }}
                    domain={[0, Math.ceil(maxValue * 1.1)]}
                    width={70}
                  />
                  <Tooltip
                    formatter={(value) => [formatFullNumber(value as number), ""]}
                    labelFormatter={(year) => `Year: ${year}`}
                    contentStyle={{ borderRadius: '8px', border: '1px solid #e5e7eb', backgroundColor: 'white' }}
                  />
                  {showLegend && yKeys.length > 1 && <Legend />}
                  {yKeys.map((key, i) => (
                    <Line
                      key={key}
                      type="monotone"
                      dataKey={key}
                      stroke={colors[i % colors.length]}
                      strokeWidth={2.5}
                      dot={{ fill: colors[i % colors.length], r: 4 }}
                      activeDot={{ r: 8 }}
                      name={key}
                    />
                  ))}
                </LineChart>
              )}
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </div>
  );
}

export default SharedChart;
