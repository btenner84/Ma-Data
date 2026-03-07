'use client';

import { useState } from 'react';
import { ChevronDown, ChevronRight, Download, Table } from 'lucide-react';

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

function formatCellValue(val: unknown, colName?: string): { text: string; className?: string } {
  if (val === null || val === undefined) return { text: '-', className: 'text-gray-400' };
  
  if (typeof val === 'number') {
    const isPercent = colName?.toLowerCase().includes('pct') || 
                      colName?.toLowerCase().includes('percent') ||
                      colName?.toLowerCase().includes('share');
    const isChange = colName?.toLowerCase().includes('change') || 
                     colName?.toLowerCase().includes('growth') ||
                     colName?.toLowerCase().includes('yoy');
    
    if (isPercent) {
      const formatted = `${val.toFixed(1)}%`;
      if (isChange) {
        if (val > 0) return { text: `+${formatted}`, className: 'text-green-600 font-semibold' };
        if (val < 0) return { text: formatted, className: 'text-red-600 font-semibold' };
      }
      return { text: formatted, className: 'font-mono' };
    }
    
    if (isChange) {
      const formatted = val.toFixed(1);
      if (val > 0) return { text: `+${formatted}`, className: 'text-green-600 font-semibold' };
      if (val < 0) return { text: formatted, className: 'text-red-600 font-semibold' };
    }
    
    // Large numbers
    if (Math.abs(val) >= 1000) {
      return { text: formatFullNumber(val), className: 'font-mono' };
    }
    
    return { text: Number.isInteger(val) ? val.toString() : val.toFixed(2), className: 'font-mono' };
  }
  
  // String values
  const str = String(val);
  if (str === 'Yes' || str === 'RECOVERED') return { text: str, className: 'text-green-600 font-medium' };
  if (str === 'No' || str === 'NOT_RECOVERED') return { text: str, className: 'text-red-600 font-medium' };
  if (str === 'Partial' || str === 'Ongoing') return { text: str, className: 'text-yellow-600 font-medium' };
  
  return { text: str };
}

export interface TableData {
  [key: string]: string | number | null | undefined;
}

export interface SharedTableProps {
  title: string;
  subtitle?: string;
  data: TableData[];
  columns?: string[];
  showRowNumbers?: boolean;
  showYoY?: boolean;
  yoyColumns?: string[];
  collapsible?: boolean;
  defaultCollapsed?: boolean;
  maxRows?: number;
  downloadable?: boolean;
  colorColumn?: string;
  colors?: string[];
}

const COLORS = [
  "#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c",
  "#0891b2", "#4f46e5", "#c026d3", "#059669", "#d97706",
];

export function SharedTable({
  title,
  subtitle,
  data,
  columns,
  showRowNumbers = false,
  showYoY = false,
  yoyColumns = [],
  collapsible = true,
  defaultCollapsed = false,
  maxRows = 50,
  downloadable = true,
  colorColumn,
  colors = COLORS,
}: SharedTableProps) {
  const [collapsed, setCollapsed] = useState(defaultCollapsed);
  
  if (!data || data.length === 0) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-center gap-2 text-gray-500">
          <Table className="w-5 h-5" />
          <span>{title}</span>
          <span className="text-sm">(No data)</span>
        </div>
      </div>
    );
  }

  // Infer columns from data if not provided
  const tableColumns = columns || Object.keys(data[0]);
  const displayData = data.slice(0, maxRows);
  
  // Download CSV
  const downloadCSV = () => {
    const headers = tableColumns.join(',');
    const rows = displayData.map(row =>
      tableColumns.map(col => {
        const val = row[col];
        const { text } = formatCellValue(val, col);
        return text.includes(',') ? `"${text}"` : text;
      }).join(',')
    );
    const csv = [headers, ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${title.replace(/\s+/g, '_')}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // Get color for row based on colorColumn index
  const getRowColor = (rowIndex: number) => {
    if (!colorColumn) return undefined;
    return colors[rowIndex % colors.length];
  };

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-gray-50 to-white flex items-center justify-between">
        {collapsible ? (
          <button
            onClick={() => setCollapsed(!collapsed)}
            className="flex items-center gap-3 text-left flex-1"
          >
            {collapsed ? (
              <ChevronRight className="w-5 h-5 text-gray-400" />
            ) : (
              <ChevronDown className="w-5 h-5 text-gray-400" />
            )}
            <div>
              <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
                <Table className="w-5 h-5 text-blue-500" />
                {title}
                <span className="text-sm font-normal text-gray-500">({data.length} rows)</span>
              </h3>
              {subtitle && (
                <p className="text-sm text-gray-500 mt-0.5">{subtitle}</p>
              )}
            </div>
          </button>
        ) : (
          <div>
            <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
              <Table className="w-5 h-5 text-blue-500" />
              {title}
              <span className="text-sm font-normal text-gray-500">({data.length} rows)</span>
            </h3>
            {subtitle && (
              <p className="text-sm text-gray-500 mt-0.5">{subtitle}</p>
            )}
          </div>
        )}
        
        {downloadable && !collapsed && (
          <button
            onClick={downloadCSV}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-gray-600 hover:text-blue-600 hover:bg-blue-50 rounded-lg transition-colors"
          >
            <Download className="w-4 h-4" />
            CSV
          </button>
        )}
      </div>

      {/* Table */}
      {!collapsed && (
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                {showRowNumbers && (
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider w-12">
                    #
                  </th>
                )}
                {tableColumns.map((col, i) => (
                  <th 
                    key={col} 
                    className={`px-4 py-3 text-xs font-semibold text-gray-600 uppercase tracking-wider whitespace-nowrap ${
                      i === 0 ? 'text-left sticky left-0 bg-gray-50' : 'text-right'
                    }`}
                  >
                    {col.replace(/_/g, ' ')}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {displayData.map((row, rowIdx) => (
                <tr 
                  key={rowIdx} 
                  className={`${rowIdx % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'} hover:bg-blue-50/50 transition-colors`}
                >
                  {showRowNumbers && (
                    <td className="px-4 py-3 text-sm text-gray-400 font-mono">
                      {rowIdx + 1}
                    </td>
                  )}
                  {tableColumns.map((col, colIdx) => {
                    const { text, className } = formatCellValue(row[col], col);
                    const rowColor = colIdx === 0 ? getRowColor(rowIdx) : undefined;
                    
                    return (
                      <td 
                        key={col}
                        className={`px-4 py-3 text-sm ${colIdx === 0 ? 'sticky left-0 bg-inherit font-medium text-gray-900' : 'text-right text-gray-700'} ${className || ''}`}
                      >
                        <div className="flex items-center gap-2">
                          {colIdx === 0 && rowColor && (
                            <span 
                              className="w-2.5 h-2.5 rounded-full flex-shrink-0" 
                              style={{ backgroundColor: rowColor }}
                            />
                          )}
                          <span className={colIdx === 0 ? 'truncate max-w-[250px]' : ''} title={colIdx === 0 ? text : undefined}>
                            {text}
                          </span>
                        </div>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
          
          {data.length > maxRows && (
            <div className="px-4 py-3 text-sm text-gray-500 bg-gray-50 border-t border-gray-200">
              Showing {maxRows} of {data.length} rows
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default SharedTable;
