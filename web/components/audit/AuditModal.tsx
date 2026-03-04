'use client';

import { useEffect, useRef, useState } from 'react';
import type { AuditMetadata } from './AuditButton';

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

interface RawDataRecord {
  [key: string]: unknown;
}

interface ReplayResult {
  original_query_id: string;
  new_query_id: string;
  original_row_count: number;
  new_row_count: number;
  match: boolean;
  data: RawDataRecord[];
}

interface AuditModalProps {
  isOpen: boolean;
  onClose: () => void;
  audit: AuditMetadata;
  cellValue?: string | number | null;
  cellContext?: {
    row?: string;
    column?: string;
    formula?: string;
  };
}

export function AuditModal({ isOpen, onClose, audit, cellValue, cellContext }: AuditModalProps) {
  const modalRef = useRef<HTMLDivElement>(null);
  const [activeTab, setActiveTab] = useState<'query' | 'raw' | 'validate'>('query');
  const [rawData, setRawData] = useState<RawDataRecord[] | null>(null);
  const [rawDataLoading, setRawDataLoading] = useState(false);
  const [replayResult, setReplayResult] = useState<ReplayResult | null>(null);
  const [replayLoading, setReplayLoading] = useState(false);

  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };

    if (isOpen) {
      document.addEventListener('keydown', handleEscape);
      document.body.style.overflow = 'hidden';
      setActiveTab('query');
      setRawData(null);
      setReplayResult(null);
    }

    return () => {
      document.removeEventListener('keydown', handleEscape);
      document.body.style.overflow = 'unset';
    };
  }, [isOpen, onClose]);

  const fetchRawData = async () => {
    if (!audit.query_id) return;
    setRawDataLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/audit/${audit.query_id}/data?limit=100`);
      if (res.ok) {
        const data = await res.json();
        setRawData(data.records || data.data || []);
      }
    } catch (e) {
      console.error('Failed to fetch raw data:', e);
    } finally {
      setRawDataLoading(false);
    }
  };

  const replayQuery = async () => {
    if (!audit.query_id) return;
    setReplayLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/audit/replay/${audit.query_id}`, { method: 'POST' });
      if (res.ok) {
        const result = await res.json();
        setReplayResult(result);
      }
    } catch (e) {
      console.error('Failed to replay query:', e);
    } finally {
      setReplayLoading(false);
    }
  };

  if (!isOpen) return null;

  const formatDate = (dateStr: string) => {
    try {
      return new Date(dateStr).toLocaleString();
    } catch {
      return dateStr;
    }
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="fixed inset-0 bg-black/50" />
      
      <div
        ref={modalRef}
        className="relative bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-3xl w-full max-h-[80vh] overflow-hidden flex flex-col"
      >
        {/* Header with Cell Context */}
        <div className="p-4 border-b dark:border-gray-700">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
              Data Audit Trail
            </h2>
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
            >
              <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <line x1="18" y1="6" x2="6" y2="18" />
                <line x1="6" y1="6" x2="18" y2="18" />
              </svg>
            </button>
          </div>
          
          {/* Cell Value Display */}
          {cellValue !== undefined && (
            <div className="mt-3 p-3 bg-blue-50 dark:bg-blue-900/30 rounded-lg">
              <div className="text-xs text-blue-600 dark:text-blue-400 font-medium mb-1">
                Selected Value
              </div>
              <div className="text-2xl font-bold text-blue-900 dark:text-blue-100">
                {cellValue !== null ? String(cellValue) : '—'}
              </div>
              {cellContext && (
                <div className="mt-2 text-xs text-blue-700 dark:text-blue-300 space-y-1">
                  {cellContext.row && <div>Row: <span className="font-mono">{cellContext.row}</span></div>}
                  {cellContext.column && <div>Column: <span className="font-mono">{cellContext.column}</span></div>}
                  {cellContext.formula && (
                    <div className="mt-2 p-2 bg-blue-100 dark:bg-blue-900/50 rounded font-mono text-xs">
                      {cellContext.formula}
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {/* Tabs */}
          <div className="flex gap-1 mt-4">
            {(['query', 'raw', 'validate'] as const).map((tab) => (
              <button
                key={tab}
                onClick={() => {
                  setActiveTab(tab);
                  if (tab === 'raw' && !rawData) fetchRawData();
                  if (tab === 'validate' && !replayResult) replayQuery();
                }}
                className={`px-4 py-2 text-sm font-medium rounded-t-lg transition-colors ${
                  activeTab === tab
                    ? 'bg-gray-100 dark:bg-gray-700 text-gray-900 dark:text-white'
                    : 'text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300'
                }`}
              >
                {tab === 'query' && 'Query Details'}
                {tab === 'raw' && 'Raw Data'}
                {tab === 'validate' && 'Validate'}
              </button>
            ))}
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          {/* QUERY TAB */}
          {activeTab === 'query' && (
            <>
              {/* Query ID and Timing */}
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="text-xs font-medium text-gray-500 dark:text-gray-400">
                    Query ID
                  </label>
                  <p className="font-mono text-sm text-gray-900 dark:text-white">
                    {audit.query_id}
                  </p>
                </div>
                <div>
                  <label className="text-xs font-medium text-gray-500 dark:text-gray-400">
                    Executed At
                  </label>
                  <p className="text-sm text-gray-900 dark:text-white">
                    {formatDate(audit.executed_at)}
                  </p>
                </div>
              </div>

              {/* Stats */}
              <div className="grid grid-cols-3 gap-4">
                <div className="bg-gray-50 dark:bg-gray-700 rounded p-3">
                  <label className="text-xs font-medium text-gray-500 dark:text-gray-400">
                    Rows Returned
                  </label>
                  <p className="text-lg font-semibold text-gray-900 dark:text-white">
                    {audit.row_count.toLocaleString()}
                  </p>
                </div>
                {audit.execution_ms && (
                  <div className="bg-gray-50 dark:bg-gray-700 rounded p-3">
                    <label className="text-xs font-medium text-gray-500 dark:text-gray-400">
                      Execution Time
                    </label>
                    <p className="text-lg font-semibold text-gray-900 dark:text-white">
                      {audit.execution_ms.toFixed(0)}ms
                    </p>
                  </div>
                )}
                <div className="bg-gray-50 dark:bg-gray-700 rounded p-3">
                  <label className="text-xs font-medium text-gray-500 dark:text-gray-400">
                    Tables Queried
                  </label>
                  <p className="text-lg font-semibold text-gray-900 dark:text-white">
                    {audit.tables_queried.length}
                  </p>
                </div>
              </div>

              {/* SQL Query */}
              <div>
                <label className="text-xs font-medium text-gray-500 dark:text-gray-400 block mb-1">
                  SQL Query
                </label>
                <pre className="bg-gray-900 text-green-400 p-3 rounded text-xs overflow-x-auto whitespace-pre-wrap">
                  {audit.sql}
                </pre>
              </div>

              {/* Filters Applied */}
              {Object.keys(audit.filters_applied).length > 0 && (
                <div>
                  <label className="text-xs font-medium text-gray-500 dark:text-gray-400 block mb-1">
                    Filters Applied
                  </label>
                  <div className="bg-gray-50 dark:bg-gray-700 rounded p-3">
                    <pre className="text-xs text-gray-700 dark:text-gray-300">
                      {JSON.stringify(audit.filters_applied, null, 2)}
                    </pre>
                  </div>
                </div>
              )}

              {/* Tables */}
              <div>
                <label className="text-xs font-medium text-gray-500 dark:text-gray-400 block mb-1">
                  Tables Queried
                </label>
                <div className="flex flex-wrap gap-2">
                  {audit.tables_queried.map((table) => (
                    <span
                      key={table}
                      className="px-2 py-1 bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 rounded text-xs font-mono"
                    >
                      {table}
                    </span>
                  ))}
                </div>
              </div>

              {/* Source Files */}
              {audit.source_files && audit.source_files.length > 0 && (
                <div>
                  <label className="text-xs font-medium text-gray-500 dark:text-gray-400 block mb-1">
                    Source Files ({audit.source_files.length})
                  </label>
                  <div className="bg-gray-50 dark:bg-gray-700 rounded p-3 max-h-32 overflow-y-auto">
                    {audit.source_files.map((file, i) => (
                      <p key={i} className="text-xs font-mono text-gray-600 dark:text-gray-400">
                        {file}
                      </p>
                    ))}
                  </div>
                </div>
              )}

              {/* Pipeline Run */}
              {audit.pipeline_run_id && (
                <div>
                  <label className="text-xs font-medium text-gray-500 dark:text-gray-400 block mb-1">
                    Pipeline Run ID
                  </label>
                  <p className="font-mono text-sm text-gray-600 dark:text-gray-400">
                    {audit.pipeline_run_id}
                  </p>
                </div>
              )}
            </>
          )}

          {/* RAW DATA TAB */}
          {activeTab === 'raw' && (
            <div>
              <div className="flex items-center justify-between mb-3">
                <label className="text-xs font-medium text-gray-500 dark:text-gray-400">
                  Raw Data (First 100 rows)
                </label>
                <button
                  onClick={fetchRawData}
                  className="text-xs text-blue-500 hover:text-blue-700"
                >
                  Refresh
                </button>
              </div>
              
              {rawDataLoading ? (
                <div className="flex items-center justify-center py-12">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                </div>
              ) : rawData && rawData.length > 0 ? (
                <div className="overflow-x-auto border dark:border-gray-700 rounded-lg">
                  <table className="w-full text-xs">
                    <thead className="bg-gray-50 dark:bg-gray-700">
                      <tr>
                        {Object.keys(rawData[0]).map((col) => (
                          <th key={col} className="px-3 py-2 text-left font-medium text-gray-600 dark:text-gray-300 whitespace-nowrap">
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
                      {rawData.slice(0, 50).map((row, i) => (
                        <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                          {Object.values(row).map((val, j) => (
                            <td key={j} className="px-3 py-2 text-gray-700 dark:text-gray-300 whitespace-nowrap font-mono">
                              {val !== null && val !== undefined ? String(val) : '—'}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {rawData.length > 50 && (
                    <div className="px-3 py-2 text-xs text-gray-500 bg-gray-50 dark:bg-gray-700">
                      Showing 50 of {rawData.length} rows
                    </div>
                  )}
                </div>
              ) : (
                <div className="text-center py-12 text-gray-500">
                  No raw data available. The audit endpoint may not support data retrieval.
                </div>
              )}
            </div>
          )}

          {/* VALIDATE TAB */}
          {activeTab === 'validate' && (
            <div>
              <div className="flex items-center justify-between mb-3">
                <label className="text-xs font-medium text-gray-500 dark:text-gray-400">
                  Re-run Query to Validate
                </label>
                <button
                  onClick={replayQuery}
                  disabled={replayLoading}
                  className="px-3 py-1 text-xs bg-blue-500 text-white rounded hover:bg-blue-600 disabled:opacity-50"
                >
                  {replayLoading ? 'Running...' : 'Re-run Query'}
                </button>
              </div>

              {replayLoading ? (
                <div className="flex items-center justify-center py-12">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                </div>
              ) : replayResult ? (
                <div className="space-y-4">
                  {/* Validation Result */}
                  <div className={`p-4 rounded-lg ${replayResult.match ? 'bg-green-50 dark:bg-green-900/30' : 'bg-red-50 dark:bg-red-900/30'}`}>
                    <div className="flex items-center gap-2">
                      {replayResult.match ? (
                        <>
                          <svg className="w-5 h-5 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                          </svg>
                          <span className="font-medium text-green-700 dark:text-green-300">Data Validated</span>
                        </>
                      ) : (
                        <>
                          <svg className="w-5 h-5 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                          </svg>
                          <span className="font-medium text-red-700 dark:text-red-300">Data Mismatch Detected</span>
                        </>
                      )}
                    </div>
                    <p className="mt-2 text-sm text-gray-600 dark:text-gray-400">
                      Original: {replayResult.original_row_count.toLocaleString()} rows
                      {' → '}
                      Current: {replayResult.new_row_count.toLocaleString()} rows
                    </p>
                  </div>

                  {/* Fresh Data Preview */}
                  {replayResult.data && replayResult.data.length > 0 && (
                    <div>
                      <label className="text-xs font-medium text-gray-500 dark:text-gray-400 block mb-2">
                        Fresh Query Results
                      </label>
                      <div className="overflow-x-auto border dark:border-gray-700 rounded-lg max-h-64">
                        <table className="w-full text-xs">
                          <thead className="bg-gray-50 dark:bg-gray-700 sticky top-0">
                            <tr>
                              {Object.keys(replayResult.data[0]).map((col) => (
                                <th key={col} className="px-3 py-2 text-left font-medium text-gray-600 dark:text-gray-300 whitespace-nowrap">
                                  {col}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
                            {replayResult.data.slice(0, 20).map((row, i) => (
                              <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                                {Object.values(row).map((val, j) => (
                                  <td key={j} className="px-3 py-2 text-gray-700 dark:text-gray-300 whitespace-nowrap font-mono">
                                    {val !== null && val !== undefined ? String(val) : '—'}
                                  </td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                <div className="text-center py-12 text-gray-500">
                  Click "Re-run Query" to validate the data against the current source.
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex justify-end gap-2 p-4 border-t dark:border-gray-700">
          <button
            onClick={() => navigator.clipboard.writeText(audit.sql)}
            className="px-3 py-1.5 text-sm bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 rounded transition-colors"
          >
            Copy SQL
          </button>
          <button
            onClick={onClose}
            className="px-3 py-1.5 text-sm bg-blue-500 text-white hover:bg-blue-600 rounded transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
