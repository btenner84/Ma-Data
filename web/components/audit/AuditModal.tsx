'use client';

import { useEffect, useRef } from 'react';
import type { AuditMetadata } from './AuditButton';

interface AuditModalProps {
  isOpen: boolean;
  onClose: () => void;
  audit: AuditMetadata;
}

export function AuditModal({ isOpen, onClose, audit }: AuditModalProps) {
  const modalRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };

    if (isOpen) {
      document.addEventListener('keydown', handleEscape);
      document.body.style.overflow = 'hidden';
    }

    return () => {
      document.removeEventListener('keydown', handleEscape);
      document.body.style.overflow = 'unset';
    };
  }, [isOpen, onClose]);

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
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b dark:border-gray-700">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
            Query Audit Details
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

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-4 space-y-4">
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
            <pre className="bg-gray-900 text-green-400 p-3 rounded text-xs overflow-x-auto">
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
