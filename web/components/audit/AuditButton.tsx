'use client';

import { useState } from 'react';
import { AuditModal } from './AuditModal';

export interface AuditMetadata {
  query_id: string;
  sql: string;
  tables_queried: string[];
  filters_applied: Record<string, unknown>;
  row_count: number;
  source_files?: string[];
  pipeline_run_id?: string;
  executed_at: string;
  execution_ms?: number;
}

interface AuditButtonProps {
  audit?: AuditMetadata;
  label?: string;
  className?: string;
}

export function AuditButton({ audit, label, className = '' }: AuditButtonProps) {
  const [isOpen, setIsOpen] = useState(false);

  if (!audit) {
    return null;
  }

  return (
    <>
      <button
        onClick={() => setIsOpen(true)}
        className={`inline-flex items-center gap-1 text-xs text-gray-400 hover:text-blue-500 transition-colors ${className}`}
        title="View query details and data lineage"
      >
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="14"
          height="14"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <circle cx="12" cy="12" r="10" />
          <line x1="12" y1="16" x2="12" y2="12" />
          <line x1="12" y1="8" x2="12.01" y2="8" />
        </svg>
        {label && <span>{label}</span>}
      </button>

      <AuditModal
        isOpen={isOpen}
        onClose={() => setIsOpen(false)}
        audit={audit}
      />
    </>
  );
}
