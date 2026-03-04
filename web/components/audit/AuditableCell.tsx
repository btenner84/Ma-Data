'use client';

import { useState } from 'react';
import { AuditModal } from './AuditModal';
import type { AuditMetadata } from './AuditButton';

interface AuditableCellProps {
  value: string | number | null | undefined;
  audit?: AuditMetadata;
  row?: string;
  column?: string;
  formula?: string;
  className?: string;
  formatter?: (value: any) => string;
  children?: React.ReactNode;
}

export function AuditableCell({
  value,
  audit,
  row,
  column,
  formula,
  className = '',
  formatter,
  children,
}: AuditableCellProps) {
  const [showModal, setShowModal] = useState(false);

  const displayValue = children ?? (formatter ? formatter(value) : (value !== null && value !== undefined ? String(value) : '—'));

  const handleClick = () => {
    if (audit) {
      setShowModal(true);
    }
  };

  return (
    <>
      <span
        onClick={handleClick}
        className={`${audit ? 'cursor-pointer hover:bg-blue-50 dark:hover:bg-blue-900/20 transition-colors' : ''} ${className}`}
        title={audit ? 'Click to view audit trail' : undefined}
      >
        {displayValue}
      </span>

      {audit && (
        <AuditModal
          isOpen={showModal}
          onClose={() => setShowModal(false)}
          audit={audit}
          cellValue={value}
          cellContext={{ row, column, formula }}
        />
      )}
    </>
  );
}

// Helper to create audit metadata from API response
export function createAuditFromResponse(
  auditId: string | undefined,
  sql: string,
  tables: string[],
  filters: Record<string, unknown> = {},
  rowCount: number = 0
): AuditMetadata | undefined {
  if (!auditId) return undefined;

  return {
    query_id: auditId,
    sql,
    tables_queried: tables,
    filters_applied: filters,
    row_count: rowCount,
    executed_at: new Date().toISOString(),
  };
}
