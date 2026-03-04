/**
 * Tests for AuditButton Component
 * 
 * Verifies:
 * - Renders correctly with audit metadata
 * - Hides when no audit data
 * - Opens modal on click
 * - Displays label when provided
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { AuditButton, AuditMetadata } from '@/components/audit/AuditButton';

const mockAudit: AuditMetadata = {
  query_id: 'test-123',
  sql: 'SELECT * FROM enrollment WHERE year = 2024',
  tables_queried: ['gold_fact_enrollment_national'],
  filters_applied: { year: 2024 },
  row_count: 100,
  source_files: ['s3://bucket/file.parquet'],
  pipeline_run_id: 'run-001',
  executed_at: '2024-01-15T10:00:00Z',
  execution_ms: 45,
};

describe('AuditButton', () => {
  describe('Rendering', () => {
    it('renders when audit data is provided', () => {
      render(<AuditButton audit={mockAudit} />);
      
      const button = screen.getByRole('button');
      expect(button).toBeInTheDocument();
    });

    it('does not render when audit is undefined', () => {
      render(<AuditButton audit={undefined} />);
      
      const button = screen.queryByRole('button');
      expect(button).not.toBeInTheDocument();
    });

    it('displays label when provided', () => {
      render(<AuditButton audit={mockAudit} label="View details" />);
      
      expect(screen.getByText('View details')).toBeInTheDocument();
    });

    it('applies custom className', () => {
      render(<AuditButton audit={mockAudit} className="custom-class" />);
      
      const button = screen.getByRole('button');
      expect(button).toHaveClass('custom-class');
    });
  });

  describe('Interaction', () => {
    it('opens modal when clicked', () => {
      render(<AuditButton audit={mockAudit} />);
      
      const button = screen.getByRole('button');
      fireEvent.click(button);
      
      expect(screen.getByText('Query Audit Details')).toBeInTheDocument();
    });

    it('modal shows SQL query', () => {
      render(<AuditButton audit={mockAudit} />);
      
      fireEvent.click(screen.getByRole('button'));
      
      expect(screen.getByText(/SELECT \* FROM enrollment/)).toBeInTheDocument();
    });

    it('modal shows tables queried', () => {
      render(<AuditButton audit={mockAudit} />);
      
      fireEvent.click(screen.getByRole('button'));
      
      expect(screen.getByText('gold_fact_enrollment_national')).toBeInTheDocument();
    });
  });

  describe('Accessibility', () => {
    it('has accessible title attribute', () => {
      render(<AuditButton audit={mockAudit} />);
      
      const button = screen.getByRole('button');
      expect(button).toHaveAttribute('title', 'View query details and data lineage');
    });
  });
});
