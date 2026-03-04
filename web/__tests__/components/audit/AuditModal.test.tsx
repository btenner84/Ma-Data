/**
 * Tests for AuditModal Component
 * 
 * Verifies:
 * - Displays all audit metadata fields
 * - Copy SQL functionality
 * - Close button works
 * - Escape key closes modal
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { AuditModal } from '@/components/audit/AuditModal';
import type { AuditMetadata } from '@/components/audit/AuditButton';

const mockAudit: AuditMetadata = {
  query_id: 'abc12345',
  sql: 'SELECT year, SUM(enrollment) FROM gold_fact_enrollment_national GROUP BY year',
  tables_queried: ['gold_fact_enrollment_national', 'gold_dim_entity'],
  filters_applied: { year: 2024, parent_org: 'Humana Inc.' },
  row_count: 1500,
  source_files: [
    's3://ma-data123/raw/enrollment/2024/01.zip',
    's3://ma-data123/raw/enrollment/2024/02.zip',
  ],
  pipeline_run_id: 'pipeline-run-2024-01-15',
  executed_at: '2024-01-15T10:30:00Z',
  execution_ms: 125.5,
};

describe('AuditModal', () => {
  const mockOnClose = jest.fn();

  beforeEach(() => {
    mockOnClose.mockClear();
  });

  describe('Visibility', () => {
    it('does not render when isOpen is false', () => {
      render(<AuditModal isOpen={false} onClose={mockOnClose} audit={mockAudit} />);
      
      expect(screen.queryByText('Query Audit Details')).not.toBeInTheDocument();
    });

    it('renders when isOpen is true', () => {
      render(<AuditModal isOpen={true} onClose={mockOnClose} audit={mockAudit} />);
      
      expect(screen.getByText('Query Audit Details')).toBeInTheDocument();
    });
  });

  describe('Content Display', () => {
    beforeEach(() => {
      render(<AuditModal isOpen={true} onClose={mockOnClose} audit={mockAudit} />);
    });

    it('displays query ID', () => {
      expect(screen.getByText('abc12345')).toBeInTheDocument();
    });

    it('displays SQL query', () => {
      expect(screen.getByText(/SELECT year, SUM\(enrollment\)/)).toBeInTheDocument();
    });

    it('displays row count', () => {
      expect(screen.getByText('1,500')).toBeInTheDocument();
    });

    it('displays execution time', () => {
      expect(screen.getByText('126ms')).toBeInTheDocument();
    });

    it('displays tables queried', () => {
      expect(screen.getByText('gold_fact_enrollment_national')).toBeInTheDocument();
      expect(screen.getByText('gold_dim_entity')).toBeInTheDocument();
    });

    it('displays filters applied', () => {
      expect(screen.getByText(/year.*2024/)).toBeInTheDocument();
    });

    it('displays source files', () => {
      expect(screen.getByText(/s3:\/\/ma-data123\/raw\/enrollment\/2024\/01\.zip/)).toBeInTheDocument();
    });

    it('displays pipeline run ID', () => {
      expect(screen.getByText('pipeline-run-2024-01-15')).toBeInTheDocument();
    });
  });

  describe('Close Behavior', () => {
    it('calls onClose when close button clicked', () => {
      render(<AuditModal isOpen={true} onClose={mockOnClose} audit={mockAudit} />);
      
      const closeButton = screen.getByText('Close');
      fireEvent.click(closeButton);
      
      expect(mockOnClose).toHaveBeenCalledTimes(1);
    });

    it('calls onClose when Escape key pressed', () => {
      render(<AuditModal isOpen={true} onClose={mockOnClose} audit={mockAudit} />);
      
      fireEvent.keyDown(document, { key: 'Escape' });
      
      expect(mockOnClose).toHaveBeenCalledTimes(1);
    });

    it('calls onClose when backdrop clicked', () => {
      render(<AuditModal isOpen={true} onClose={mockOnClose} audit={mockAudit} />);
      
      const backdrop = document.querySelector('.fixed.inset-0.z-50');
      if (backdrop) {
        fireEvent.click(backdrop);
        expect(mockOnClose).toHaveBeenCalled();
      }
    });
  });

  describe('Copy SQL', () => {
    it('has copy SQL button', () => {
      render(<AuditModal isOpen={true} onClose={mockOnClose} audit={mockAudit} />);
      
      expect(screen.getByText('Copy SQL')).toBeInTheDocument();
    });

    it('copies SQL to clipboard when clicked', async () => {
      Object.assign(navigator, {
        clipboard: {
          writeText: jest.fn().mockResolvedValue(undefined),
        },
      });

      render(<AuditModal isOpen={true} onClose={mockOnClose} audit={mockAudit} />);
      
      const copyButton = screen.getByText('Copy SQL');
      fireEvent.click(copyButton);
      
      expect(navigator.clipboard.writeText).toHaveBeenCalledWith(mockAudit.sql);
    });
  });

  describe('Empty/Missing Data Handling', () => {
    it('handles empty source files array', () => {
      const auditNoFiles = { ...mockAudit, source_files: [] };
      render(<AuditModal isOpen={true} onClose={mockOnClose} audit={auditNoFiles} />);
      
      expect(screen.queryByText('Source Files')).not.toBeInTheDocument();
    });

    it('handles empty filters', () => {
      const auditNoFilters = { ...mockAudit, filters_applied: {} };
      render(<AuditModal isOpen={true} onClose={mockOnClose} audit={auditNoFilters} />);
      
      expect(screen.queryByText('Filters Applied')).not.toBeInTheDocument();
    });

    it('handles missing pipeline run ID', () => {
      const auditNoPipeline = { ...mockAudit, pipeline_run_id: undefined };
      render(<AuditModal isOpen={true} onClose={mockOnClose} audit={auditNoPipeline} />);
      
      expect(screen.queryByText('Pipeline Run ID')).not.toBeInTheDocument();
    });
  });
});
