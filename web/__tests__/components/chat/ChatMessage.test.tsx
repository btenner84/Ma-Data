/**
 * Tests for ChatMessage Component
 * 
 * Verifies:
 * - User vs assistant message styling
 * - Data table rendering
 * - Audit button integration
 * - Error state handling
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { ChatMessage, Message } from '@/components/chat/ChatMessage';

describe('ChatMessage', () => {
  describe('User Messages', () => {
    const userMessage: Message = {
      id: '1',
      role: 'user',
      content: 'What is total enrollment?',
      timestamp: '2024-01-15T10:00:00Z',
    };

    it('renders user message content', () => {
      render(<ChatMessage message={userMessage} />);
      
      expect(screen.getByText('What is total enrollment?')).toBeInTheDocument();
    });

    it('applies user message styling', () => {
      render(<ChatMessage message={userMessage} />);
      
      const messageContainer = screen.getByText('What is total enrollment?').closest('div');
      expect(messageContainer).toHaveClass('bg-blue-500');
    });

    it('aligns user messages to the right', () => {
      render(<ChatMessage message={userMessage} />);
      
      const wrapper = screen.getByText('What is total enrollment?').parentElement?.parentElement;
      expect(wrapper).toHaveClass('justify-end');
    });
  });

  describe('Assistant Messages', () => {
    const assistantMessage: Message = {
      id: '2',
      role: 'assistant',
      content: 'Total MA enrollment is 33 million.',
      timestamp: '2024-01-15T10:00:01Z',
    };

    it('renders assistant message content', () => {
      render(<ChatMessage message={assistantMessage} />);
      
      expect(screen.getByText('Total MA enrollment is 33 million.')).toBeInTheDocument();
    });

    it('applies assistant message styling', () => {
      render(<ChatMessage message={assistantMessage} />);
      
      const messageContainer = screen.getByText('Total MA enrollment is 33 million.').closest('div');
      expect(messageContainer).toHaveClass('bg-gray-100');
    });

    it('aligns assistant messages to the left', () => {
      render(<ChatMessage message={assistantMessage} />);
      
      const wrapper = screen.getByText('Total MA enrollment is 33 million.').parentElement?.parentElement;
      expect(wrapper).toHaveClass('justify-start');
    });
  });

  describe('Data Tables', () => {
    const messageWithData: Message = {
      id: '3',
      role: 'assistant',
      content: 'Here are the results:',
      timestamp: '2024-01-15T10:00:02Z',
      data: [
        { year: 2022, enrollment: 29000000 },
        { year: 2023, enrollment: 30500000 },
        { year: 2024, enrollment: 33000000 },
      ],
    };

    it('renders data table when data is present', () => {
      render(<ChatMessage message={messageWithData} />);
      
      expect(screen.getByRole('table')).toBeInTheDocument();
    });

    it('displays column headers', () => {
      render(<ChatMessage message={messageWithData} />);
      
      expect(screen.getByText('Year')).toBeInTheDocument();
      expect(screen.getByText('Enrollment')).toBeInTheDocument();
    });

    it('displays data rows', () => {
      render(<ChatMessage message={messageWithData} />);
      
      expect(screen.getByText('2022')).toBeInTheDocument();
      expect(screen.getByText('29,000,000')).toBeInTheDocument();
    });

    it('limits displayed rows to 10', () => {
      const manyRows = Array.from({ length: 15 }, (_, i) => ({
        year: 2010 + i,
        enrollment: 20000000 + i * 1000000,
      }));

      const messageWithManyRows: Message = {
        ...messageWithData,
        data: manyRows,
      };

      render(<ChatMessage message={messageWithManyRows} />);
      
      expect(screen.getByText(/Showing 10 of 15 rows/)).toBeInTheDocument();
    });
  });

  describe('Audit Integration', () => {
    const messageWithAudit: Message = {
      id: '4',
      role: 'assistant',
      content: 'Query results:',
      timestamp: '2024-01-15T10:00:03Z',
      audit: {
        query_id: 'test-123',
        sql: 'SELECT * FROM enrollment',
        tables_queried: ['enrollment'],
        filters_applied: {},
        row_count: 10,
        executed_at: '2024-01-15T10:00:03Z',
      },
    };

    it('renders audit button when audit data present', () => {
      render(<ChatMessage message={messageWithAudit} />);
      
      expect(screen.getByText('View query details')).toBeInTheDocument();
    });

    it('does not render audit button for user messages', () => {
      const userWithAudit: Message = {
        ...messageWithAudit,
        role: 'user',
      };

      render(<ChatMessage message={userWithAudit} />);
      
      expect(screen.queryByText('View query details')).not.toBeInTheDocument();
    });
  });

  describe('Error Messages', () => {
    const errorMessage: Message = {
      id: '5',
      role: 'assistant',
      content: 'Sorry, an error occurred.',
      timestamp: '2024-01-15T10:00:04Z',
      isError: true,
    };

    it('applies error styling', () => {
      render(<ChatMessage message={errorMessage} />);
      
      const messageContainer = screen.getByText('Sorry, an error occurred.').closest('div');
      expect(messageContainer).toHaveClass('bg-red-50');
    });
  });

  describe('Timestamp Display', () => {
    const message: Message = {
      id: '6',
      role: 'user',
      content: 'Test message',
      timestamp: '2024-01-15T10:30:00Z',
    };

    it('displays formatted timestamp', () => {
      render(<ChatMessage message={message} />);
      
      expect(screen.getByText(/10:30/)).toBeInTheDocument();
    });
  });

  describe('Multi-line Content', () => {
    const multilineMessage: Message = {
      id: '7',
      role: 'assistant',
      content: 'Line 1\nLine 2\nLine 3',
      timestamp: '2024-01-15T10:00:05Z',
    };

    it('renders multiple paragraphs for newlines', () => {
      render(<ChatMessage message={multilineMessage} />);
      
      expect(screen.getByText('Line 1')).toBeInTheDocument();
      expect(screen.getByText('Line 2')).toBeInTheDocument();
      expect(screen.getByText('Line 3')).toBeInTheDocument();
    });
  });
});
