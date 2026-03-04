/**
 * Tests for ChatInput Component
 * 
 * Verifies:
 * - Input handling
 * - Submit on Enter
 * - Submit on button click
 * - Disabled state
 * - Auto-resize behavior
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ChatInput } from '@/components/chat/ChatInput';

describe('ChatInput', () => {
  const mockOnSend = jest.fn();

  beforeEach(() => {
    mockOnSend.mockClear();
  });

  describe('Rendering', () => {
    it('renders textarea', () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      expect(screen.getByRole('textbox')).toBeInTheDocument();
    });

    it('renders submit button', () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      expect(screen.getByRole('button')).toBeInTheDocument();
    });

    it('displays placeholder text', () => {
      render(<ChatInput onSend={mockOnSend} placeholder="Type here..." />);
      
      expect(screen.getByPlaceholderText('Type here...')).toBeInTheDocument();
    });

    it('uses default placeholder when not provided', () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      expect(screen.getByPlaceholderText(/Ask about enrollment/)).toBeInTheDocument();
    });
  });

  describe('Input Handling', () => {
    it('updates value as user types', async () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      const input = screen.getByRole('textbox');
      await userEvent.type(input, 'Hello world');
      
      expect(input).toHaveValue('Hello world');
    });

    it('clears input after successful send', async () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      const input = screen.getByRole('textbox');
      await userEvent.type(input, 'Test message');
      await userEvent.keyboard('{Enter}');
      
      expect(input).toHaveValue('');
    });
  });

  describe('Submit Behavior', () => {
    it('calls onSend when Enter pressed', async () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      const input = screen.getByRole('textbox');
      await userEvent.type(input, 'Test message');
      await userEvent.keyboard('{Enter}');
      
      expect(mockOnSend).toHaveBeenCalledWith('Test message');
    });

    it('calls onSend when button clicked', async () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      const input = screen.getByRole('textbox');
      await userEvent.type(input, 'Test message');
      
      const button = screen.getByRole('button');
      await userEvent.click(button);
      
      expect(mockOnSend).toHaveBeenCalledWith('Test message');
    });

    it('does not call onSend with empty input', async () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      const button = screen.getByRole('button');
      await userEvent.click(button);
      
      expect(mockOnSend).not.toHaveBeenCalled();
    });

    it('does not call onSend with whitespace-only input', async () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      const input = screen.getByRole('textbox');
      await userEvent.type(input, '   ');
      await userEvent.keyboard('{Enter}');
      
      expect(mockOnSend).not.toHaveBeenCalled();
    });

    it('trims whitespace from message', async () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      const input = screen.getByRole('textbox');
      await userEvent.type(input, '  Test message  ');
      await userEvent.keyboard('{Enter}');
      
      expect(mockOnSend).toHaveBeenCalledWith('Test message');
    });
  });

  describe('Multiline Support', () => {
    it('allows Shift+Enter for new line', async () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      const input = screen.getByRole('textbox');
      await userEvent.type(input, 'Line 1{Shift>}{Enter}{/Shift}Line 2');
      
      expect(input).toHaveValue('Line 1\nLine 2');
      expect(mockOnSend).not.toHaveBeenCalled();
    });
  });

  describe('Disabled State', () => {
    it('disables textarea when disabled prop is true', () => {
      render(<ChatInput onSend={mockOnSend} disabled={true} />);
      
      expect(screen.getByRole('textbox')).toBeDisabled();
    });

    it('disables button when disabled prop is true', () => {
      render(<ChatInput onSend={mockOnSend} disabled={true} />);
      
      expect(screen.getByRole('button')).toBeDisabled();
    });

    it('does not call onSend when disabled', async () => {
      render(<ChatInput onSend={mockOnSend} disabled={true} />);
      
      const input = screen.getByRole('textbox');
      fireEvent.change(input, { target: { value: 'Test' } });
      fireEvent.keyDown(input, { key: 'Enter' });
      
      expect(mockOnSend).not.toHaveBeenCalled();
    });
  });

  describe('Button State', () => {
    it('disables button when input is empty', () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      expect(screen.getByRole('button')).toBeDisabled();
    });

    it('enables button when input has content', async () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      const input = screen.getByRole('textbox');
      await userEvent.type(input, 'Test');
      
      expect(screen.getByRole('button')).not.toBeDisabled();
    });
  });

  describe('Accessibility', () => {
    it('textarea is focusable', () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      const input = screen.getByRole('textbox');
      input.focus();
      
      expect(input).toHaveFocus();
    });

    it('button is keyboard accessible', async () => {
      render(<ChatInput onSend={mockOnSend} />);
      
      const input = screen.getByRole('textbox');
      await userEvent.type(input, 'Test');
      
      const button = screen.getByRole('button');
      button.focus();
      await userEvent.keyboard('{Enter}');
      
      expect(mockOnSend).toHaveBeenCalled();
    });
  });
});
