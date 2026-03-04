'use client';

import { AuditButton, type AuditMetadata } from '@/components/audit';

export interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  data?: Record<string, unknown>[];
  chart?: {
    type: 'line' | 'bar' | 'pie';
    data: unknown;
  };
  audit?: AuditMetadata;
  isError?: boolean;
}

interface ChatMessageProps {
  message: Message;
}

export function ChatMessage({ message }: ChatMessageProps) {
  const isUser = message.role === 'user';

  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div
        className={`max-w-[80%] rounded-lg px-4 py-3 ${
          isUser
            ? 'bg-blue-500 text-white'
            : message.isError
            ? 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300'
            : 'bg-gray-100 dark:bg-gray-700 text-gray-900 dark:text-white'
        }`}
      >
        {/* Message Content */}
        <div className="prose prose-sm dark:prose-invert max-w-none">
          {message.content.split('\n').map((line, i) => (
            <p key={i} className={i > 0 ? 'mt-2' : ''}>
              {line}
            </p>
          ))}
        </div>

        {/* Data Table */}
        {message.data && message.data.length > 0 && (
          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm border-collapse">
              <thead>
                <tr className="bg-gray-200 dark:bg-gray-600">
                  {Object.keys(message.data[0]).map((key) => (
                    <th
                      key={key}
                      className="px-3 py-1.5 text-left font-medium text-gray-700 dark:text-gray-200"
                    >
                      {key.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {message.data.slice(0, 10).map((row, i) => (
                  <tr
                    key={i}
                    className={i % 2 === 0 ? 'bg-white dark:bg-gray-800' : 'bg-gray-50 dark:bg-gray-750'}
                  >
                    {Object.values(row).map((value, j) => (
                      <td
                        key={j}
                        className="px-3 py-1.5 border-t border-gray-200 dark:border-gray-600"
                      >
                        {formatValue(value)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
            {message.data.length > 10 && (
              <p className="text-xs text-gray-500 mt-2">
                Showing 10 of {message.data.length} rows
              </p>
            )}
          </div>
        )}

        {/* Audit Button */}
        {message.audit && !isUser && (
          <div className="mt-2 flex justify-end">
            <AuditButton audit={message.audit} label="View query details" />
          </div>
        )}

        {/* Timestamp */}
        <div
          className={`text-xs mt-2 ${
            isUser ? 'text-blue-200' : 'text-gray-400 dark:text-gray-500'
          }`}
        >
          {new Date(message.timestamp).toLocaleTimeString()}
        </div>
      </div>
    </div>
  );
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return '-';
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value) && value > 1000) {
      return value.toLocaleString();
    }
    if (!Number.isInteger(value)) {
      return value.toFixed(2);
    }
  }
  return String(value);
}
