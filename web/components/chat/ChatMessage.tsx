'use client';

import { useState } from 'react';
import { ThumbsUp, ThumbsDown, ChevronDown, ChevronUp, Copy, Check, User, Sparkles } from 'lucide-react';
import ReactMarkdown from 'react-markdown';

export interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  queryId?: string;
  confidence?: number;
  sources?: string[];
  toolsUsed?: Array<{ tool: string; success: boolean }>;
  sqlExecuted?: string[];
  warnings?: string[];
  data?: Record<string, unknown>[];
  chart?: {
    type: 'line' | 'bar' | 'pie';
    data: unknown;
  };
  audit?: Record<string, unknown>;
  isError?: boolean;
}

interface ChatMessageProps {
  message: Message;
  onFeedback?: (queryId: string, rating: 'positive' | 'negative' | 'correction', correction?: string) => void;
}

export function ChatMessage({ message, onFeedback }: ChatMessageProps) {
  const isUser = message.role === 'user';
  const [showDetails, setShowDetails] = useState(false);
  const [showCorrectionInput, setShowCorrectionInput] = useState(false);
  const [correction, setCorrection] = useState('');
  const [feedbackGiven, setFeedbackGiven] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const handleFeedback = (rating: 'positive' | 'negative') => {
    if (onFeedback && message.queryId) {
      onFeedback(message.queryId, rating);
      setFeedbackGiven(rating);
    }
  };

  const handleCorrection = () => {
    if (onFeedback && message.queryId && correction.trim()) {
      onFeedback(message.queryId, 'correction', correction);
      setFeedbackGiven('correction');
      setShowCorrectionInput(false);
    }
  };

  const handleCopy = async () => {
    await navigator.clipboard.writeText(message.content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  if (isUser) {
    return (
      <div className="flex items-start gap-3">
        <div className="w-8 h-8 rounded-full bg-gray-200 dark:bg-gray-700 flex items-center justify-center flex-shrink-0">
          <User className="w-4 h-4 text-gray-600 dark:text-gray-300" />
        </div>
        <div className="flex-1">
          <p className="text-gray-900 dark:text-white">{message.content}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex items-start gap-3">
      <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center flex-shrink-0">
        <Sparkles className="w-4 h-4 text-white" />
      </div>
      <div className="flex-1 min-w-0">
        {/* Response content with markdown */}
        <div className="prose prose-sm dark:prose-invert max-w-none prose-p:my-2 prose-headings:mt-4 prose-headings:mb-2 prose-table:text-sm prose-td:py-1 prose-th:py-1">
          <ReactMarkdown>{message.content}</ReactMarkdown>
        </div>

        {/* Warnings */}
        {message.warnings && message.warnings.length > 0 && (
          <div className="mt-3 text-sm text-amber-600 dark:text-amber-400">
            {message.warnings.map((w, i) => (
              <p key={i}>⚠️ {w}</p>
            ))}
          </div>
        )}

        {/* Sources - subtle */}
        {message.sources && message.sources.length > 0 && (
          <div className="mt-3 text-xs text-gray-400 dark:text-gray-500">
            Sources: {message.sources.join(', ')}
          </div>
        )}

        {/* Action bar */}
        <div className="mt-3 flex items-center gap-1">
          {/* Copy button */}
          <button
            onClick={handleCopy}
            className="p-1.5 rounded hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
            title="Copy response"
          >
            {copied ? <Check className="w-4 h-4 text-green-500" /> : <Copy className="w-4 h-4" />}
          </button>

          {/* Feedback buttons */}
          {!message.isError && (
            <>
              {feedbackGiven ? (
                <span className="text-xs text-gray-400 ml-2">
                  {feedbackGiven === 'correction' ? 'Correction saved' : 'Thanks!'}
                </span>
              ) : (
                <>
                  <button
                    onClick={() => handleFeedback('positive')}
                    className="p-1.5 rounded hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-400 hover:text-green-600 transition-colors"
                    title="Good response"
                  >
                    <ThumbsUp className="w-4 h-4" />
                  </button>
                  <button
                    onClick={() => handleFeedback('negative')}
                    className="p-1.5 rounded hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-400 hover:text-red-500 transition-colors"
                    title="Bad response"
                  >
                    <ThumbsDown className="w-4 h-4" />
                  </button>
                </>
              )}
            </>
          )}

          {/* Details toggle */}
          {(message.sqlExecuted?.length || message.toolsUsed?.length) && (
            <button
              onClick={() => setShowDetails(!showDetails)}
              className="ml-2 flex items-center gap-1 text-xs text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
            >
              {showDetails ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
              Details
            </button>
          )}

          {/* Correct link */}
          {!feedbackGiven && !showCorrectionInput && (
            <button
              onClick={() => setShowCorrectionInput(true)}
              className="ml-auto text-xs text-gray-400 hover:text-blue-500"
            >
              Correct this
            </button>
          )}
        </div>

        {/* Details panel */}
        {showDetails && (
          <div className="mt-2 p-3 bg-gray-50 dark:bg-gray-900 rounded-lg text-xs space-y-2 font-mono">
            {message.toolsUsed && message.toolsUsed.length > 0 && (
              <div className="text-gray-500">
                <span className="font-semibold">Tools:</span>{' '}
                {message.toolsUsed.map((t, i) => (
                  <span key={i} className={t.success ? 'text-green-600' : 'text-red-500'}>
                    {t.tool}{i < message.toolsUsed!.length - 1 ? ', ' : ''}
                  </span>
                ))}
              </div>
            )}
            {message.sqlExecuted && message.sqlExecuted.length > 0 && (
              <div>
                <span className="text-gray-500 font-semibold">SQL:</span>
                {message.sqlExecuted.map((sql, i) => (
                  <pre key={i} className="mt-1 p-2 bg-gray-100 dark:bg-gray-800 rounded overflow-x-auto whitespace-pre-wrap text-gray-700 dark:text-gray-300">
                    {sql}
                  </pre>
                ))}
              </div>
            )}
            {message.audit && (
              <div className="text-gray-400">
                Model: {String(message.audit.model)} | Latency: {String(message.audit.latency_ms)}ms
              </div>
            )}
          </div>
        )}

        {/* Correction input */}
        {showCorrectionInput && !feedbackGiven && (
          <div className="mt-3 space-y-2">
            <textarea
              value={correction}
              onChange={(e) => setCorrection(e.target.value)}
              placeholder="What should the correct answer be?"
              className="w-full px-3 py-2 text-sm border border-gray-200 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-900 dark:text-white"
              rows={3}
            />
            <div className="flex gap-2">
              <button
                onClick={handleCorrection}
                disabled={!correction.trim()}
                className="px-3 py-1.5 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
              >
                Submit
              </button>
              <button
                onClick={() => setShowCorrectionInput(false)}
                className="px-3 py-1.5 text-sm text-gray-500 hover:text-gray-700"
              >
                Cancel
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
