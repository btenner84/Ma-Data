'use client';

import { useState, useCallback, useRef, useEffect } from 'react';
import { 
  Send, Sparkles, User, ChevronDown, ChevronRight, 
  DollarSign, Clock, Database, Brain, CheckCircle, 
  XCircle, AlertCircle, Activity, Loader2, Zap
} from 'lucide-react';
import ReactMarkdown from 'react-markdown';

const API_BASE = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000').replace(/\/$/, '');

interface LLMCall {
  call_id: string;
  phase: string;
  model: string;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  cost_usd: number;
  latency_ms: number;
}

interface ToolCall {
  call_id: string;
  tool_name: string;
  arguments: Record<string, unknown>;
  result_preview: string;
  success: boolean;
  error?: string;
  latency_ms: number;
  rows_returned?: number;
}

interface AgentStep {
  step_id: string;
  step_number: number;
  phase: string;
  description: string;
  llm_calls: LLMCall[];
  tool_calls: ToolCall[];
  decision?: string;
  timestamp: string;
}

interface AgentResponse {
  answer: string;
  run_id: string;
  llm_calls: number;
  tool_calls: number;
  total_tokens: number;
  cost_usd: number;
  latency_ms: number;
  confidence: number;
  data_tables: Record<string, unknown>[];
  charts: Record<string, unknown>[];
  sources: string[];
  audit?: {
    steps: AgentStep[];
  };
}

interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  isLoading?: boolean;
  response?: AgentResponse;
}

const PHASE_ICONS: Record<string, React.ReactNode> = {
  planning: <Brain className="w-4 h-4" />,
  executing: <Database className="w-4 h-4" />,
  analyzing: <Activity className="w-4 h-4" />,
  validating: <CheckCircle className="w-4 h-4" />,
  synthesizing: <Sparkles className="w-4 h-4" />,
};

const PHASE_COLORS: Record<string, string> = {
  planning: 'text-purple-500 bg-purple-500/10',
  executing: 'text-blue-500 bg-blue-500/10',
  analyzing: 'text-green-500 bg-green-500/10',
  validating: 'text-yellow-500 bg-yellow-500/10',
  synthesizing: 'text-pink-500 bg-pink-500/10',
};

function formatCost(cost: number): string {
  if (cost < 0.01) return `$${(cost * 100).toFixed(2)}¢`;
  return `$${cost.toFixed(4)}`;
}

function formatLatency(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function StepDetails({ step }: { step: AgentStep }) {
  const [expanded, setExpanded] = useState(false);
  const phaseColor = PHASE_COLORS[step.phase] || 'text-gray-500 bg-gray-500/10';
  const phaseIcon = PHASE_ICONS[step.phase] || <Zap className="w-4 h-4" />;

  return (
    <div className="border-l-2 border-gray-200 dark:border-gray-700 pl-3 py-1">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full text-left hover:bg-gray-50 dark:hover:bg-gray-800/50 rounded p-1 -ml-1"
      >
        {expanded ? <ChevronDown className="w-3 h-3 text-gray-400" /> : <ChevronRight className="w-3 h-3 text-gray-400" />}
        <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium ${phaseColor}`}>
          {phaseIcon}
          {step.phase}
        </span>
        <span className="text-xs text-gray-500 truncate flex-1">{step.description}</span>
        {step.llm_calls.length > 0 && (
          <span className="text-xs text-gray-400">
            {step.llm_calls.reduce((sum, c) => sum + c.total_tokens, 0)} tokens
          </span>
        )}
      </button>

      {expanded && (
        <div className="mt-2 ml-5 space-y-2 text-xs">
          {step.llm_calls.map((call) => (
            <div key={call.call_id} className="p-2 bg-gray-50 dark:bg-gray-800 rounded">
              <div className="flex items-center gap-3 text-gray-500">
                <Brain className="w-3 h-3" />
                <span>LLM Call</span>
                <span className="text-gray-400">|</span>
                <span>{call.prompt_tokens} in</span>
                <span>{call.completion_tokens} out</span>
                <span className="text-gray-400">|</span>
                <span>{formatCost(call.cost_usd)}</span>
                <span className="text-gray-400">|</span>
                <span>{formatLatency(call.latency_ms)}</span>
              </div>
            </div>
          ))}

          {step.tool_calls.map((call) => (
            <div key={call.call_id} className="p-2 bg-gray-50 dark:bg-gray-800 rounded">
              <div className="flex items-center gap-3 text-gray-500">
                <Database className="w-3 h-3" />
                <span className="font-medium">{call.tool_name}</span>
                {call.success ? (
                  <CheckCircle className="w-3 h-3 text-green-500" />
                ) : (
                  <XCircle className="w-3 h-3 text-red-500" />
                )}
                {call.rows_returned !== undefined && (
                  <span className="text-gray-400">{call.rows_returned} rows</span>
                )}
                <span className="text-gray-400">{formatLatency(call.latency_ms)}</span>
              </div>
              {call.result_preview && (
                <pre className="mt-1 text-gray-600 dark:text-gray-400 truncate max-w-full">
                  {call.result_preview.slice(0, 150)}...
                </pre>
              )}
            </div>
          ))}

          {step.decision && (
            <div className="text-gray-500 italic">
              Decision: {step.decision}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function AuditPanel({ response }: { response: AgentResponse }) {
  const [showSteps, setShowSteps] = useState(false);

  return (
    <div className="mt-4 border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
      {/* Summary bar */}
      <div className="flex items-center justify-between px-3 py-2 bg-gray-50 dark:bg-gray-800/50 text-xs">
        <div className="flex items-center gap-4">
          <span className="flex items-center gap-1 text-gray-500">
            <Brain className="w-3 h-3" />
            {response.llm_calls} LLM calls
          </span>
          <span className="flex items-center gap-1 text-gray-500">
            <Database className="w-3 h-3" />
            {response.tool_calls} tools
          </span>
          <span className="flex items-center gap-1 text-gray-500">
            <Zap className="w-3 h-3" />
            {response.total_tokens.toLocaleString()} tokens
          </span>
        </div>
        <div className="flex items-center gap-4">
          <span className="flex items-center gap-1 text-green-600">
            <DollarSign className="w-3 h-3" />
            {formatCost(response.cost_usd)}
          </span>
          <span className="flex items-center gap-1 text-gray-500">
            <Clock className="w-3 h-3" />
            {formatLatency(response.latency_ms)}
          </span>
          <span className={`flex items-center gap-1 ${response.confidence >= 0.8 ? 'text-green-600' : response.confidence >= 0.6 ? 'text-yellow-600' : 'text-red-600'}`}>
            {(response.confidence * 100).toFixed(0)}% confidence
          </span>
        </div>
      </div>

      {/* Steps toggle */}
      {response.audit?.steps && response.audit.steps.length > 0 && (
        <>
          <button
            onClick={() => setShowSteps(!showSteps)}
            className="w-full px-3 py-2 flex items-center gap-2 text-xs text-gray-500 hover:bg-gray-50 dark:hover:bg-gray-800/30 border-t border-gray-200 dark:border-gray-700"
          >
            {showSteps ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
            {showSteps ? 'Hide' : 'Show'} agent steps ({response.audit.steps.length})
          </button>

          {showSteps && (
            <div className="p-3 border-t border-gray-200 dark:border-gray-700 space-y-1 max-h-96 overflow-y-auto">
              {response.audit.steps.map((step) => (
                <StepDetails key={step.step_id} step={step} />
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function AssistantMessage({ message }: { message: ChatMessage }) {
  return (
    <div className="flex items-start gap-3">
      <div className="w-8 h-8 rounded-full bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center flex-shrink-0 shadow-lg">
        <Sparkles className="w-4 h-4 text-white" />
      </div>
      <div className="flex-1 min-w-0">
        {message.isLoading ? (
          <div className="flex items-center gap-2 text-gray-500">
            <Loader2 className="w-4 h-4 animate-spin" />
            <span className="text-sm">Thinking...</span>
          </div>
        ) : (
          <>
            <div className="prose prose-sm dark:prose-invert max-w-none prose-p:my-2 prose-headings:mt-4 prose-headings:mb-2">
              <ReactMarkdown>{message.content}</ReactMarkdown>
            </div>

            {message.response && (
              <AuditPanel response={message.response} />
            )}
          </>
        )}
      </div>
    </div>
  );
}

function UserMessage({ message }: { message: ChatMessage }) {
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

export function ChatV2() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages, scrollToBottom]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    const userMessage: ChatMessage = {
      id: crypto.randomUUID(),
      role: 'user',
      content: input.trim(),
      timestamp: new Date().toISOString(),
    };

    const loadingMessage: ChatMessage = {
      id: crypto.randomUUID(),
      role: 'assistant',
      content: '',
      timestamp: new Date().toISOString(),
      isLoading: true,
    };

    setMessages((prev) => [...prev, userMessage, loadingMessage]);
    setInput('');
    setIsLoading(true);

    try {
      const response = await fetch(`${API_BASE}/api/v2/agent/ask`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question: userMessage.content,
          include_full_audit: true,
        }),
      });

      if (!response.ok) {
        throw new Error(`Request failed: ${response.status}`);
      }

      const data: AgentResponse = await response.json();

      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === loadingMessage.id
            ? {
                ...msg,
                content: data.answer,
                isLoading: false,
                response: data,
              }
            : msg
        )
      );
    } catch (error) {
      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === loadingMessage.id
            ? {
                ...msg,
                content: `Sorry, something went wrong: ${error instanceof Error ? error.message : 'Unknown error'}`,
                isLoading: false,
              }
            : msg
        )
      );
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Messages area */}
      <div className="flex-1 overflow-y-auto p-4 space-y-6">
        {messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-center text-gray-500">
            <Sparkles className="w-12 h-12 mb-4 text-blue-500 opacity-50" />
            <h3 className="text-lg font-medium text-gray-700 dark:text-gray-300">
              MA Intelligence Agent V2
            </h3>
            <p className="text-sm mt-1 max-w-md">
              Multi-step reasoning with full audit trail. Ask about rate notices, 
              enrollment trends, star ratings, risk adjustment - anything MA.
            </p>
            <div className="mt-4 grid grid-cols-2 gap-2 text-xs">
              <button
                onClick={() => setInput('Tell me about the 2027 advance notice')}
                className="px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-700 text-left"
              >
                2027 Advance Notice
              </button>
              <button
                onClick={() => setInput('Compare V24 vs V28 risk models')}
                className="px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-700 text-left"
              >
                V24 vs V28 Risk Models
              </button>
              <button
                onClick={() => setInput('What are the Part D benefit changes in 2025-2027?')}
                className="px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-700 text-left"
              >
                Part D Changes
              </button>
              <button
                onClick={() => setInput('Top 10 MA plans by enrollment growth')}
                className="px-3 py-2 bg-gray-100 dark:bg-gray-800 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-700 text-left"
              >
                Enrollment Leaders
              </button>
            </div>
          </div>
        ) : (
          messages.map((message) => (
            message.role === 'user' ? (
              <UserMessage key={message.id} message={message} />
            ) : (
              <AssistantMessage key={message.id} message={message} />
            )
          ))
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input area */}
      <div className="border-t border-gray-200 dark:border-gray-700 p-4">
        <form onSubmit={handleSubmit} className="flex gap-3">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about Medicare Advantage..."
            disabled={isLoading}
            className="flex-1 px-4 py-2 border border-gray-200 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50"
          />
          <button
            type="submit"
            disabled={isLoading || !input.trim()}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
          >
            {isLoading ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Send className="w-4 h-4" />
            )}
          </button>
        </form>

        {/* Usage hint */}
        <div className="mt-2 text-xs text-gray-400 text-center">
          Multi-step agent with planning, execution, analysis, validation, and synthesis
        </div>
      </div>
    </div>
  );
}

export default ChatV2;
