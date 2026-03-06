'use client';

import { useState, useEffect } from 'react';
import { 
  DollarSign, Clock, Zap, Brain, Database, 
  TrendingUp, BarChart3, RefreshCw
} from 'lucide-react';

const API_BASE = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000').replace(/\/$/, '');

interface Metrics {
  total_runs: number;
  total_cost_usd: number;
  total_tokens: number;
  avg_latency_ms: number;
  avg_llm_calls: number;
  avg_tool_calls: number;
  avg_confidence: number;
}

interface DailyCosts {
  [date: string]: {
    cost: number;
    tokens: number;
    runs: number;
  };
}

interface RecentRun {
  run_id: string;
  question: string;
  user_id: string;
  start_time: string;
  status: string;
  llm_calls: number;
  cost_usd: number;
  confidence: number;
}

function formatCost(cost: number): string {
  if (cost === 0) return '$0';
  if (cost < 0.01) return `<$0.01`;
  if (cost < 1) return `$${cost.toFixed(2)}`;
  return `$${cost.toFixed(2)}`;
}

function formatTokens(tokens: number): string {
  if (tokens >= 1_000_000) return `${(tokens / 1_000_000).toFixed(1)}M`;
  if (tokens >= 1_000) return `${(tokens / 1_000).toFixed(1)}K`;
  return tokens.toString();
}

function StatCard({ 
  icon, 
  label, 
  value, 
  subValue, 
  color = 'blue' 
}: { 
  icon: React.ReactNode; 
  label: string; 
  value: string; 
  subValue?: string;
  color?: string;
}) {
  const colorClasses: Record<string, string> = {
    blue: 'bg-blue-500/10 text-blue-500',
    green: 'bg-green-500/10 text-green-500',
    purple: 'bg-purple-500/10 text-purple-500',
    orange: 'bg-orange-500/10 text-orange-500',
    pink: 'bg-pink-500/10 text-pink-500',
    cyan: 'bg-cyan-500/10 text-cyan-500',
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg p-4 border border-gray-200 dark:border-gray-700">
      <div className="flex items-center gap-3">
        <div className={`p-2 rounded-lg ${colorClasses[color]}`}>
          {icon}
        </div>
        <div>
          <div className="text-xs text-gray-500 uppercase tracking-wide">{label}</div>
          <div className="text-xl font-semibold text-gray-900 dark:text-white">{value}</div>
          {subValue && <div className="text-xs text-gray-400">{subValue}</div>}
        </div>
      </div>
    </div>
  );
}

export function AgentMetrics() {
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [dailyCosts, setDailyCosts] = useState<DailyCosts>({});
  const [recentRuns, setRecentRuns] = useState<RecentRun[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = async () => {
    setIsLoading(true);
    setError(null);
    
    try {
      const [metricsRes, dailyRes, historyRes] = await Promise.all([
        fetch(`${API_BASE}/api/v2/agent/metrics`),
        fetch(`${API_BASE}/api/v2/agent/costs/daily?days=7`),
        fetch(`${API_BASE}/api/v2/agent/history?limit=10`),
      ]);

      if (metricsRes.ok) {
        setMetrics(await metricsRes.json());
      }
      if (dailyRes.ok) {
        setDailyCosts(await dailyRes.json());
      }
      if (historyRes.ok) {
        setRecentRuns(await historyRes.json());
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch metrics');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 30000); // Refresh every 30s
    return () => clearInterval(interval);
  }, []);

  if (isLoading && !metrics) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="w-6 h-6 animate-spin text-gray-400" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 p-4 rounded-lg">
        {error}
      </div>
    );
  }

  const totalDailyCost = Object.values(dailyCosts).reduce((sum, d) => sum + d.cost, 0);
  const totalDailyRuns = Object.values(dailyCosts).reduce((sum, d) => sum + d.runs, 0);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
          Agent Usage & Costs
        </h2>
        <button
          onClick={fetchData}
          disabled={isLoading}
          className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-500"
        >
          <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
        </button>
      </div>

      {/* Summary Stats */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        <StatCard
          icon={<TrendingUp className="w-5 h-5" />}
          label="Total Runs"
          value={metrics?.total_runs.toString() || '0'}
          color="blue"
        />
        <StatCard
          icon={<DollarSign className="w-5 h-5" />}
          label="Total Cost"
          value={formatCost(metrics?.total_cost_usd || 0)}
          subValue="all time"
          color="green"
        />
        <StatCard
          icon={<Zap className="w-5 h-5" />}
          label="Total Tokens"
          value={formatTokens(metrics?.total_tokens || 0)}
          color="purple"
        />
        <StatCard
          icon={<Clock className="w-5 h-5" />}
          label="Avg Latency"
          value={`${((metrics?.avg_latency_ms || 0) / 1000).toFixed(1)}s`}
          color="orange"
        />
        <StatCard
          icon={<Brain className="w-5 h-5" />}
          label="Avg LLM Calls"
          value={(metrics?.avg_llm_calls || 0).toFixed(1)}
          subValue="per query"
          color="pink"
        />
        <StatCard
          icon={<Database className="w-5 h-5" />}
          label="Avg Confidence"
          value={`${((metrics?.avg_confidence || 0) * 100).toFixed(0)}%`}
          color="cyan"
        />
      </div>

      {/* Daily Costs Chart */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
        <h3 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-4 flex items-center gap-2">
          <BarChart3 className="w-4 h-4" />
          Daily Usage (Last 7 Days)
        </h3>
        
        <div className="flex items-end gap-2 h-32">
          {Object.entries(dailyCosts)
            .sort(([a], [b]) => a.localeCompare(b))
            .slice(-7)
            .map(([date, data]) => {
              const maxCost = Math.max(...Object.values(dailyCosts).map(d => d.cost), 0.01);
              const height = Math.max((data.cost / maxCost) * 100, 5);
              
              return (
                <div key={date} className="flex-1 flex flex-col items-center gap-1">
                  <div className="text-xs text-gray-400">{formatCost(data.cost)}</div>
                  <div 
                    className="w-full bg-blue-500 rounded-t transition-all hover:bg-blue-600"
                    style={{ height: `${height}%` }}
                    title={`${date}: ${data.runs} runs, ${formatTokens(data.tokens)} tokens`}
                  />
                  <div className="text-xs text-gray-500">
                    {new Date(date).toLocaleDateString('en-US', { weekday: 'short' })}
                  </div>
                </div>
              );
            })}
        </div>

        <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700 flex justify-between text-sm">
          <span className="text-gray-500">
            7-Day Total: <span className="font-medium text-gray-700 dark:text-gray-300">{totalDailyRuns} runs</span>
          </span>
          <span className="text-gray-500">
            Cost: <span className="font-medium text-green-600">{formatCost(totalDailyCost)}</span>
          </span>
        </div>
      </div>

      {/* Recent Queries */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700">
          <h3 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Recent Queries
          </h3>
        </div>
        
        <div className="divide-y divide-gray-200 dark:divide-gray-700">
          {recentRuns.length === 0 ? (
            <div className="p-4 text-center text-gray-500 text-sm">
              No queries yet
            </div>
          ) : (
            recentRuns.map((run) => (
              <div key={run.run_id} className="p-4 hover:bg-gray-50 dark:hover:bg-gray-800/50">
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-gray-900 dark:text-white truncate">
                      {run.question}
                    </p>
                    <div className="mt-1 flex items-center gap-3 text-xs text-gray-500">
                      <span>{new Date(run.start_time).toLocaleString()}</span>
                      <span>{run.llm_calls} LLM calls</span>
                      <span className={run.confidence >= 0.8 ? 'text-green-600' : 'text-yellow-600'}>
                        {(run.confidence * 100).toFixed(0)}% confidence
                      </span>
                    </div>
                  </div>
                  <div className="text-right">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                      run.status === 'complete' 
                        ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400'
                        : run.status === 'error'
                        ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400'
                        : 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300'
                    }`}>
                      {run.status}
                    </span>
                    <div className="mt-1 text-xs text-gray-400">
                      {formatCost(run.cost_usd)}
                    </div>
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}

export default AgentMetrics;
