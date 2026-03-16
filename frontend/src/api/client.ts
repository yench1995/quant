import axios from 'axios'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000,
})

export interface ParameterSpec {
  name: string
  type: string
  default: unknown
  min_val?: unknown
  max_val?: unknown
  description: string
}

export interface Strategy {
  id: string
  name: string
  description: string
  parameters: ParameterSpec[]
}

export interface BacktestCreateRequest {
  strategy_id: string
  parameters: Record<string, unknown>
  start_date: string
  end_date: string
  initial_capital: number
}

export interface BacktestRun {
  id: string
  strategy_id: string
  status: 'pending' | 'running' | 'completed' | 'failed'
  parameters: Record<string, unknown>
  start_date: string
  end_date: string
  initial_capital: number
  error_message?: string
  created_at: string
  completed_at?: string
}

export interface BacktestResult {
  run_id: string
  total_return: number
  annual_return: number
  sharpe_ratio: number
  max_drawdown: number
  win_rate: number
  total_trades: number
  equity_curve: { date: string; value: number }[]
  benchmark_curve: { date: string; value: number }[]
  holding_analysis: Record<string, number>
}

export interface BacktestDetail {
  run: BacktestRun
  result?: BacktestResult
}

export interface Trade {
  id: number
  run_id: string
  symbol: string
  name: string
  entry_date: string
  exit_date: string
  entry_price: number
  exit_price: number
  shares: number
  gross_pnl: number
  commission: number
  net_pnl: number
  return_pct: number
  holding_days: number
  signal_net_buy: number
}

export interface PaginatedTrades {
  total: number
  page: number
  page_size: number
  items: Trade[]
}

export const strategiesApi = {
  list: () => api.get<Strategy[]>('/strategies').then(r => r.data),
  get: (id: string) => api.get<Strategy>(`/strategies/${id}`).then(r => r.data),
}

export const backtestsApi = {
  create: (req: BacktestCreateRequest) =>
    api.post<BacktestRun>('/backtests', req).then(r => r.data),
  list: () => api.get<BacktestRun[]>('/backtests').then(r => r.data),
  get: (id: string) => api.get<BacktestDetail>(`/backtests/${id}`).then(r => r.data),
}

export const resultsApi = {
  trades: (runId: string, page = 1, pageSize = 20) =>
    api
      .get<PaginatedTrades>(`/results/${runId}/trades`, {
        params: { page, page_size: pageSize },
      })
      .then(r => r.data),
  equityCurve: (runId: string) =>
    api.get(`/results/${runId}/equity-curve`).then(r => r.data),
  holdingAnalysis: (runId: string) =>
    api.get(`/results/${runId}/holding-analysis`).then(r => r.data),
}

export default api
