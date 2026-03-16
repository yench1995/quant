import { create } from 'zustand'
import type { BacktestRun, BacktestDetail, Strategy } from '../api/client'

interface BacktestStore {
  strategies: Strategy[]
  setStrategies: (s: Strategy[]) => void
  runs: BacktestRun[]
  setRuns: (r: BacktestRun[]) => void
  addRun: (r: BacktestRun) => void
  currentDetail: BacktestDetail | null
  setCurrentDetail: (d: BacktestDetail | null) => void
}

export const useBacktestStore = create<BacktestStore>((set) => ({
  strategies: [],
  setStrategies: (strategies) => set({ strategies }),
  runs: [],
  setRuns: (runs) => set({ runs }),
  addRun: (run) => set((s) => ({ runs: [run, ...s.runs] })),
  currentDetail: null,
  setCurrentDetail: (currentDetail) => set({ currentDetail }),
}))
