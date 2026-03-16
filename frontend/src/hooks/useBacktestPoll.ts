import { useEffect, useRef, useCallback } from 'react'
import { backtestsApi } from '../api/client'
import { useBacktestStore } from '../store/useBacktestStore'

export function useBacktestPoll(runId: string | null) {
  const setCurrentDetail = useBacktestStore((s) => s.setCurrentDetail)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const stop = useCallback(() => {
    if (timerRef.current) {
      clearTimeout(timerRef.current)
      timerRef.current = null
    }
  }, [])

  useEffect(() => {
    if (!runId) return
    let cancelled = false

    const poll = async () => {
      try {
        const detail = await backtestsApi.get(runId)
        if (!cancelled) {
          setCurrentDetail(detail)
          if (detail.run.status === 'completed' || detail.run.status === 'failed') {
            return
          }
          timerRef.current = setTimeout(poll, 2000)
        }
      } catch {
        if (!cancelled) {
          timerRef.current = setTimeout(poll, 3000)
        }
      }
    }

    poll()

    return () => {
      cancelled = true
      stop()
    }
  }, [runId, setCurrentDetail, stop])

  return { stop }
}
