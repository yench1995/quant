import { useEffect, useState } from 'react'
import { Typography } from 'antd'
import { backtestsApi } from '../api/client'
import type { BacktestRun } from '../api/client'
import { RunsTable } from '../components/tables/RunsTable'

const { Title } = Typography

export function RunHistory() {
  const [runs, setRuns] = useState<BacktestRun[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    setLoading(true)
    backtestsApi.list().then(setRuns).catch(console.error).finally(() => setLoading(false))
  }, [])

  const handleDeleted = (id: string) => {
    setRuns(prev => prev.filter(r => r.id !== id))
  }

  return (
    <div>
      <Title level={3}>历史回测记录</Title>
      <RunsTable runs={runs} loading={loading} onDeleted={handleDeleted} />
    </div>
  )
}
