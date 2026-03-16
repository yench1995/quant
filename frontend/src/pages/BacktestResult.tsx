import { useEffect } from 'react'
import { useParams } from 'react-router-dom'
import { Card, Typography, Spin, Alert, Row, Col } from 'antd'
import { useBacktestPoll } from '../hooks/useBacktestPoll'
import { useBacktestStore } from '../store/useBacktestStore'
import { MetricsRow } from '../components/metrics/MetricsRow'
import { EquityCurve } from '../components/charts/EquityCurve'
import { DrawdownChart } from '../components/charts/DrawdownChart'
import { ReturnsByHolding } from '../components/charts/ReturnsByHolding'
import { TradeTable } from '../components/tables/TradeTable'
import { resultsApi } from '../api/client'
import { useState, useCallback } from 'react'
import type { Trade, PaginatedTrades } from '../api/client'

const { Title, Text } = Typography

export function BacktestResult() {
  const { runId } = useParams<{ runId: string }>()
  const { currentDetail, setCurrentDetail } = useBacktestStore()
  const [trades, setTrades] = useState<Trade[]>([])
  const [tradesTotal, setTradesTotal] = useState(0)
  const [tradesPage, setTradesPage] = useState(1)
  const [tradesLoading, setTradesLoading] = useState(false)

  useBacktestPoll(runId ?? null)

  const loadTrades = useCallback(async (page: number, pageSize: number) => {
    if (!runId) return
    setTradesLoading(true)
    try {
      const data: PaginatedTrades = await resultsApi.trades(runId, page, pageSize)
      setTrades(data.items)
      setTradesTotal(data.total)
      setTradesPage(data.page)
    } catch {
      // ignore
    } finally {
      setTradesLoading(false)
    }
  }, [runId])

  useEffect(() => {
    if (currentDetail?.run.status === 'completed') {
      loadTrades(1, 20)
    }
  }, [currentDetail?.run.status, loadTrades])

  // Clear when leaving
  useEffect(() => {
    return () => setCurrentDetail(null)
  }, [setCurrentDetail])

  if (!currentDetail) {
    return <Spin tip="加载中..." size="large" style={{ display: 'block', marginTop: 80 }} />
  }

  const { run, result } = currentDetail

  if (run.status === 'failed') {
    return (
      <Alert
        type="error"
        message="回测失败"
        description={run.error_message}
        style={{ margin: 24 }}
      />
    )
  }

  if (run.status === 'pending' || run.status === 'running') {
    return (
      <Card style={{ textAlign: 'center', margin: 24 }}>
        <Spin tip="回测执行中..." size="large" />
        <div style={{ marginTop: 16 }}>
          <Text type="secondary">策略: {run.strategy_id} | 时间范围: {run.start_date} ~ {run.end_date}</Text>
        </div>
        <div style={{ marginTop: 8 }}>
          <Text type="secondary">状态: {run.status === 'pending' ? '等待中' : '运行中'}</Text>
        </div>
      </Card>
    )
  }

  if (!result) {
    return <Spin tip="加载结果..." size="large" style={{ display: 'block', marginTop: 80 }} />
  }

  return (
    <div>
      <Title level={3}>
        回测结果 — {run.strategy_id} ({run.start_date} ~ {run.end_date})
      </Title>

      <MetricsRow result={result} />

      <Row gutter={16} style={{ marginTop: 16 }}>
        <Col span={14}>
          <Card size="small">
            <EquityCurve result={result} />
          </Card>
        </Col>
        <Col span={10}>
          <Card size="small">
            <DrawdownChart result={result} />
          </Card>
        </Col>
      </Row>

      {Object.keys(result.holding_analysis).length > 0 && (
        <Card size="small" style={{ marginTop: 16 }}>
          <ReturnsByHolding result={result} />
        </Card>
      )}

      <Card size="small" style={{ marginTop: 16 }} title="交易明细">
        <TradeTable
          trades={trades}
          total={tradesTotal}
          page={tradesPage}
          pageSize={20}
          onPageChange={(p, ps) => loadTrades(p, ps)}
          loading={tradesLoading}
        />
      </Card>
    </div>
  )
}
