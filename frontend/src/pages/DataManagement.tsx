import { useEffect, useRef, useState } from 'react'
import { Button, Card, Col, DatePicker, Progress, Row, Space, Statistic, Tag, Typography } from 'antd'
import { CheckCircleOutlined, LoadingOutlined, SyncOutlined } from '@ant-design/icons'
import dayjs from 'dayjs'
import api from '../api/client'

const { Title, Text } = Typography

interface LHBCoverage {
  total_dates: number
  earliest: string
  latest: string
  total_records: number
}

interface PriceCoverage {
  total_symbols: number
  total_records: number
  earliest: string
  latest: string
}

interface Coverage {
  lhb: LHBCoverage
  price: PriceCoverage
}

interface PhaseStatus {
  status: string
  months_done?: number
  total_months?: number
  symbols_done?: number
  total_symbols?: number
}

interface SeedStatus {
  running: boolean
  phase: string
  lhb: PhaseStatus
  price: PhaseStatus
  error: string | null
}

const DEFAULT_START = dayjs().subtract(3, 'year').format('YYYY-MM-DD')
const DEFAULT_END = dayjs().format('YYYY-MM-DD')

export function DataManagement() {
  const [coverage, setCoverage] = useState<Coverage | null>(null)
  const [seedStatus, setSeedStatus] = useState<SeedStatus | null>(null)
  const [startDate, setStartDate] = useState(DEFAULT_START)
  const [endDate, setEndDate] = useState(DEFAULT_END)
  const [loading, setLoading] = useState(false)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const fetchCoverage = async () => {
    try {
      const res = await api.get('/data/coverage')
      setCoverage(res.data)
    } catch {
      // ignore
    }
  }

  const fetchStatus = async () => {
    try {
      const res = await api.get('/data/seed-status')
      setSeedStatus(res.data)
      if (!res.data.running && pollRef.current) {
        clearInterval(pollRef.current)
        pollRef.current = null
        fetchCoverage()
      }
    } catch {
      // ignore
    }
  }

  useEffect(() => {
    fetchCoverage()
    fetchStatus()
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
    }
  }, [])

  const startPolling = () => {
    if (pollRef.current) return
    pollRef.current = setInterval(fetchStatus, 2000)
  }

  const handleSeed = async () => {
    setLoading(true)
    try {
      await api.post('/data/seed-all', { start_date: startDate, end_date: endDate })
      await fetchStatus()
      startPolling()
    } finally {
      setLoading(false)
    }
  }

  const isRunning = seedStatus?.running ?? false

  const lhbProgress =
    seedStatus?.lhb?.total_months
      ? Math.round((seedStatus.lhb.months_done! / seedStatus.lhb.total_months) * 100)
      : 0

  const priceProgress =
    seedStatus?.price?.total_symbols
      ? Math.round((seedStatus.price.symbols_done! / seedStatus.price.total_symbols) * 100)
      : 0

  const phaseLabel = (phase: string) => {
    if (phase === 'lhb') return 'Phase 1/2 · 下载龙虎榜数据'
    if (phase === 'price') return 'Phase 2/2 · 下载股价数据'
    if (phase === 'done') return '下载完成'
    if (phase === 'error') return '下载出错'
    return '等待中'
  }

  const statusTag = (s: string) => {
    if (s === 'done') return <Tag icon={<CheckCircleOutlined />} color="success">完成</Tag>
    if (s === 'running') return <Tag icon={<LoadingOutlined />} color="processing">进行中</Tag>
    if (s === 'error') return <Tag color="error">错误</Tag>
    return <Tag color="default">待开始</Tag>
  }

  return (
    <div style={{ maxWidth: 900, margin: '0 auto' }}>
      <Title level={3}>数据管理</Title>

      {/* Coverage stats */}
      <Row gutter={16} style={{ marginBottom: 24 }}>
        <Col span={12}>
          <Card title="龙虎榜数据 (lhb_daily)" size="small">
            <Row gutter={8}>
              <Col span={12}>
                <Statistic title="覆盖天数" value={coverage?.lhb.total_dates ?? '-'} />
              </Col>
              <Col span={12}>
                <Statistic title="总记录数" value={coverage?.lhb.total_records ?? '-'} />
              </Col>
            </Row>
            <Text type="secondary" style={{ fontSize: 12 }}>
              {coverage?.lhb.earliest && `${coverage.lhb.earliest} ~ ${coverage.lhb.latest}`}
            </Text>
          </Card>
        </Col>
        <Col span={12}>
          <Card title="股价数据 (stock_price_daily)" size="small">
            <Row gutter={8}>
              <Col span={8}>
                <Statistic title="股票数" value={coverage?.price.total_symbols ?? '-'} />
              </Col>
              <Col span={8}>
                <Statistic title="总记录数" value={coverage?.price.total_records ?? '-'} />
              </Col>
              <Col span={8}>
                <Statistic title="最新日期" value={coverage?.price.latest || '-'} valueStyle={{ fontSize: 13 }} />
              </Col>
            </Row>
            <Text type="secondary" style={{ fontSize: 12 }}>
              {coverage?.price.earliest && `${coverage.price.earliest} ~ ${coverage.price.latest}`}
            </Text>
          </Card>
        </Col>
      </Row>

      {/* Seed controls */}
      <Card title="下载历史数据（龙虎榜 + 涉及股票价格）" style={{ marginBottom: 24 }}>
        <Space wrap style={{ marginBottom: 16 }}>
          <span>开始日期</span>
          <DatePicker
            defaultValue={dayjs(startDate)}
            onChange={(d) => d && setStartDate(d.format('YYYY-MM-DD'))}
            disabled={isRunning}
          />
          <span>结束日期</span>
          <DatePicker
            defaultValue={dayjs(endDate)}
            onChange={(d) => d && setEndDate(d.format('YYYY-MM-DD'))}
            disabled={isRunning}
          />
          <Button
            type="primary"
            icon={isRunning ? <SyncOutlined spin /> : undefined}
            loading={loading}
            disabled={isRunning}
            onClick={handleSeed}
          >
            {isRunning ? '下载中…' : '全量下载'}
          </Button>
          <Button onClick={fetchCoverage} disabled={isRunning}>刷新统计</Button>
        </Space>

        {/* Progress section */}
        {seedStatus && seedStatus.phase !== 'idle' && (
          <div>
            <div style={{ marginBottom: 12 }}>
              <Text strong>{phaseLabel(seedStatus.phase)}</Text>
            </div>

            {/* LHB phase */}
            <div style={{ marginBottom: 16 }}>
              <Space style={{ marginBottom: 4 }}>
                <Text>龙虎榜数据</Text>
                {statusTag(seedStatus.lhb.status)}
                <Text type="secondary">
                  {seedStatus.lhb.months_done}/{seedStatus.lhb.total_months} 月
                </Text>
              </Space>
              <Progress
                percent={lhbProgress}
                status={seedStatus.lhb.status === 'done' ? 'success' : seedStatus.lhb.status === 'running' ? 'active' : 'normal'}
              />
            </div>

            {/* Price phase */}
            <div>
              <Space style={{ marginBottom: 4 }}>
                <Text>股价数据</Text>
                {statusTag(seedStatus.price.status)}
                <Text type="secondary">
                  {seedStatus.price.symbols_done}/{seedStatus.price.total_symbols} 只
                  {seedStatus.price.total_symbols ? ` · ${priceProgress}%` : ''}
                </Text>
              </Space>
              <Progress
                percent={priceProgress}
                status={seedStatus.price.status === 'done' ? 'success' : seedStatus.price.status === 'running' ? 'active' : 'normal'}
              />
            </div>

            {seedStatus.error && (
              <Text type="danger" style={{ marginTop: 8, display: 'block' }}>
                错误: {seedStatus.error}
              </Text>
            )}
          </div>
        )}
      </Card>
    </div>
  )
}
