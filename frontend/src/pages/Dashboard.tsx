import { useEffect, useState } from 'react'
import { Card, Row, Col, Statistic, Typography, List, Tag, Button } from 'antd'
import { useNavigate } from 'react-router-dom'
import { backtestsApi, strategiesApi } from '../api/client'
import type { BacktestRun, Strategy } from '../api/client'

const { Title } = Typography

const STATUS_COLOR: Record<string, string> = {
  pending: 'blue', running: 'processing', completed: 'green', failed: 'red'
}

export function Dashboard() {
  const [runs, setRuns] = useState<BacktestRun[]>([])
  const [strategies, setStrategies] = useState<Strategy[]>([])
  const navigate = useNavigate()

  useEffect(() => {
    backtestsApi.list().then(setRuns).catch(console.error)
    strategiesApi.list().then(setStrategies).catch(console.error)
  }, [])

  const completed = runs.filter((r) => r.status === 'completed').length
  const running = runs.filter((r) => r.status === 'running' || r.status === 'pending').length
  const failed = runs.filter((r) => r.status === 'failed').length

  return (
    <div>
      <Title level={3}>仪表盘</Title>
      <Row gutter={16} style={{ marginBottom: 24 }}>
        <Col span={6}>
          <Card><Statistic title="策略总数" value={strategies.length} /></Card>
        </Col>
        <Col span={6}>
          <Card><Statistic title="回测总数" value={runs.length} /></Card>
        </Col>
        <Col span={6}>
          <Card><Statistic title="已完成" value={completed} valueStyle={{ color: '#3f8600' }} /></Card>
        </Col>
        <Col span={6}>
          <Card><Statistic title="进行中/失败" value={`${running}/${failed}`} /></Card>
        </Col>
      </Row>
      <Card
        title="最近回测记录"
        extra={<Button type="link" onClick={() => navigate('/history')}>查看全部</Button>}
      >
        <List
          dataSource={runs.slice(0, 5)}
          renderItem={(r) => (
            <List.Item
              actions={[
                r.status === 'completed' ? (
                  <Button type="link" size="small" onClick={() => navigate(`/results/${r.id}`)}>
                    查看结果
                  </Button>
                ) : null,
              ]}
            >
              <List.Item.Meta
                title={<>{r.strategy_id} <Tag color={STATUS_COLOR[r.status]}>{r.status}</Tag></>}
                description={`${r.start_date} ~ ${r.end_date} | 初始资金: ¥${(r.initial_capital / 10000).toFixed(0)}万`}
              />
            </List.Item>
          )}
        />
      </Card>
    </div>
  )
}
