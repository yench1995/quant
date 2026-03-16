import { useEffect } from 'react'
import { Card, List, Tag, Typography, Descriptions } from 'antd'
import { useBacktestStore } from '../store/useBacktestStore'
import { strategiesApi } from '../api/client'

const { Title, Text } = Typography

export function StrategyList() {
  const { strategies, setStrategies } = useBacktestStore()

  useEffect(() => {
    strategiesApi.list().then(setStrategies).catch(console.error)
  }, [setStrategies])

  return (
    <div>
      <Title level={3}>策略列表</Title>
      <List
        dataSource={strategies}
        renderItem={(s) => (
          <Card style={{ marginBottom: 16 }} title={<><Tag color="blue">{s.id}</Tag> {s.name}</>}>
            <Text type="secondary">{s.description}</Text>
            <Descriptions size="small" column={3} style={{ marginTop: 12 }}>
              {s.parameters.map((p) => (
                <Descriptions.Item key={p.name} label={p.description || p.name}>
                  <Tag>{p.type}</Tag> 默认: <strong>{String(p.default)}</strong>
                  {p.min_val != null && ` [${p.min_val} - ${p.max_val ?? '∞'}]`}
                </Descriptions.Item>
              ))}
            </Descriptions>
          </Card>
        )}
      />
    </div>
  )
}
