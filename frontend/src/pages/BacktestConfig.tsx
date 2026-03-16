import { useState, useEffect } from 'react'
import {
  Card, Form, Select, InputNumber, DatePicker, Button, Switch, message, Typography, Space
} from 'antd'
import { useNavigate } from 'react-router-dom'
import dayjs from 'dayjs'
import { strategiesApi, backtestsApi } from '../api/client'
import type { Strategy } from '../api/client'
import { useBacktestStore } from '../store/useBacktestStore'

const { Title } = Typography

export function BacktestConfig() {
  const [strategies, setStrategies] = useState<Strategy[]>([])
  const [selectedStrategy, setSelectedStrategy] = useState<Strategy | null>(null)
  const [loading, setLoading] = useState(false)
  const [form] = Form.useForm()
  const navigate = useNavigate()
  const addRun = useBacktestStore((s) => s.addRun)

  useEffect(() => {
    strategiesApi.list().then(setStrategies).catch(console.error)
  }, [])

  const onStrategyChange = async (id: string) => {
    const s = await strategiesApi.get(id)
    setSelectedStrategy(s)
    // Set default parameter values
    const defaults: Record<string, unknown> = {}
    s.parameters.forEach((p) => { defaults[p.name] = p.default })
    form.setFieldsValue({ parameters: defaults })
  }

  const onFinish = async (values: {
    strategy_id: string
    dateRange: [dayjs.Dayjs, dayjs.Dayjs]
    initial_capital: number
    parameters: Record<string, unknown>
  }) => {
    setLoading(true)
    try {
      const run = await backtestsApi.create({
        strategy_id: values.strategy_id,
        parameters: values.parameters || {},
        start_date: values.dateRange[0].format('YYYY-MM-DD'),
        end_date: values.dateRange[1].format('YYYY-MM-DD'),
        initial_capital: values.initial_capital,
      })
      addRun(run)
      message.success('回测已提交，正在执行...')
      navigate(`/results/${run.id}`)
    } catch (e) {
      message.error('提交失败')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div>
      <Title level={3}>配置回测</Title>
      <Card>
        <Form
          form={form}
          layout="vertical"
          onFinish={onFinish}
          initialValues={{
            initial_capital: 1000000,
            dateRange: [dayjs('2023-01-01'), dayjs('2024-12-31')],
          }}
        >
          <Form.Item name="strategy_id" label="选择策略" rules={[{ required: true }]}>
            <Select
              placeholder="请选择策略"
              onChange={onStrategyChange}
              options={strategies.map((s) => ({ value: s.id, label: s.name }))}
            />
          </Form.Item>

          <Form.Item name="dateRange" label="回测时间范围" rules={[{ required: true }]}>
            <DatePicker.RangePicker style={{ width: '100%' }} />
          </Form.Item>

          <Form.Item name="initial_capital" label="初始资金 (元)" rules={[{ required: true }]}>
            <InputNumber min={10000} step={100000} style={{ width: '100%' }} formatter={(v) => `¥ ${v}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')} />
          </Form.Item>

          {selectedStrategy && selectedStrategy.parameters.length > 0 && (
            <Card size="small" title="策略参数" style={{ marginBottom: 16 }}>
              {selectedStrategy.parameters.map((p) => (
                <Form.Item
                  key={p.name}
                  name={['parameters', p.name]}
                  label={p.description || p.name}
                  initialValue={p.default}
                >
                  {p.type === 'bool' ? (
                    <Switch defaultChecked={Boolean(p.default)} />
                  ) : p.type === 'int' ? (
                    <InputNumber
                      min={p.min_val as number | undefined}
                      max={p.max_val as number | undefined}
                      style={{ width: '100%' }}
                    />
                  ) : p.type === 'float' ? (
                    <InputNumber
                      min={p.min_val as number | undefined}
                      max={p.max_val as number | undefined}
                      step={0.01}
                      style={{ width: '100%' }}
                    />
                  ) : (
                    <Select />
                  )}
                </Form.Item>
              ))}
            </Card>
          )}

          <Form.Item>
            <Space>
              <Button type="primary" htmlType="submit" loading={loading}>
                提交回测
              </Button>
              <Button onClick={() => form.resetFields()}>重置</Button>
            </Space>
          </Form.Item>
        </Form>
      </Card>
    </div>
  )
}
