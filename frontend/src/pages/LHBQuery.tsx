import { useState } from 'react'
import {
  Card, DatePicker, InputNumber, Button, Table, Tag, Typography, Space,
  Statistic, Row, Col, Switch, Spin, Tabs
} from 'antd'
import { SearchOutlined, BankOutlined } from '@ant-design/icons'
import type { ColumnsType, ExpandableConfig } from 'antd/es/table'
import dayjs from 'dayjs'
import api from '../api/client'

const { Title } = Typography

interface LHBRecord {
  symbol: string
  name: string
  change_pct: number
  buy_amount_wan: number
  sell_amount_wan: number
  net_buy_wan: number
  buy_inst_count: number
  sell_inst_count: number
}

interface SeatRecord {
  seat_name: string
  buy_amount_wan: number
  sell_amount_wan: number
  net_amount_wan: number
  is_institution: boolean
}

interface SeatDetail {
  buy: SeatRecord[]
  sell: SeatRecord[]
}

interface LHBResponse {
  date: string
  total: number
  data: LHBRecord[]
}

const seatColumns: ColumnsType<SeatRecord> = [
  {
    title: '席位名称',
    dataIndex: 'seat_name',
    render: (v: string, row: SeatRecord) => (
      <Space>
        {row.is_institution && <BankOutlined style={{ color: '#1677ff' }} />}
        <span style={{ fontWeight: row.is_institution ? 600 : 400 }}>{v}</span>
        {row.is_institution && <Tag color="blue" style={{ fontSize: 11 }}>机构</Tag>}
      </Space>
    ),
  },
  {
    title: '买入（万元）',
    dataIndex: 'buy_amount_wan',
    width: 120,
    render: (v: number) => v > 0
      ? <span style={{ color: '#3f8600' }}>{v.toLocaleString('zh-CN', { maximumFractionDigits: 0 })}</span>
      : <span style={{ color: '#ccc' }}>—</span>,
  },
  {
    title: '卖出（万元）',
    dataIndex: 'sell_amount_wan',
    width: 120,
    render: (v: number) => v > 0
      ? <span style={{ color: '#cf1322' }}>{v.toLocaleString('zh-CN', { maximumFractionDigits: 0 })}</span>
      : <span style={{ color: '#ccc' }}>—</span>,
  },
  {
    title: '净额（万元）',
    dataIndex: 'net_amount_wan',
    width: 130,
    render: (v: number) => (
      <Tag color={v > 0 ? 'green' : v < 0 ? 'red' : 'default'}>
        {v > 0 ? '+' : ''}{v.toLocaleString('zh-CN', { maximumFractionDigits: 0 })}
      </Tag>
    ),
  },
]

function SeatDetailPanel({ symbol, date }: { symbol: string; date: string }) {
  const [detail, setDetail] = useState<SeatDetail | null>(null)
  const [loading, setLoading] = useState(false)
  const [loaded, setLoaded] = useState(false)

  if (!loaded && !loading) {
    setLoading(true)
    api.get<SeatDetail & { symbol: string; date: string }>('/market-data/lhb-seat-detail', {
      params: { symbol, date },
    }).then(res => {
      setDetail({ buy: res.data.buy, sell: res.data.sell })
    }).catch(() => {
      setDetail({ buy: [], sell: [] })
    }).finally(() => {
      setLoading(false)
      setLoaded(true)
    })
  }

  if (loading) return <div style={{ padding: '12px 24px' }}><Spin size="small" /> 加载席位数据...</div>
  if (!detail) return null

  const items = [
    {
      key: 'buy',
      label: `买入席位 (${detail.buy.length})`,
      children: (
        <Table
          columns={seatColumns}
          dataSource={detail.buy}
          rowKey="seat_name"
          size="small"
          pagination={false}
          rowClassName={(r: SeatRecord) => r.is_institution ? 'institution-row' : ''}
        />
      ),
    },
    {
      key: 'sell',
      label: `卖出席位 (${detail.sell.length})`,
      children: (
        <Table
          columns={seatColumns}
          dataSource={detail.sell}
          rowKey="seat_name"
          size="small"
          pagination={false}
          rowClassName={(r: SeatRecord) => r.is_institution ? 'institution-row' : ''}
        />
      ),
    },
  ]

  return (
    <div style={{ padding: '8px 24px 16px', background: '#fafafa' }}>
      <Tabs size="small" items={items} />
    </div>
  )
}

const mainColumns: ColumnsType<LHBRecord> = [
  {
    title: '排名',
    width: 60,
    render: (_: unknown, __: unknown, index: number) => index + 1,
  },
  { title: '代码', dataIndex: 'symbol', width: 90 },
  { title: '名称', dataIndex: 'name', width: 100 },
  {
    title: '当日涨跌幅',
    dataIndex: 'change_pct',
    width: 110,
    sorter: (a, b) => a.change_pct - b.change_pct,
    render: (v: number) => (
      <Tag color={v < 0 ? 'red' : v > 0 ? 'green' : 'default'}>
        {v > 0 ? '+' : ''}{v.toFixed(2)}%
      </Tag>
    ),
  },
  {
    title: '机构买入（万元）',
    dataIndex: 'buy_amount_wan',
    width: 140,
    sorter: (a, b) => a.buy_amount_wan - b.buy_amount_wan,
    render: (v: number) => <span style={{ color: '#3f8600' }}>{v.toLocaleString('zh-CN', { maximumFractionDigits: 0 })}</span>,
  },
  {
    title: '机构卖出（万元）',
    dataIndex: 'sell_amount_wan',
    width: 140,
    sorter: (a, b) => a.sell_amount_wan - b.sell_amount_wan,
    render: (v: number) => (
      <span style={{ color: v > 0 ? '#cf1322' : '#999' }}>
        {v.toLocaleString('zh-CN', { maximumFractionDigits: 0 })}
      </span>
    ),
  },
  {
    title: '机构净买入（万元）',
    dataIndex: 'net_buy_wan',
    width: 150,
    defaultSortOrder: 'descend',
    sorter: (a, b) => a.net_buy_wan - b.net_buy_wan,
    render: (v: number) => (
      <Tag color="green" style={{ fontSize: 13, fontWeight: 600 }}>
        +{v.toLocaleString('zh-CN', { maximumFractionDigits: 0 })}
      </Tag>
    ),
  },
  {
    title: '买方机构数',
    dataIndex: 'buy_inst_count',
    width: 110,
    sorter: (a, b) => a.buy_inst_count - b.buy_inst_count,
    render: (v: number) => <Tag color="blue">{v} 家</Tag>,
  },
  {
    title: '卖方机构数',
    dataIndex: 'sell_inst_count',
    width: 110,
    sorter: (a, b) => a.sell_inst_count - b.sell_inst_count,
    render: (v: number) => v > 0 ? <Tag color="orange">{v} 家</Tag> : <Tag>0 家</Tag>,
  },
]

export function LHBQuery() {
  const [date, setDate] = useState<dayjs.Dayjs>(dayjs().subtract(1, 'day'))
  const [minNetBuy, setMinNetBuy] = useState<number>(0)
  const [onlyDown, setOnlyDown] = useState<boolean>(false)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<LHBResponse | null>(null)
  const [expandedKeys, setExpandedKeys] = useState<string[]>([])

  const handleQuery = async () => {
    setLoading(true)
    setExpandedKeys([])
    try {
      const res = await api.get<LHBResponse>('/market-data/lhb-institutions', {
        params: { date: date.format('YYYY-MM-DD'), min_net_buy_wan: minNetBuy, only_down: onlyDown },
      })
      setResult(res.data)
    } catch {
      setResult({ date: date.format('YYYY-MM-DD'), total: 0, data: [] })
    } finally {
      setLoading(false)
    }
  }

  const totalNetBuy = result?.data.reduce((s, r) => s + r.net_buy_wan, 0) ?? 0
  const totalBuy = result?.data.reduce((s, r) => s + r.buy_amount_wan, 0) ?? 0

  const expandable: ExpandableConfig<LHBRecord> = {
    expandedRowKeys: expandedKeys,
    onExpandedRowsChange: (keys) => setExpandedKeys(keys as string[]),
    expandedRowRender: (record) => (
      <SeatDetailPanel symbol={record.symbol} date={result!.date} />
    ),
    rowExpandable: () => true,
  }

  return (
    <div>
      <Title level={3}>龙虎榜机构净买入查询</Title>

      <Card style={{ marginBottom: 16 }}>
        <Space size="large" wrap>
          <Space>
            <span>查询日期：</span>
            <DatePicker
              value={date}
              onChange={(d) => d && setDate(d)}
              disabledDate={(d) => d.isAfter(dayjs())}
              allowClear={false}
            />
          </Space>
          <Space>
            <span>净买入下限（万元）：</span>
            <InputNumber
              min={0}
              value={minNetBuy}
              onChange={(v) => setMinNetBuy(v ?? 0)}
              style={{ width: 120 }}
              placeholder="0 = 全部"
            />
          </Space>
          <Space>
            <span>仅显示当天下跌：</span>
            <Switch checked={onlyDown} onChange={setOnlyDown} />
          </Space>
          <Button type="primary" icon={<SearchOutlined />} loading={loading} onClick={handleQuery}>
            查询
          </Button>
        </Space>
      </Card>

      {result && (
        <>
          <Row gutter={16} style={{ marginBottom: 16 }}>
            <Col span={6}>
              <Card size="small"><Statistic title="查询日期" value={result.date} /></Card>
            </Col>
            <Col span={6}>
              <Card size="small"><Statistic title="符合条件个股数" value={result.total} suffix="只" /></Card>
            </Col>
            <Col span={6}>
              <Card size="small">
                <Statistic title="合计机构净买入" value={totalNetBuy.toFixed(0)} suffix="万元" valueStyle={{ color: '#3f8600' }} />
              </Card>
            </Col>
            <Col span={6}>
              <Card size="small">
                <Statistic title="合计机构买入" value={totalBuy.toFixed(0)} suffix="万元" />
              </Card>
            </Col>
          </Row>

          <Card extra={<span style={{ color: '#888', fontSize: 12 }}>点击行左侧箭头查看具体买卖席位</span>}>
            <Table
              columns={mainColumns}
              dataSource={result.data}
              rowKey="symbol"
              loading={loading}
              size="middle"
              expandable={expandable}
              pagination={{ pageSize: 50, showSizeChanger: false, showTotal: (t) => `共 ${t} 条` }}
              locale={{ emptyText: result.total === 0 ? '该日期暂无符合条件数据（可能为非交易日或节假日）' : '无数据' }}
            />
          </Card>
        </>
      )}
    </div>
  )
}
