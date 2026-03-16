import { Table, Tag } from 'antd'
import type { ColumnsType } from 'antd/es/table'
import type { Trade } from '../../api/client'

interface Props {
  trades: Trade[]
  total: number
  page: number
  pageSize: number
  onPageChange: (page: number, pageSize: number) => void
  loading?: boolean
}

export function TradeTable({ trades, total, page, pageSize, onPageChange, loading }: Props) {
  const columns: ColumnsType<Trade> = [
    { title: '股票代码', dataIndex: 'symbol', width: 100 },
    { title: '股票名称', dataIndex: 'name', width: 100 },
    { title: '买入日期', dataIndex: 'entry_date', width: 110, sorter: (a, b) => a.entry_date.localeCompare(b.entry_date) },
    { title: '卖出日期', dataIndex: 'exit_date', width: 110 },
    { title: '买入价', dataIndex: 'entry_price', width: 80, render: (v: number) => v.toFixed(2) },
    { title: '卖出价', dataIndex: 'exit_price', width: 80, render: (v: number) => v.toFixed(2) },
    { title: '持股数', dataIndex: 'shares', width: 80 },
    {
      title: '净盈亏 (元)',
      dataIndex: 'net_pnl',
      width: 110,
      sorter: (a, b) => a.net_pnl - b.net_pnl,
      render: (v: number) => (
        <span style={{ color: v >= 0 ? '#3f8600' : '#cf1322' }}>
          {v >= 0 ? '+' : ''}{v.toFixed(0)}
        </span>
      ),
    },
    {
      title: '收益率',
      dataIndex: 'return_pct',
      width: 90,
      sorter: (a, b) => a.return_pct - b.return_pct,
      render: (v: number) => (
        <Tag color={v >= 0 ? 'green' : 'red'}>{v >= 0 ? '+' : ''}{v.toFixed(2)}%</Tag>
      ),
    },
    { title: '持仓天数', dataIndex: 'holding_days', width: 90 },
    {
      title: '机构净买入 (万)',
      dataIndex: 'signal_net_buy',
      width: 130,
      render: (v: number) => v.toFixed(0),
    },
  ]

  return (
    <Table
      columns={columns}
      dataSource={trades}
      rowKey="id"
      loading={loading}
      size="small"
      scroll={{ x: 1000 }}
      pagination={{
        current: page,
        pageSize,
        total,
        showSizeChanger: true,
        showTotal: (t) => `共 ${t} 条交易记录`,
        onChange: onPageChange,
      }}
    />
  )
}
