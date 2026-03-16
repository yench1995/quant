import { Table, Tag, Button } from 'antd'
import type { ColumnsType } from 'antd/es/table'
import type { BacktestRun } from '../../api/client'
import { useNavigate } from 'react-router-dom'

const STATUS_COLOR: Record<string, string> = {
  pending: 'blue',
  running: 'processing',
  completed: 'green',
  failed: 'red',
}

const STATUS_LABEL: Record<string, string> = {
  pending: '等待中',
  running: '运行中',
  completed: '已完成',
  failed: '失败',
}

interface Props {
  runs: BacktestRun[]
  loading?: boolean
}

export function RunsTable({ runs, loading }: Props) {
  const navigate = useNavigate()

  const columns: ColumnsType<BacktestRun> = [
    { title: '策略', dataIndex: 'strategy_id', width: 150 },
    { title: '开始日期', dataIndex: 'start_date', width: 110 },
    { title: '结束日期', dataIndex: 'end_date', width: 110 },
    {
      title: '初始资金',
      dataIndex: 'initial_capital',
      width: 120,
      render: (v: number) => `¥${(v / 10000).toFixed(0)}万`,
    },
    {
      title: '状态',
      dataIndex: 'status',
      width: 100,
      render: (s: string) => <Tag color={STATUS_COLOR[s]}>{STATUS_LABEL[s]}</Tag>,
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      width: 160,
      render: (v: string) => new Date(v).toLocaleString('zh-CN'),
    },
    {
      title: '操作',
      width: 100,
      render: (_, record) => (
        <Button
          type="link"
          size="small"
          disabled={record.status !== 'completed'}
          onClick={() => navigate(`/results/${record.id}`)}
        >
          查看结果
        </Button>
      ),
    },
  ]

  return (
    <Table
      columns={columns}
      dataSource={runs}
      rowKey="id"
      loading={loading}
      size="small"
      pagination={{ pageSize: 20, showSizeChanger: false }}
    />
  )
}
