import { Table, Tag, Button, Popconfirm } from 'antd'
import type { ColumnsType } from 'antd/es/table'
import type { BacktestRun } from '../../api/client'
import { backtestsApi } from '../../api/client'
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
  onDeleted?: (id: string) => void
}

export function RunsTable({ runs, loading, onDeleted }: Props) {
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
      width: 160,
      render: (_, record) => (
        <div style={{ display: 'flex', gap: 4 }}>
          <Button
            type="link"
            size="small"
            disabled={record.status !== 'completed'}
            onClick={() => navigate(`/results/${record.id}`)}
          >
            查看结果
          </Button>
          <Popconfirm
            title="确认删除这条回测记录？"
            okText="删除"
            okButtonProps={{ danger: true }}
            cancelText="取消"
            onConfirm={() =>
              backtestsApi.delete(record.id).then(() => onDeleted?.(record.id))
            }
          >
            <Button type="link" size="small" danger>
              删除
            </Button>
          </Popconfirm>
        </div>
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
