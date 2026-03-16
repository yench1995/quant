import { Card, Row, Col, Statistic } from 'antd'
import { ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons'
import type { BacktestResult } from '../../api/client'

interface Props {
  result: BacktestResult
}

export function MetricsRow({ result }: Props) {
  const totalReturnColor = result.total_return >= 0 ? '#3f8600' : '#cf1322'
  const drawdownColor = '#cf1322'

  return (
    <Row gutter={16}>
      <Col span={4}>
        <Card size="small">
          <Statistic
            title="总收益"
            value={result.total_return}
            precision={2}
            suffix="%"
            valueStyle={{ color: totalReturnColor }}
            prefix={result.total_return >= 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
          />
        </Card>
      </Col>
      <Col span={4}>
        <Card size="small">
          <Statistic
            title="年化收益"
            value={result.annual_return}
            precision={2}
            suffix="%"
            valueStyle={{ color: result.annual_return >= 0 ? '#3f8600' : '#cf1322' }}
          />
        </Card>
      </Col>
      <Col span={4}>
        <Card size="small">
          <Statistic
            title="夏普比率"
            value={result.sharpe_ratio}
            precision={3}
            valueStyle={{ color: result.sharpe_ratio >= 1 ? '#3f8600' : '#666' }}
          />
        </Card>
      </Col>
      <Col span={4}>
        <Card size="small">
          <Statistic
            title="最大回撤"
            value={result.max_drawdown}
            precision={2}
            suffix="%"
            valueStyle={{ color: drawdownColor }}
          />
        </Card>
      </Col>
      <Col span={4}>
        <Card size="small">
          <Statistic
            title="胜率"
            value={result.win_rate}
            precision={1}
            suffix="%"
            valueStyle={{ color: result.win_rate >= 50 ? '#3f8600' : '#cf1322' }}
          />
        </Card>
      </Col>
      <Col span={4}>
        <Card size="small">
          <Statistic
            title="总交易次数"
            value={result.total_trades}
          />
        </Card>
      </Col>
    </Row>
  )
}
