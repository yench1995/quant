import ReactECharts from 'echarts-for-react'
import type { BacktestResult } from '../../api/client'

interface Props {
  result: BacktestResult
}

export function ReturnsByHolding({ result }: Props) {
  const data = Object.entries(result.holding_analysis)
    .map(([k, v]) => ({ days: parseInt(k), return: v }))
    .sort((a, b) => a.days - b.days)

  if (data.length === 0) {
    return (
      <div style={{ textAlign: 'center', color: '#888', padding: '40px' }}>
        请启用 run_holding_sweep 以查看持仓周期分析
      </div>
    )
  }

  const option = {
    title: { text: '持仓天数收益分析', left: 'center' },
    tooltip: {
      trigger: 'axis',
      formatter: (params: { dataIndex: number; value: number }[]) => {
        return `持仓 ${data[params[0].dataIndex].days} 天<br/>平均收益: ${params[0].value.toFixed(2)}%`
      },
    },
    grid: { left: '3%', right: '4%', bottom: '10%', containLabel: true },
    xAxis: {
      type: 'category',
      data: data.map((d) => `${d.days}天`),
      name: '持仓天数',
    },
    yAxis: { type: 'value', name: '平均收益 (%)' },
    series: [
      {
        name: '平均收益',
        type: 'bar',
        data: data.map((d) => ({
          value: d.return,
          itemStyle: { color: d.return >= 0 ? '#3f8600' : '#cf1322' },
        })),
      },
    ],
  }

  return <ReactECharts option={option} style={{ height: 280 }} />
}
