import ReactECharts from 'echarts-for-react'
import type { BacktestResult } from '../../api/client'

interface Props {
  result: BacktestResult
}

export function EquityCurve({ result }: Props) {
  const option = {
    title: { text: '净值曲线', left: 'center' },
    tooltip: { trigger: 'axis' },
    legend: { data: ['策略净值', '沪深300'], bottom: 0 },
    grid: { left: '3%', right: '4%', bottom: '10%', containLabel: true },
    xAxis: {
      type: 'category',
      data: result.equity_curve.map((p) => p.date),
      axisLabel: { rotate: 30, fontSize: 10 },
    },
    yAxis: { type: 'value', name: '资金 (元)' },
    series: [
      {
        name: '策略净值',
        type: 'line',
        data: result.equity_curve.map((p) => p.value),
        smooth: true,
        lineStyle: { color: '#1890ff', width: 2 },
        areaStyle: { color: 'rgba(24,144,255,0.1)' },
      },
      ...(result.benchmark_curve.length > 0
        ? [
            {
              name: '沪深300',
              type: 'line',
              data: result.benchmark_curve.map((p) => p.value),
              smooth: true,
              lineStyle: { color: '#faad14', width: 1.5, type: 'dashed' },
            },
          ]
        : []),
    ],
  }

  return <ReactECharts option={option} style={{ height: 320 }} />
}
