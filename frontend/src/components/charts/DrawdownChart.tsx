import ReactECharts from 'echarts-for-react'
import type { BacktestResult } from '../../api/client'

interface Props {
  result: BacktestResult
}

export function DrawdownChart({ result }: Props) {
  const values = result.equity_curve.map((p) => p.value)
  const dates = result.equity_curve.map((p) => p.date)

  // Calculate drawdown series
  let peak = values[0] || 1
  const drawdowns = values.map((v) => {
    if (v > peak) peak = v
    return peak > 0 ? ((v - peak) / peak) * 100 : 0
  })

  const option = {
    title: { text: '回撤曲线', left: 'center' },
    tooltip: {
      trigger: 'axis',
      formatter: (params: { dataIndex: number }[]) => {
        const i = params[0].dataIndex
        return `${dates[i]}<br/>回撤: ${drawdowns[i].toFixed(2)}%`
      },
    },
    grid: { left: '3%', right: '4%', bottom: '10%', containLabel: true },
    xAxis: {
      type: 'category',
      data: dates,
      axisLabel: { rotate: 30, fontSize: 10 },
    },
    yAxis: { type: 'value', name: '回撤 (%)', max: 0 },
    series: [
      {
        name: '回撤',
        type: 'line',
        data: drawdowns,
        areaStyle: { color: 'rgba(207,19,34,0.3)' },
        lineStyle: { color: '#cf1322' },
        smooth: true,
      },
    ],
  }

  return <ReactECharts option={option} style={{ height: 280 }} />
}
