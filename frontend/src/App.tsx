import { Routes, Route, Navigate, Link, useLocation } from 'react-router-dom'
import { Layout, Menu } from 'antd'
import {
  DashboardOutlined,
  DatabaseOutlined,
  ExperimentOutlined,
  PlayCircleOutlined,
  HistoryOutlined,
  SearchOutlined,
} from '@ant-design/icons'
import { Dashboard } from './pages/Dashboard'
import { StrategyList } from './pages/StrategyList'
import { BacktestConfig } from './pages/BacktestConfig'
import { BacktestResult } from './pages/BacktestResult'
import { RunHistory } from './pages/RunHistory'
import { LHBQuery } from './pages/LHBQuery'
import { DataManagement } from './pages/DataManagement'

const { Header, Sider, Content } = Layout

const menuItems = [
  { key: '/', label: <Link to="/">仪表盘</Link>, icon: <DashboardOutlined /> },
  { key: '/lhb', label: <Link to="/lhb">龙虎榜查询</Link>, icon: <SearchOutlined /> },
  { key: '/strategies', label: <Link to="/strategies">策略列表</Link>, icon: <ExperimentOutlined /> },
  { key: '/backtest', label: <Link to="/backtest">配置回测</Link>, icon: <PlayCircleOutlined /> },
  { key: '/history', label: <Link to="/history">历史记录</Link>, icon: <HistoryOutlined /> },
  { key: '/data', label: <Link to="/data">数据管理</Link>, icon: <DatabaseOutlined /> },
]

export default function App() {
  const location = useLocation()
  const selectedKey = location.pathname.startsWith('/results')
    ? '/history'
    : location.pathname || '/'

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider collapsible theme="dark">
        <div style={{ color: '#fff', textAlign: 'center', padding: '16px 8px', fontWeight: 'bold', fontSize: 14 }}>
          A股量化回测
        </div>
        <Menu theme="dark" mode="inline" selectedKeys={[selectedKey]} items={menuItems} />
      </Sider>
      <Layout>
        <Header style={{ background: '#fff', padding: '0 24px', borderBottom: '1px solid #f0f0f0' }}>
          <span style={{ fontSize: 16, fontWeight: 500 }}>A股量化回测平台</span>
        </Header>
        <Content style={{ margin: 24, background: '#fff', padding: 24, minHeight: 280 }}>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/strategies" element={<StrategyList />} />
            <Route path="/backtest" element={<BacktestConfig />} />
            <Route path="/history" element={<RunHistory />} />
            <Route path="/lhb" element={<LHBQuery />} />
            <Route path="/data" element={<DataManagement />} />
            <Route path="/results/:runId" element={<BacktestResult />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Content>
      </Layout>
    </Layout>
  )
}
