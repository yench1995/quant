# A股量化回测平台

基于 FastAPI + React 的 A 股量化回测系统，支持本地历史数据库与多策略插件。

## 技术栈

- **后端**：Python 3.11+、FastAPI、SQLAlchemy (async)、AkShare、SQLite
- **前端**：React 18、TypeScript、Vite、Ant Design、ECharts

## 项目结构

```
quant/
├── backend/        # FastAPI 后端
│   ├── app/
│   │   ├── api/        # 路由层
│   │   ├── core/       # 回测引擎
│   │   ├── models/     # 数据库模型
│   │   ├── strategies/ # 策略插件
│   │   └── utils/
│   └── pyproject.toml
├── frontend/       # React 前端
│   ├── src/
│   └── package.json
└── data/           # SQLite 数据库（自动生成）
```

## 启动方式

### 1. 后端

```bash
cd backend

# 安装依赖（推荐用 uv，也可用 pip）
pip install -e .

# 复制环境变量配置
cp .env.example .env

# 启动开发服务器（默认端口 8000）
uvicorn app.main:app --reload
```

API 文档访问：http://localhost:8000/docs

### 2. 前端

```bash
cd frontend

# 安装依赖
npm install

# 启动开发服务器（默认端口 5173）
npm run dev
```

前端访问：http://localhost:5173

> 前端已配置代理，`/api` 请求自动转发到后端 `http://localhost:8000`，无需额外配置跨域。

### 3. 同时启动（推荐）

打开两个终端窗口分别执行上述后端和前端命令。

## 环境变量说明（`backend/.env`）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DATABASE_URL` | `sqlite+aiosqlite:///../../data/finance.db` | 数据库路径，相对于 `backend/` 目录 |
| `CORS_ORIGINS` | `["http://localhost:5173","http://localhost:3000"]` | 允许的前端地址 |

## 添加自定义策略

在 `backend/app/strategies/` 目录下新建 Python 文件，继承 `BaseStrategy` 并实现 `generate_signals` 方法，系统启动时会自动发现并注册。
