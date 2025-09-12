import os

import uvicorn
from fastapi import FastAPI
from google.adk.cli.fast_api import get_fast_api_app

# 获取 main.py 所在的目录
AGENT_DIR = os.path.dirname(os.path.abspath(__file__))
# 示例会话服务 URI (例如: SQLite)
SESSION_SERVICE_URI = "sqlite:///./sessions.db"
# 示例允许的 CORS 来源
ALLOWED_ORIGINS = ["http://localhost", "http://localhost:8080", "*"]
# 如果您打算提供 Web 界面，则设置为 True，否则为 False
SERVE_WEB_INTERFACE = True

# 调用函数获取 FastAPI 应用实例
# 确保 agent 目录名 ('capital_agent') 与您的 agent 文件夹匹配
app: FastAPI = get_fast_api_app(
    agents_dir=AGENT_DIR,
    session_service_uri=SESSION_SERVICE_URI,
    allow_origins=ALLOWED_ORIGINS,
    web=SERVE_WEB_INTERFACE,
)

if __name__ == "__main__":
    # 使用 Cloud Run 提供的 PORT 环境变量，默认为 8080
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))