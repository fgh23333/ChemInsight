import os

from dotenv import load_dotenv
from logger_config import logger

# --- ADK Framework Imports ---
from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
# 我们依然需要原始的加载工具，但在新的智能工具内部使用它
from tools.file_reader_tool import read_files_as_text_callback
from google.adk.tools.mcp_tool import StreamableHTTPConnectionParams
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset
from google.adk.planners.plan_re_act_planner import PlanReActPlanner

# Your ADK agent connects to the remote MCP service via Streamable HTTP
# mcp_toolset = McpToolset(
#     connection_params=StreamableHTTPConnectionParams(
#         url="http://localhost:8000/mcp",
#     )
# )

# --- Load Environment Variables ---
load_dotenv()
# API_KEY = os.getenv("OPENAI_API_KEY")
# ENDPOINT = os.getenv("OPENAI_API_BASE")

# --- 【重要】更新后的系统提示 ---
SYSTEM_PROMPT = """
你是一个强大的 AI 助理，精通ADK框架和多模态处理。
你的任务是根据用户的需求（可能包含文本、代码和上传的文件），提供专业、准确的回答。
"""

react_planner = PlanReActPlanner()

# --- Agent 实例化 (使用新工具和新提示) ---
# model_config = LiteLlm(
#     # 强烈建议使用原生支持多模态的强大模型
#     model="openai/google/gemini-2.5-pro", 
#     api_base=ENDPOINT,
#     api_key=API_KEY
# )

meta_agent = LlmAgent(
    name="MetaAgent",
    model="gemini-2.5-flash",
    instruction=SYSTEM_PROMPT,
    planner=react_planner,
    # before_model_callback=read_files_as_text_callback,
    # tools=[mcp_toolset]
)

# 当 ADK Web 服务器加载这个文件时，它会自动寻找这个 root_agent 实例
root_agent = meta_agent
