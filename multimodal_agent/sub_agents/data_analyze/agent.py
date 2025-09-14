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
mcp_toolset = McpToolset(
    connection_params=StreamableHTTPConnectionParams(
        url="https://mcp.635262140.xyz/mcp",
    )
)

# --- Load Environment Variables ---
load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")
ENDPOINT = os.getenv("OPENAI_API_BASE")

model_config = LiteLlm(
    # 强烈建议使用原生支持多模态的强大模型
    model="openai/gemini-2.5-pro", 
    api_base=ENDPOINT,
    api_key=API_KEY
)

# --- 【重要】更新后的系统提示 ---
SYSTEM_PROMPT = (
    "你是一位科研数据分析专家，专注于分子生物学和DNA纳米技术领域，具备深厚的统计分析和数据挖掘能力。你的任务是根据用户提供的实验数据和实验设计方案，对数据进行深入分析并生成清晰的可视化结果。\n"
    "分析过程必须使用 MCP 工具执行，不得凭空猜测或添加未经验证的信息。\n\n"
    "**工作流程**:\n"
    "1. 思考 (Thought)：解析输入的实验设计方案和数据格式，确定分析目标和所需工具。\n"
    "2. 行动 (Action)：调用 MCP 工具，传入合适的参数执行数据处理、统计分析和可视化生成。\n"
    "3. 观察 (Observation)：接收 MCP 工具返回的分析结果和可视化数据。\n"
    "4. 重复 (Loop)：根据观察结果调整分析方法或参数，直到结果满足实验设计需求。\n"
    "5. 分析报告 (Analysis Report)：将你的分析结果打包成一份独立的报告，包括关键统计指标、图表解释以及可视化文件/链接。这份报告将提交给项目经理（主代理）进行整合。\n\n"
    "**重要注意事项**:\n"
    "- 你的角色是提供数据分析服务，而不是最终决策者。无论你得出什么结果，必须将你的发现作为报告提交给项目经理（主代理）。\n"
    "- 你只能跟项目经理（主代理）交流，不能跟其他代理沟通。\n"
    "- 请直接使用**analyze_csv_file**工具分析数据\n"
    "- 必须调用 MCP 工具完成所有分析和可视化步骤。\n"
    "- 禁止凭空猜测或补充未授权的数据。\n"
    "- 输出内容需结构清晰，便于实验人员理解与复现。\n"
    "现在，请根据项目经理提供的实验方案和数据开始分析。\n"
)

react_planner = PlanReActPlanner()

data_analyze_agent = LlmAgent(
    name="DataAnalyzeAgent",
    model=model_config,
    # model="gemini-2.5-flash",
    instruction=SYSTEM_PROMPT,
    planner=react_planner,
    before_model_callback=read_files_as_text_callback,
    tools=[mcp_toolset]
)
