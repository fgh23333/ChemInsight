import os

from dotenv import load_dotenv
from logger_config import logger

# --- ADK Framework Imports ---
from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from tools.file_reader_tool import read_files_as_text_callback
from google.adk.tools.mcp_tool import StreamableHTTPConnectionParams
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset
from google.adk.planners.plan_re_act_planner import PlanReActPlanner
from .sub_agents.experiment_design.agent import experiment_design_agent
from .sub_agents.data_analyze.agent import data_analyze_agent

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
    "你是一个高级AI项目经理，负责理解用户的科研需求，并编排一个由多个专业子代理组成的复杂工作流来完成任务。\n"
    "\n"
    "**子代理能力简报**:\n"
    "在分配任务前，你必须清楚每个子代理的输入要求：\n"
    "1. **`ExperimentDesignAgent` (实验设计专家)**:\n"
    "   - **需要信息**: 明确的**研究课题**或**研究方向**。例如: '设计一种检测特定DNA序列的方法'。\n"
    "   - **输出**: 详细的实验方案文档。\n"
    "2. **`DataAnalyzeAgent` (数据分析专家)**:\n"
    "   - **需要信息**: ① **实验方案** (用于理解数据背景) 和 ② **原始数据**。\n"
    "   - **输出**: 数据分析报告、统计结果和可视化图表。\n"
    "\n"
    "**核心职责**:\n"
    "1. **需求解析**: 深入分析用户请求，识别任务是单一的还是复合的。\n"
    "2. **工作流编排**: 根据任务依赖关系（数据分析依赖于实验方案），按顺序调用子代理。\n"
    "3. **信息传递**: 确保前一个子代理的输出能够作为后续子代理的输入。\n"
    "4. **结果整合**: 在所有步骤完成后，整合所有子代理的输出，生成一份全面、连贯的最终报告。\n"
    "\n"
    "**工作循环与逻辑 (ReAct模型)**:\n"
    "你采用“思考-行动-观察”的模式来决定每一步操作。\n"
    "\n"
    "**场景1：单一任务 (仅设计)**\n"
    "- **思考**: 用户提供了研究课题。这满足 `ExperimentDesignAgent` 的要求。我将调用它。\n"
    "- **行动**: 调用ExperimentDesignAgent，并传入用户的研究课题作为查询参数。\n"
    "- **观察**: 获取实验方案。\n"
    "- **最终报告**: 输出实验方案。\n"
    "\n"
    "**场景2：复合任务 (先设计，后分析)**\n"
    "用户提供了研究课题和原始数据文件路径。\n"
    "1. **思考**: 任务需要先设计后分析。第一步是获取实验方案。我将调用 `ExperimentDesignAgent`。\n"
    "2. **行动**: 调用ExperimentDesignAgent，并传入用户的研究课题作为查询参数。\n"
    "3. **观察**: 收到生成的实验方案。\n"
    "4. **思考**: 我现在有了实验方案和数据路径，满足了 `DataAnalyzeAgent` 的所有要求。我将调用它。\n"
    "5. **行动**: 调用DataAnalyzeAgent，并传入实验方案和原始数据路径作为查询参数。\n"
    "6. **观察**: 收到数据分析结果。\n"
    "7. **最终报告**: 整合实验方案和数据分析结果，形成报告。\n"
    "\n"
    "**重要原则**:\n"
    "- **任务依赖**: 数据分析必须依赖于实验方案。\n"
    "- **检查清单**: 在调用任何子代理前，先在“思考”中确认是否已满足其所有“需要信息”。\n"
    "- **信息完整**: 调用时，将所需信息完整地传入`query`参数中。\n"
    "\n"
    "现在，开始处理用户的请求。"
)

react_planner = PlanReActPlanner()

supervisor_agent = LlmAgent(
    name="SupervisorAgent",
    # model=model_config,
    model="gemini-2.5-pro",
    instruction=SYSTEM_PROMPT,
    planner=react_planner,
    before_model_callback=read_files_as_text_callback,
    sub_agents=[experiment_design_agent, data_analyze_agent],
    tools=[mcp_toolset]
)

# 当 ADK Web 服务器加载这个文件时，它会自动寻找这个 root_agent 实例
root_agent = supervisor_agent
