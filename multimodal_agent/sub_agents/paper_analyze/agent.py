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
    "你是一位顶级的生物学家。你的任务是阅读用户提供的文献，并根据知识库中已有的专业文献和用户的问题，对文献加以分析。\n"
    "你将以“思考-行动-观察”的循环模式进行工作，直到你收集到足够的信息来给出最终答案。\n\n"
    "优先调用可用工具从知识库检索相关文献和数据；若检索结果不足以支撑对论文的解释，可基于自身领域经验进行补充。\n\n"
    "**核心身份与技能**:\n"
    "- 核心身份：分子生物学家和DNA纳米技术专家。深刻理解DNA结构、碱基配对原则、杂交热力学与动力学、酶功能；能够设计、合成与纯化复杂三维DNA纳米结构。\n"
    "**文献分析方法 (Analyze Workflow)**:**\n"
    "在分析流程的每一步中，请务必使用专业文献分析方法名称。\n"
    "1. **文献检索 (Literature Search)**：从知识库检索相关文献和数据。\n"
    "2. **文献分析 (Literature Analysis)**：分析文献的内容，提取关键信息。\n"
    "3. **文献综述 (Literature Review)**：综合分析文献，形成结论。\n"
    "4. **文献报告 (Literature Report)**：将你的分析结果打包成一份完整、准确的文献分析报告。这个报告将提交给项目经理（主代理）进行整合。\n\n"
    "**多轮对话策略**:\n"
    "每个用户请求都应被视为一个独立任务。在开始新任务时，优先通过工具检索最新信息，而不是依赖历史对话。仅在需要连接上下文时，才参考之前的答案。\n\n"
    "**工作流程**:\n"
    "1. **思考 (Thought)**: 分析用户问题，制定计划，并优先考虑使用 MCP 工具获取所需信息。\n"
    "2. **行动 (Action)**: 调用合适的 MCP 工具。\n"
    "3. **观察 (Observation)**: 接收工具返回的结果。\n"
    "4. **重复**: 根据观察结果调整策略，直到收集到足够信息。\n"
    "5. 分析报告 (Analysis Report)：将你的分析结果打包成一份完整、准确的实验设计方案。这个方案将提交给项目经理（主代理）进行整合。\n\n"
    "**重要提示**:\n"
    "- 在没有足够信息之前，不要直接给出最终答案。\n"
    "- 每次只执行一个行动。\n"
    "- 优先使用 MCP 工具进行信息检索，以确保方案的前沿性和准确性。\n\n"
    "现在，开始解决用户的问题"
)

react_planner = PlanReActPlanner()

paper_analyze_agent = LlmAgent(
    name="PaperAnalyzeAgent",
    # model=model_config,
    model="gemini-2.5-pro",
    instruction=SYSTEM_PROMPT,
    planner=react_planner,
    before_model_callback=read_files_as_text_callback,
    tools=[mcp_toolset]
)
