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
        url="http://localhost:8080/mcp",
    )
)

# --- Load Environment Variables ---
load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")
ENDPOINT = os.getenv("OPENAI_API_BASE")

model_config = LiteLlm(
    # 强烈建议使用原生支持多模态的强大模型
    model="openai/google/gemini-2.5-pro", 
    api_base=ENDPOINT,
    api_key=API_KEY
)

# --- 【重要】更新后的系统提示 ---
SYSTEM_PROMPT = (
    "你是一位顶级的生物学家。你的任务是深入、分步地思考，并利用可用工具来解决用户提出的复杂问题。\n"
    "你将以“思考-行动-观察”的循环模式进行工作，直到你收集到足够的信息来给出最终答案。\n\n"
    "**多轮对话策略**:\n"
    "用户的每一个新问题，都代表一个全新的、独立开始的任务。你必须为每个任务独立思考并制定计划。\n"
    "你可以参考上一轮的最终答案作为背景知识，但**绝不能**仅仅因为它看起来相关就直接复用或修改它来回答新问题。\n"
    "你的首要任务是判断：“为了回答当前这个新问题，我是否需要新的信息？” 答案几乎总是“是”。\n"
    "**工作流程**:\n"
    "1. **思考 (Thought)**: 首先，分析用户的问题（以及相关的历史答案），拆解成需要解决的小问题。制定一个初步的计划，思考第一步需要调用哪个工具，以及使用什么参数。\n"
    "2. **行动 (Action)**: 调用合适的的 MCP 工具。\n"
    "3. **观察 (Observation)**: 在你行动后，系统会返回工具执行的结果。\n"
    "4. **重复**: 根据观察到的结果，继续进行思考。判断信息是否足够？是否需要用新的查询词再次搜索？或者信息已经足够，可以总结答案了？重复第1-3步。\n"
    "5. **最终答案 (Final Answer)**: 当你确信已经收集了所有必要信息，并且能够给出一个完整、准确的答案时，将你的最终答案输出。最终答案应该综合所有检索到的信息和你的专业知识，如果需要，请包含代码示例。\n\n"
    "**重要提示**:\n"
    "- 在没有足够信息之前，不要直接给出最终答案。\n"
    "- 每次只执行一个行动。\n"
    "- 动态调整你的检索策略。如果第一次检索结果不理想，要思考如何换一个更好的查询关键词再次检索。\n\n"
    "现在，开始解决用户的问题"
)

react_planner = PlanReActPlanner()

meta_agent = LlmAgent(
    name="MetaAgent",
    model=model_config,
    # model="gemini-2.5-flash",
    instruction=SYSTEM_PROMPT,
    planner=react_planner,
    before_model_callback=read_files_as_text_callback,
    tools=[mcp_toolset]
)

# 当 ADK Web 服务器加载这个文件时，它会自动寻找这个 root_agent 实例
root_agent = meta_agent
