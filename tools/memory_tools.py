"""
一个专门用于定义“写入记忆”工具的模块。

注意：ADK框架没有提供内置的“写入”工具，因为写入操作通常与Agent的具体业务逻辑紧密相关。
因此，这里的 `save_memory_tool` 是遵循ADK官方推荐的设计模式创建的：
定义一个函数来执行具体操作，然后使用 `FunctionTool.from_function()` 将其包装成一个工具。
"""

from google.adk.sessions.session import Session
from google.adk.tools import ToolContext

async def save_memory_tool(
    session: Session, tool_context: ToolContext
) -> str:
    """
    将session保存到长期记忆中。

    Args:
        session: 要存储的会话。
        tool_context: 工具调用上下文，由ADK自动提供。

    Returns:
        一条确认信息。
    """
    await tool_context._invocation_context.memory_service.add_session_to_memory(session)
    return f"{session} 已成功保存。"
