"""
一个专门用于定义“写入记忆”工具的模块。

注意：ADK框架没有提供内置的“写入”工具，因为写入操作通常与Agent的具体业务逻辑紧密相关。
因此，这里的 `save_memory_tool` 是遵循ADK官方推荐的设计模式创建的：
定义一个函数来执行具体操作，然后使用 `FunctionTool.from_function()` 将其包装成一个工具。
"""

from google.adk.tools import ToolContext

async def save_memory_tool(
    key: str, value: str, tool_context: ToolContext
) -> str:
    """
    将信息片段保存到长期记忆中。

    Args:
        key: 信息的唯一标识符。
        value: 要存储的信息内容。
        tool_context: 工具调用上下文，由ADK自动提供。

    Returns:
        一条确认信息。
    """
    print(f"--- [工具执行中] 正在保存记忆: key='{key}' ---")
    # `tool_context.memory_service` 是由自定义的 `VertexMemoryRunner` 注入的实例。
    await tool_context._invocation_context.memory_service.add_session_to_memory(value)
    return f"信息已成功以 '{key}' 为键保存。"
