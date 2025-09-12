import os
import httpx
from ragflow_sdk import RAGFlow
from loguru import logger
from dotenv import load_dotenv
from typing import Dict, Any, List

load_dotenv()

# --- RagFlow API 的配置 ---
RAGFLOW_BASE_URL = os.getenv("RAGFLOW_BASE_URL", "http://localhost:8000")
RAGFLOW_API_KEY = os.getenv("RAGFLOW_API_KEY", "YOUR_API_KEY_HERE")
# 注意：这里的默认值仅用于向后兼容，新的函数实现将强制要求传入 dataset_id
RAGFLOW_DATASET_ID = os.getenv("RAGFLOW_DATASET_ID", "YOUR_DATASET_ID_HERE")

# 初始化 RAGFlow 客户端
rag_flow = RAGFlow(api_key=RAGFLOW_API_KEY, base_url=RAGFLOW_BASE_URL)

def knowledge_retrieval_tool(query: str, dataset_id: str) -> Dict[str, Any]:
    """
    根据用户问题，从指定的RagFlow知识库数据集中检索相关文档。

    Args:
        query (str): 要查询的问题或关键词。
        dataset_id (str): 要查询的RagFlow知识库数据集ID。此参数为必需项。

    Returns:
        一个包含检索结果的字典。
    """
    logger.info(f"--- 🛠️ 执行工具: knowledge_retrieval_tool (query='{query}', dataset_id='{dataset_id}') ---")

    # --- 新增：强制检查 dataset_id ---
    if not dataset_id or dataset_id == "YOUR_DATASET_ID_HERE":
        error_msg = "知识库检索失败：调用 knowledge_retrieval_tool 时未提供有效的 'dataset_id'。"
        logger.error(error_msg)
        return {"status": "error", "error_message": error_msg}

    try:
        # 调用 RAGFlow SDK 进行检索
        chunks = rag_flow.retrieve(
            question=query,
            dataset_ids=[dataset_id]
        )
        if chunks:
            processed_chunks = []
            for r in chunks:
                if hasattr(r, 'text'):
                    processed_chunks.append(r.text)
                elif isinstance(r, dict) and 'content' in r:
                    processed_chunks.append(r['content'])
                else:
                    processed_chunks.append(str(r))
            
            result_content = "\n\n".join(processed_chunks)
            logger.info(f"工具输出: {result_content}")
            return {"status": "success", "summary": result_content}
        else:
            logger.info("工具输出: 在知识库中没有找到相关信息。")
            return {"status": "not_found", "summary": "在知识库中没有找到相关信息。"}
    except httpx.RequestError as e:
        logger.error(f"RagFlow知识库检索失败: {e}")
        return {"status": "error", "error_message": f"知识库检索失败，错误信息: {e}"}

