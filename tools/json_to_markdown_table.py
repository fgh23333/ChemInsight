
import json
from typing import List, Dict, Any

def json_to_markdown_table(json_data: str) -> str:
    """
    将JSON格式的查询结果转换为Markdown表格。

    Args:
        json_data: 包含查询结果的JSON字符串。

    Returns:
        格式化为Markdown表格的字符串。
    """
    try:
        data = json.loads(json_data)
    except json.JSONDecodeError:
        return "Error: 无效的JSON数据。"

    if not data:
        return "数据为空。"

    if not isinstance(data, list) or not all(isinstance(row, dict) for row in data):
        return "Error: JSON数据必须是字典列表。"

    headers = data[0].keys()
    md_table = ["| " + " | ".join(headers) + " |"]
    md_table.append("|" + "---|" * len(headers))

    for row in data:
        md_table.append("| " + " | ".join(str(row.get(h, "")) for h in headers) + " |")

    return "\n".join(md_table)
