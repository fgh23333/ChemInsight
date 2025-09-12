import os
import re
import json
import pymysql
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime
from typing import Optional, List

load_dotenv()

# Database connection details
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "123456")

def get_db_connection(db_name=None):
    try:
        return pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        return None

def list_databases() -> str:
    """连接到MySQL服务器并列出所有数据库的名称。当不确定有哪些数据库可用时调用。"""
    logger.info("--- 🛠️ 执行工具: list_databases ---")
    conn = get_db_connection()
    if conn is None:
        return "无法连接到数据库，请检查配置。"
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            databases = [db['Database'] for db in cursor.fetchall()]
            user_databases = [db for db in databases if db not in ['information_schema', 'mysql', 'performance_schema', 'sys']]
            result = json.dumps(user_databases, ensure_ascii=False, indent=2)
            logger.info(f"工具输出: {result}")
            return result
    except Exception as e:
        logger.error(f"列出数据库失败: {e}")
        return f"列出数据库失败，错误信息: {e}"
    finally:
        conn.close()

def get_schema_of_database(db_name: str) -> str:
    """
    获取指定数据库的完整表结构（包括表名、字段名、字段类型、主键、是否可空、默认值和注释）。
    这是理解数据库结构的关键工具，在构建SQL查询前必须调用。
    """
    logger.info(f"--- 🛠️ 执行工具: get_schema_of_database (db_name='{db_name}') ---")
    if not re.match(r'^[a-zA-Z0-9_-]+$', db_name):
        return "无效的数据库名称。"
    conn = get_db_connection(db_name)
    if conn is None:
        return "无法连接到数据库，请检查配置或数据库名称是否正确。"
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = [table[f'Tables_in_{db_name}'] for table in cursor.fetchall()]
            schema_info = {}
            for table_name in tables:
                cursor.execute(f"SHOW FULL COLUMNS FROM `{table_name}`")
                columns = cursor.fetchall()
                table_comment_query = f"SELECT TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = '{table_name}'"
                cursor.execute(table_comment_query)
                table_comment_result = cursor.fetchone()
                table_comment = table_comment_result['TABLE_COMMENT'] if table_comment_result else ""
                schema_info[table_name] = {
                    "columns": [{"field": col['Field'], "type": col['Type'], "null": col['Null'], "key": col['Key'], "default": col['Default'], "extra": col['Extra'], "comment": col['Comment']} for col in columns],
                    "table_comment": table_comment
                }
            result = json.dumps(schema_info, ensure_ascii=False, indent=2)
            logger.info(f"工具输出: {result}")
            return result
    except Exception as e:
        logger.error(f"获取数据库 '{db_name}' 结构失败: {e}")
        return f"获取数据库 '{db_name}' 结构失败，错误信息: {e}"
    finally:
        conn.close()

def run_readonly_query_in_database(db_name: str, query: str) -> str:
    """
    在指定的数据库中执行只读SQL查询。
    允许的查询类型包括 'SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN'。
    禁止执行任何可能修改数据库状态的写操作（如 INSERT, UPDATE, DELETE, CREATE, DROP, ALTER）。
    返回查询结果的JSON字符串。
    """
    logger.info(f"--- 🛠️ 执行工具: run_readonly_query_in_database (db_name='{db_name}', query='{query}') ---")
    if not re.match(r'^[a-zA-Z0-9_-]+$', db_name):
        return "无效的数据库名称。"
    query_upper = query.strip().upper()
    disallowed_keywords = ["INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER", "TRUNCATE", "GRANT", "REVOKE", "LOCK", "UNLOCK"]
    if any(re.search(r'\b' + keyword + r'\b', query_upper) for keyword in disallowed_keywords):
        return "检测到潜在的写操作，已禁止执行。只允许执行不修改数据库的查询。"
    conn = get_db_connection(db_name)
    if conn is None:
        return "无法连接到数据库，请检查配置或数据库名称是否正确。"
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            result_json = json.dumps(result, ensure_ascii=False, indent=2)
            logger.info(f"工具输出: {result_json}")
            return result_json
    except Exception as e:
        logger.error(f"执行查询失败: {e}")
        return f"执行查询失败，错误信息: {e}"
    finally:
        conn.close()

def list_tables_in_database(db_name: str) -> str:
    """
    【后备工具】当get_schema_of_database工具失败时，用于列出指定数据库中的所有表名。
    """
    logger.info(f"--- 🛠️ 执行后备工具: list_tables_in_database (db_name='{db_name}') ---")
    if not re.match(r'^[a-zA-Z0-9_-]+$', db_name):
        return "无效的数据库名称。"
    conn = get_db_connection(db_name)
    if conn is None:
        return "无法连接到数据库，请检查配置或数据库名称是否正确。"
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE `{db_name}`")
            cursor.execute("SHOW TABLES")
            tables = [table[f'Tables_in_{db_name}'] for table in cursor.fetchall()]
            result = json.dumps(tables, ensure_ascii=False, indent=2)
            logger.info(f"工具输出: {result}")
            return result
    except Exception as e:
        logger.error(f"列出数据库 '{db_name}' 中的表失败: {e}")
        return f"列出数据库 '{db_name}' 中的表失败，错误信息: {e}"
    finally:
        conn.close()

def describe_table_in_database(db_name: str, table_name: str) -> str:
    """
    【后备工具】当get_schema_of_database工具失败时，用于获取指定数据库中单个表的详细结构。
    """
    logger.info(f"--- 🛠️ 执行后备工具: describe_table_in_database (db_name='{db_name}', table_name='{table_name}') ---")
    if not re.match(r'^[a-zA-Z0-9_-]+$', db_name) or not re.match(r'^[a-zA-Z0-9_-]+$', table_name):
        return "无效的数据库或表名称。"
    conn = get_db_connection(db_name)
    if conn is None:
        return "无法连接到数据库，请检查配置或数据库名称是否正确。"
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE `{db_name}`")
            cursor.execute(f"SHOW FULL COLUMNS FROM `{table_name}`")
            columns = cursor.fetchall()
            table_comment_query = f"SELECT TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = '{table_name}'"
            cursor.execute(table_comment_query)
            table_comment_result = cursor.fetchone()
            table_comment = table_comment_result['TABLE_COMMENT'] if table_comment_result else ""
            table_info = {
                "table_name": table_name,
                "table_comment": table_comment,
                "columns": [{"field": col['Field'], "type": col['Type'], "null": col['Null'], "key": col['Key'], "default": col['Default'], "extra": col['Extra'], "comment": col['Comment']} for col in columns]
            }
            result = json.dumps(table_info, ensure_ascii=False, indent=2)
            logger.info(f"工具输出: {result}")
            return result
    except Exception as e:
        logger.error(f"获取表 '{table_name}' 结构失败: {e}")
        return f"获取表 '{table_name}' 结构失败，错误信息: {e}"
    finally:
        conn.close()

def build_workstation_analysis_query(
    start_time: str,
    end_time: str,
    analysis_type: str,
    aggregations: Optional[List[str]] = None,
    interval_minutes: Optional[int] = None,
    station_codes: Optional[List[str]] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None
) -> str:
    """
    构建一个统一、多功能且精确的SQL查询，用于分析工作站的占用情况。
    此版本内置了先进的“区间合并”逻辑，可以正确处理重叠的时间段，并修复了JSON序列化问题。

    参数:
    - start_time (str): 分析的开始时间，格式 'YYYY-MM-DD HH:MM:SS'。
    - end_time (str): 分析的结束时间，格式 'YYYY-MM-DD HH:MM:SS'。
    - analysis_type (str): 分析类型，决定查询的模式。有效选项:
        - 'summary_stats': (最常用)生成准确的聚合统计数据（如总时长、次数）。基于合并后的时间段计算。
        - 'time_series': 生成占用率的时间序列数据。基于合并后的时间段计算，结果精确。
        - 'raw_periods': 获取原始的占用时段。此模式不进行合并，用于查看原始数据。
    - aggregations (Optional[List[str]]): 当 analysis_type='summary_stats' 时使用。
        有效选项: ['count', 'total_duration', 'avg_duration', 'max_duration']。
    - interval_minutes (Optional[int]): 当 analysis_type='time_series' 时**必须**提供。定义时间切片的分钟数。
    - station_codes (Optional[List[str]]): 可选，用于筛选一个或多个特定工作站。
    - order_by (Optional[str]): 可选，用于排序。格式: '指标名_asc' 或 '指标名_desc'。
        例如: 'total_duration_desc'。
    - limit (Optional[int]): 可选，限制返回的结果行数。
    """
    logger.info(
        f"--- 🛠️ 执行高级分析工具 (analysis_type='{analysis_type}', start_time='{start_time}', "
        f"end_time='{end_time}', aggregations={aggregations}, interval_minutes={interval_minutes}, "
        f"station_codes={station_codes}, order_by={order_by}, limit={limit}) ---"
    )
    try:
        datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return "无效的时间格式。请使用 'YYYY-MM-DD HH:MM:SS' 格式。"
    if analysis_type not in ['summary_stats', 'time_series', 'raw_periods']:
        return f"错误: 无效的 'analysis_type'。请从 'summary_stats', 'time_series', 'raw_periods' 中选择。"
    
    station_filter_clause = ""
    if station_codes:
        safe_stations = [f"'{station.replace('\'', '')}'" for station in station_codes]
        station_filter_clause = f"AND t.station_code IN ({','.join(safe_stations)})"

    final_query = ""
    if analysis_type == 'summary_stats':
        if not aggregations:
            return "错误: analysis_type='summary_stats' 需要提供 'aggregations' 参数。"
        merging_logic_ctes = f"""
WITH
  raw_periods AS (
    SELECT t.station_code, l.finish_time AS arrival_time, MIN(d.execute_time) AS departure_time
    FROM phoenix_rms.job_history l JOIN phoenix_rms.job_history d ON l.upstream_order_no = d.upstream_order_no AND d.job_template_code = 'DO_MOVE' AND d.execute_time > l.finish_time
    JOIN phoenix_rss.transport_order_qp_job t ON l.upstream_order_no = t.upstream_order_no
    WHERE l.job_template_code = 'QL_MoveLiftUp_Move' AND l.finish_time BETWEEN '{start_time}' AND '{end_time}' AND t.station_code IS NOT NULL {station_filter_clause}
    GROUP BY t.station_code, l.upstream_order_no, l.finish_time
  ),
  period_islands AS (
    SELECT station_code, arrival_time, departure_time,
      CASE WHEN arrival_time > MAX(departure_time) OVER (PARTITION BY station_code ORDER BY arrival_time, departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) THEN 1 ELSE 0 END AS is_island_start
    FROM raw_periods
  ),
  period_with_island_id AS (
    SELECT station_code, arrival_time, departure_time, SUM(is_island_start) OVER (PARTITION BY station_code ORDER BY arrival_time, departure_time) AS island_id
    FROM period_islands
  ),
  merged_periods AS (
    SELECT station_code, MIN(arrival_time) AS start_time, MAX(departure_time) AS end_time
    FROM period_with_island_id GROUP BY station_code, island_id
  ),
  final_durations AS (
    SELECT station_code, TIMESTAMPDIFF(SECOND, start_time, end_time) AS duration_seconds
    FROM merged_periods
  )
"""
        aggregation_map = {
            'count': "COUNT(*) AS merged_periods_count",
            'total_duration': "SUM(duration_seconds) AS total_duration_seconds",
            'avg_duration': "AVG(duration_seconds) AS avg_duration_seconds",
            'max_duration': "MAX(duration_seconds) AS max_duration_seconds"
        }
        select_clauses = ["station_code"] + [aggregation_map[agg] for agg in aggregations if agg in aggregation_map]
        order_by_statement = f"ORDER BY {order_by.replace('_desc', ' DESC').replace('_asc', ' ASC')}" if order_by else ""
        limit_statement = f"LIMIT {limit}" if limit else ""
        final_query = f"""{merging_logic_ctes}
SELECT {', '.join(select_clauses)}
FROM final_durations
GROUP BY station_code
{order_by_statement} {limit_statement};
"""
    elif analysis_type == 'time_series':
        if not interval_minutes or interval_minutes <= 0:
            return "错误: analysis_type='time_series' 需要提供正整数 'interval_minutes' 参数。"
        merging_logic_ctes = f"""
WITH RECURSIVE
  raw_periods AS (
    SELECT t.station_code, l.finish_time AS arrival_time, MIN(d.execute_time) AS departure_time
    FROM phoenix_rms.job_history l JOIN phoenix_rms.job_history d ON l.upstream_order_no = d.upstream_order_no AND d.job_template_code = 'DO_MOVE' AND d.execute_time > l.finish_time
    JOIN phoenix_rss.transport_order_qp_job t ON l.upstream_order_no = t.upstream_order_no
    WHERE l.job_template_code = 'QL_MoveLiftUp_Move' AND l.finish_time BETWEEN '{start_time}' AND '{end_time}' AND t.station_code IS NOT NULL {station_filter_clause}
    GROUP BY t.station_code, l.upstream_order_no, l.finish_time
  ),
  period_islands AS (
    SELECT station_code, arrival_time, departure_time, CASE WHEN arrival_time > MAX(departure_time) OVER (PARTITION BY station_code ORDER BY arrival_time, departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) THEN 1 ELSE 0 END AS is_island_start
    FROM raw_periods
  ),
  period_with_island_id AS (
    SELECT station_code, arrival_time, departure_time, SUM(is_island_start) OVER (PARTITION BY station_code ORDER BY arrival_time, departure_time) AS island_id
    FROM period_islands
  ),
  merged_periods AS (
    SELECT station_code, MIN(arrival_time) AS start_time, MAX(departure_time) AS end_time
    FROM period_with_island_id GROUP BY station_code, island_id
  ),
  time_slices AS (
    SELECT '{start_time}' AS slice_start, TIMESTAMPADD(MINUTE, {interval_minutes}, '{start_time}') AS slice_end
    UNION ALL
    SELECT slice_end, TIMESTAMPADD(MINUTE, {interval_minutes}, slice_end) FROM time_slices WHERE slice_end < '{end_time}'
  )
"""
        final_query = f"""{merging_logic_ctes}
SELECT
  ts.slice_start,
  CAST(
    SUM(
      TIMESTAMPDIFF(
        SECOND,
        GREATEST(mp.start_time, ts.slice_start),
        LEAST(mp.end_time, ts.slice_end)
      )
    ) AS DOUBLE
  ) / ({interval_minutes * 60}) AS occupancy_rate
FROM time_slices ts
JOIN merged_periods mp ON mp.start_time < ts.slice_end AND mp.end_time > ts.slice_start
GROUP BY ts.slice_start
ORDER BY ts.slice_start;
"""
    elif analysis_type == 'raw_periods':
        final_query = f"""
WITH raw_periods AS (
    SELECT
        t.station_code,
        l.finish_time AS arrival_time,
        MIN(d.execute_time) AS departure_time
    FROM phoenix_rms.job_history l
    JOIN phoenix_rms.job_history d ON l.upstream_order_no = d.upstream_order_no AND d.job_template_code = 'DO_MOVE' AND d.execute_time > l.finish_time
    JOIN phoenix_rss.transport_order_qp_job t ON l.upstream_order_no = t.upstream_order_no
    WHERE
        l.job_template_code = 'QL_MoveLiftUp_Move'
        AND l.finish_time BETWEEN '{start_time}' AND '{end_time}'
        AND t.station_code IS NOT NULL
        {station_filter_clause}
    GROUP BY t.station_code, l.upstream_order_no, l.finish_time
)
SELECT
    station_code,
    JSON_ARRAYAGG(JSON_ARRAY(arrival_time, departure_time)) AS occupancy_periods
FROM raw_periods
GROUP BY station_code;
"""
        logger.info(f"工具输出 (生成的最终SQL查询): \n{final_query}")
        return final_query
