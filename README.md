# ChemInsight

ChemInsight 是一个基于 Google Agent Development Kit (ADK) 构建的强大 AI 助手框架。它被设计成一个灵活且可扩展的多模态代理，能够理解和处理文本、代码和文件，以协助完成各种任务。

## ✨ 功能

- **FastAPI 后端**: 提供了一个健壮、快速的 Web 服务器。
- **Google ADK**: 利用 Google 的 Agent Development Kit 来构建复杂的 AI 代理。
- **多模态能力**: 能够处理文本、代码和文件上传。
- **可扩展的工具集**: 通过模型上下文协议 (MCP) 轻松集成新工具。
- **ReAct 规划器**: 利用 Plan-ReAct 规划器进行复杂的推理和任务执行。

## 🚀 开始使用

### 先决条件

- Python 3.12
- Pip (Python 包安装程序)

### 安装步骤

1.  **克隆仓库:**
    ```bash
    git clone https://github.com/fgh23333/ChemInsight.git
    cd ChemInsight
    ```

2.  **创建并激活虚拟环境 (推荐):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # 在 Windows 上, 使用 `venv\Scripts\activate`
    ```

3.  **安装所需的依赖:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **配置环境变量:**
    -   复制环境变量示例文件:
        ```bash
        cp .env.example .env
        ```
    -   打开 `.env` 文件并添加您的 API 凭证。例如，如果您使用 OpenAI:
        ```
        OPENAI_API_KEY="your-openai-api-key"
        OPENAI_API_BASE="https://api.openai.com/v1"
        ```

### 运行应用

安装完成后，您可以使用以下命令启动应用程序：

```bash
adk web
```

应用程序将在 `http://localhost:8080` 上可用。它还提供了一个用于与代理交互的 Web 界面。

## 项目结构

```
.
├── .gitignore
├── Dockerfile
├── logger_config.py
├── main.py
├── requirements.txt
├── multimodal_agent/
│   ├── __init__.py
│   └── agent.py
└── tools/
    └── ... (代理可用的工具)
```

-   `main.py`: FastAPI 应用程序的主入口点。
-   `multimodal_agent/agent.py`: 使用 Google ADK 定义核心 AI 代理逻辑。
-   `tools/`: 包含代理可以用来执行任务的各种工具。
-   `requirements.txt`: 列出 Python 依赖项。
-   `.env.example`: 所需环境变量的模板。
-   `Dockerfile`: 用于将应用程序容器化。
