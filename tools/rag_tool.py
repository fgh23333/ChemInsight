from ragflow_sdk import RAGFlow
from typing import List, Dict
import httpx
import io
import time
from datetime import datetime

@mcp.tool()
async def list_knowledge_bases(api_key: str, base_url: str, page: int = 1, page_size: int = 30, orderby: str = "create_time", desc: bool = True, id: str = "", name: str = "") -> List[dict]:
    """
    Lists all knowledge bases (datasets).

    Args:
        api_key: RAGFlow API key.
        base_url: RAGFlow instance base URL.
        page: Specifies the page on which the datasets will be displayed. Defaults to 1.
        page_size: The number of datasets on each page. Defaults to 30.
        orderby: The field by which datasets should be sorted. Defaults to "create_time".
        desc: Indicates whether the retrieved datasets should be sorted in descending order. Defaults to True.
        id: The ID of the dataset to retrieve. Defaults to an empty string.
        name: The name of the dataset to retrieve. Defaults to an empty string.

    Returns:
        A list of dataset objects.
    """
    id = None if not id else id
    name = None if not name else name

    try:
        rag_flow = RAGFlow(api_key=api_key, base_url=base_url)
        datasets = rag_flow.list_datasets(page=page, page_size=page_size, orderby=orderby, desc=desc, id=id, name=name)
        return [
            {
                "id": ds.id,
                "name": ds.name,
                "document_count": ds.document_count,
                "chunk_count": ds.chunk_count,
                "embedding_model": ds.embedding_model,
                "permission": ds.permission,
                "description": ds.description,
                "avatar": ds.avatar,
            }
            for ds in datasets
        ]
    except Exception as e:
        logger.error(f"Error retrieving datasets: {e}")
        return [{"error": str(e)}]

@mcp.tool()
async def search_knowledge_base(api_key: str, base_url: str, question: str, dataset_ids: List[str], document_ids: List[str] = None, page: int = 1, page_size: int = 10, similarity_threshold: float = 0.2, vector_similarity_weight: float = 0.3, top_k: int = 1024, rerank_id: str = "", keyword: bool = False) -> List[dict]:
    """
    Retrieves chunks from the specified knowledge base.

    Args:
        api_key: RAGFlow API key.
        base_url: RAGFlow instance base URL.
        question: The user query or query keywords.
        dataset_ids: The IDs of the datasets to search.
        document_ids: The IDs of the documents to search.
        page: The starting index for the documents to retrieve. Defaults to 1.
        page_size: The maximum number of chunks to retrieve. Defaults to 10.
        similarity_threshold: The minimum similarity score. Defaults to 0.2.
        vector_similarity_weight: The weight of vector cosine similarity. Defaults to 0.3.
        top_k: The number of chunks engaged in vector cosine computation. Defaults to 1024.
        rerank_id: The ID of the rerank model. Defaults to an empty string.
        keyword: Indicates whether to enable keyword-based matching. Defaults to False.

    Returns:
        A list of Chunk objects representing the document chunks.
    """
    rerank_id = None if not rerank_id else rerank_id
    
    try:
        rag_flow = RAGFlow(api_key=api_key, base_url=base_url)
        chunks = rag_flow.retrieve(
            question=question,
            dataset_ids=dataset_ids,
            document_ids=document_ids,
            page=page,
            page_size=page_size,
            similarity_threshold=similarity_threshold,
            vector_similarity_weight=vector_similarity_weight,
            top_k=top_k,
            rerank_id=rerank_id,
            keyword=keyword
        )
        return [
            {
                "available": chunk.available,
                "content": chunk.content,
                "create_time": chunk.create_time,
                "create_timestamp": chunk.create_timestamp,
                "dataset_id": chunk.dataset_id,
                "document_id": chunk.document_id,
                "document_name": chunk.document_name,
                "id": chunk.id,
                "important_keywords": chunk.important_keywords,
                "questions": chunk.questions,
            }
            for chunk in chunks
        ]
    except Exception as e:
        logger.error(f"Error retrieving chunks: {e}")
        return [{"error": str(e)}]

@mcp.tool()
async def list_documents(
    api_key: str,
    base_url: str,
    dataset_id: str,
    page: int = 1,
    page_size: int = 30
) -> Dict:
    """
    Lists documents within a specific dataset.

    Args:
        api_key: RAGFlow API key.
        base_url: RAGFlow instance base URL.
        dataset_id: The ID of the dataset.
        page: The page number for pagination. Defaults to 1.
        page_size: The number of documents per page. Defaults to 30.
        orderby: The field to order the results by. Defaults to "create_time".
        desc: Whether to sort in descending order. Defaults to True.
        keywords: Keywords to search for in document names. Defaults to None.
        id: The specific ID of the document to retrieve. Defaults to None.
        name: The name of the document to retrieve. Defaults to None.

    Returns:
        A dictionary containing the list of documents and pagination details.
    """
    try:
        if base_url.endswith('/'):
            base_url = base_url[:-1]
        url = f"{base_url}/api/v1/datasets/{dataset_id}/documents"
        headers = {"Authorization": f"Bearer {api_key}"}
        params = {
            "page": page,
            "page_size": page_size
        }
        # Filter out None values from params
        params = {k: v for k, v in params.items() if v is not None}

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("data", {})
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error listing documents: {e.response.text}")
        return {"error": f"HTTP error: {e.response.status_code} - {e.response.text}"}
    except Exception as e:
        logger.error(f"An unexpected error occurred while listing documents: {e}")
        return {"error": str(e)}

@mcp.tool()
async def get_document_by_id(api_key: str, base_url: str, dataset_id: str, document_id: str) -> Dict:
    """
    Retrieves a single document by its ID from a specific dataset by paginating through all documents.

    Args:
        api_key: RAGFlow API key.
        base_url: RAGFlow instance base URL.
        dataset_id: The ID of the dataset containing the document.
        document_id: The ID of the document to retrieve.

    Returns:
        A dictionary containing the document's details, or an error dictionary if not found.
    """
    page = 1
    page_size = 100  # Fetch 100 documents at a time
    while True:
        try:
            docs_data = await list_documents(
                api_key=api_key,
                base_url=base_url,
                dataset_id=dataset_id,
                page=page,
                page_size=page_size,
            )
            if "error" in docs_data:
                return docs_data

            documents = docs_data.get("docs", [])
            for doc in documents:
                if doc.get("id") == document_id:
                    return doc

            if len(documents) < page_size:
                # Reached the last page
                break
            
            page += 1

        except Exception as e:
            logger.error(f"An error occurred while paginating through documents: {e}")
            return {"error": str(e)}

    return {"error": f"Document with ID '{document_id}' not found in dataset '{dataset_id}'."}

@mcp.tool()
async def upload_files_to_knowledge_base(api_key: str, base_url: str, dataset_id: str, file_paths: List[str]) -> Dict:
    """
    Uploads multiple files to a specified knowledge base (dataset) from local paths or URLs.

    Args:
        api_key: RAGFlow API key.
        base_url: RAGFlow instance base URL.
        dataset_id: The ID of the dataset to upload files to.
        file_paths: A list of local file paths or URLs to upload.

    Returns:
        A dictionary with a list of uploaded document details or an error message.
    """
    try:
        rag_flow = RAGFlow(api_key=api_key, base_url=base_url)
        
        # Get the dataset object
        datasets = rag_flow.list_datasets(id=dataset_id)
        if not datasets:
            return {"error": f"Dataset with ID '{dataset_id}' not found."}
        dataset = datasets[0]

        document_list = []
        async with httpx.AsyncClient() as client:
            for file_path in file_paths:
                try:
                    if file_path.startswith("http://") or file_path.startswith("https://"):
                        # Handle URL
                        response = await client.get(file_path)
                        response.raise_for_status()
                        file_content = response.content
                        file_name = file_path.split("/")[-1]
                    else:
                        # Handle local file path
                        with open(file_path, "rb") as f:
                            file_content = f.read()
                        file_name = file_path.split("/")[-1].split("\\")[-1]
                    
                    document_list.append({"display_name": file_name, "blob": file_content})

                except FileNotFoundError:
                    return {"error": f"File not found: {file_path}"}
                except httpx.HTTPStatusError as e:
                    return {"error": f"HTTP error downloading {file_path}: {e.response.status_code} - {e.response.text}"}
                except Exception as e:
                    return {"error": f"Error processing file/URL {file_path}: {str(e)}"}

        if not document_list:
            return {"error": "No valid files to upload."}

        uploaded_docs = dataset.upload_documents(document_list)
        
        return {
            "uploaded_documents": [
                {
                    "id": doc.id,
                    "name": doc.name,
                    "status": doc.status,
                }
                for doc in uploaded_docs
            ]
        }
    except Exception as e:
        logger.error(f"Error uploading files to dataset {dataset_id}: {e}")
        return {"error": str(e)}

def _trigger_parsing_and_wait(dataset, document_ids: List[str], timeout: int = 20) -> Dict:
    """
    Triggers parsing for given document IDs and waits until all are done.

    Args:
        dataset: The dataset object from RAGFlow.
        document_ids: List of document IDs to parse.
        timeout: Max time to wait in seconds (default 20).

    Returns:
        A dictionary with status and message.
    """
    if not document_ids:
        return {"status": "skipped", "message": "No documents to parse."}

    try:
        dataset.async_parse_documents(document_ids=document_ids)
        logger.info(f"Parsing triggered for documents: {document_ids}")

        for doc_id in document_ids:
            i = 0
            current_doc = dataset.list_documents(id=doc_id)[0]
            while current_doc.run != "DONE":
                logger.info(f"Current parsing status for {doc_id}: {current_doc.run}. Will sleep 1 second.")
                time.sleep(1)
                current_doc = dataset.list_documents(id=doc_id)[0]
                i += 1
                if i >= timeout:
                    error_msg = f"Document parsing exceeded time limit: {timeout} s."
                    logger.error(error_msg)
                    return {"error": error_msg}
                elif current_doc.run == "FAIL":
                    error_msg = f"Document parsing failed for {doc_id}. Final status: {current_doc.run}"
                    logger.error(error_msg)
                    return {"error": error_msg}

            logger.success(f"Parsing completed for {doc_id}. Final status: {current_doc.run}")

        return {
            "status": "success",
            "message": f"Documents parsed successfully: {document_ids}"
        }
    except Exception as e:
        logger.error(f"Error during parsing: {e}")
        return {"error": str(e)}

@mcp.tool()
async def trigger_parsing_and_wait(
    api_key: str,
    base_url: str,
    dataset_id: str,
    document_ids: List[str],
    timeout: int = 20
) -> Dict:
    """
    Triggers parsing for given document IDs and waits until all are done.

    Args:
        api_key: RAGFlow API key.
        base_url: RAGFlow instance base URL.
        dataset_id: The ID of the dataset containing the documents.
        document_ids: List of document IDs to parse.
        timeout: Max time to wait in seconds (default 20).

    Returns:
        A dictionary with status and message.
    """
    if not document_ids:
        return {"status": "skipped", "message": "No documents to parse."}

    try:
        # 1. 连接 RAGFlow
        rag_flow = RAGFlow(api_key=api_key, base_url=base_url)
        
        # 2. 获取 dataset 实例
        dataset = rag_flow.list_datasets(id=dataset_id)[0]
        
        # 3. 调用内部函数处理解析逻辑
        return _trigger_parsing_and_wait(dataset, document_ids, timeout) # TODO wait
    
    except Exception as e:
        logger.error(f"Error linking to RAGFlow dataset {dataset_id}: {e}")
        return {"error": str(e)}

def _upload_parse_and_delete(dataset, document, document_id, modified_content_bytes, action_desc="added"):
    """
    Used in **add_chunk** and **update_document** (change in chunks).
    """
    current_time_str = datetime.now().strftime("%Y%m%d%H%M%S")
    new_document_name = f"{current_time_str}{action_desc}_{document.name.split(f'{action_desc}_', 1)[-1]}"
    logger.info(f"Document name to be uploaded: {new_document_name}")

    uploaded_docs = dataset.upload_documents([{"display_name": new_document_name, "blob": modified_content_bytes}])
    new_doc_id = uploaded_docs[0].id
    logger.info(f"Modified content uploaded. New document ID: {new_doc_id}")

    parse_result = _trigger_parsing_and_wait(dataset, [new_doc_id])
    if "error" in parse_result:
        return parse_result

    # ✅ 重新获取文档状态
    current_uploaded_doc = dataset.list_documents(id=new_doc_id)[0]
    if current_uploaded_doc.run == "DONE" and current_uploaded_doc.chunk_count >= document.chunk_count:
        dataset.delete_documents(ids=[document_id])
        logger.success(f"Original document {document_id} deleted successfully.")

    return {
        "status": "success",
        "new_document_id": new_doc_id,
        "new_document_name": new_document_name,
        "message": f"Document successfully {action_desc}."
    }

@mcp.tool()
async def add_chunk_to_document(
    api_key: str,
    base_url: str,
    dataset_id: str,
    document_id: str,
    content: str,
    important_keywords: List[str] = None
) -> Dict:
    """
    Adds a chunk to a specific document within a knowledge base (General and QA chunk method are supported temporarily).

    Args:
        api_key: RAGFlow API key.
        base_url: RAGFlow instance base URL.
        dataset_id: The ID of the dataset containing the document.
        document_id: The ID of the document to which the chunk will be added.
        content: The text content of the chunk. For `General` dataset, a common string; for `QA` dataset, a string with '\\t' to separate question and answer.
        important_keywords: Optional list of key terms or phrases to tag with the chunk.

    Returns:
        A dictionary representing the added chunk, or an error dictionary.
    """
    
    try:
        rag_flow = RAGFlow(api_key=api_key, base_url=base_url)
        dataset = rag_flow.list_datasets(id=dataset_id)[0]
    except Exception as e:
        logger.error(f"Error linking to RAGFlow dataset {dataset_id}: {e}")
        return {"error": str(e)}
    
    try:
        document = dataset.list_documents(id=document_id)[0]
    except Exception as e:
        logger.error(f"Error finding document by id {document_id}: {e}")
        return {"error": str(e)}
    
    # MAIN: chunk_method of the dataset
    # 1 General: download -> add -> upload -> parse -> if success: delete
    if dataset.chunk_method == "naive":
        try:
            logger.info("---General dataset---")
            original_content_bytes = document.download()
            original_content = original_content_bytes.decode('utf-8')
            logger.info("Original document decoded.")

            modified_content = original_content + '\n' + content
            modified_content_bytes = modified_content.encode('utf-8')
            logger.info("Original document added and encoded.")

            result = _upload_parse_and_delete(dataset, document, document_id, modified_content_bytes, action_desc="added")
        except Exception as e:
            logger.error(f"Error adding chunk to document {document_id}: {e}")
            return {"error": str(e)}
        
    # 2 QA
    elif dataset.chunk_method == "qa": # Q&A
        doc_name = document.name

        # 2.1 txt: not supported
        if doc_name.endswith(".txt"):
            try:
                logger.info("---QA dataset: txt file---")
                original_content_bytes = document.download()
                original_content = original_content_bytes.decode('utf-8')
                logger.info("Original document decoded.")

                processed_content = content.replace('\\t', '\t')
                if '\t' not in processed_content:
                    error_msg = f"Content must contain '\\t' to separate question and answer: {processed_content}"
                    logger.error(error_msg)
                    return {"error": error_msg}
                
                modified_content = original_content + '\n' + processed_content
                modified_content_bytes = modified_content.encode('utf-8')
                logger.info("Original document added and encoded.")

                result = _upload_parse_and_delete(dataset, document, document_id, modified_content_bytes, action_desc="added")
            except Exception as e:
                logger.error(f"Error adding chunk to document {document_id}: {e}")
                return {"error": str(e)}
        
        # 2.2 xlsx: download -> add -> upload -> parse -> if success: delete
        elif doc_name.endswith(".xlsx"):
            try:
                logger.info("---QA dataset: xlsx file---")
                original_content_bytes = document.download()

                # 使用pandas读取Excel文件（无标题行）
                df = pd.read_excel(io.BytesIO(original_content_bytes), header=None)
                logger.info("Original xlsx document loaded into DataFrame (no headers).")
                
                # 分割content为question和answer
                processed_content = content.replace('\\t', '\t')
                if '\t' not in processed_content:
                    error_msg = f"Content must contain '\\t' to separate question and answer: {processed_content}"
                    logger.error(error_msg)
                    return {"error": error_msg}
                question, answer = processed_content.split('\t', 1)  # 只分割一次，防止answer中包含\t
                
                # 添加新行到DataFrame（保持无标题）
                new_row = pd.DataFrame([[question, answer]])
                df = pd.concat([df, new_row], ignore_index=True)
                logger.info("New QA pair added to DataFrame.")
                
                # 将修改后的DataFrame重新编码为Excel二进制格式（不保存标题）
                excel_buffer = io.BytesIO()
                df.to_excel(excel_buffer, index=False, header=False, engine='openpyxl')
                modified_content_bytes = excel_buffer.getvalue()
                logger.info("Modified DataFrame encoded to Excel bytes (no headers).")

                result = _upload_parse_and_delete(dataset, document, document_id, modified_content_bytes, action_desc="added")
            except Exception as e:
                logger.error(f"Error adding chunk to document {document_id}: {e}")
                return {"error": str(e)}

        # 2.3 wrong file
        else: # 一般不会出现，RAGFlow的QA分块只支持txt/csv、xlsx
            logger.info("---QA dataset: other file---")
            error_msg = "For QA chunking knowledge base, adding chunks to .txt files is not supported. Please operate on .xlsx files."
            logger.error(error_msg)
            return {"error": error_msg}
        
    return result

@mcp.tool()
async def update_chunk_in_document(
    api_key: str,
    base_url: str,
    dataset_id: str,
    document_id: str,
    chunk_id: str,
    content: str,
    important_keywords: List[str] = None
) -> Dict:
    """
    Updates a chunk in a specific document within a knowledge base (General and QA chunk method are supported temporarily).

    Args:
        api_key: RAGFlow API key.
        base_url: RAGFlow instance base URL.
        dataset_id: The ID of the dataset containing the document.
        document_id: The ID of the document to which the chunk will be added.
        chunk_id: The ID of the chunk to be updated.
        content: The text content of the chunk. For `General` dataset, a common string; for `QA` dataset, a string with '\\t' to separate question and answer.
        important_keywords: Optional list of key terms or phrases to tag with the chunk.

    Returns:
        A dictionary representing the added chunk, or an error dictionary.
    """        

    try:
        rag_flow = RAGFlow(api_key=api_key, base_url=base_url)
        dataset = rag_flow.list_datasets(id=dataset_id)[0]
    except Exception as e:
        logger.error(f"Error linking to RAGFlow dataset {dataset_id}: {e}")
        return {"error": str(e)}
    
    try:
        document = dataset.list_documents(id=document_id)[0]
    except Exception as e:
        logger.error(f"Error finding document by id {document_id}: {e}")
        return {"error": str(e)}
    
    # MAIN: chunk_method of the dataset
    # 1 General: download -> modify -> upload -> parse -> if success: delete
    if dataset.chunk_method == "naive":
        try:
            logger.info("---General dataset---")
            original_content_bytes = document.download()
            original_content = original_content_bytes.decode('utf-8')
            # Normalize newlines in original_content to '\n'
            original_content = original_content.replace('\r\n', '\n').replace('\r', '\n')
            logger.info("Original document decoded and newlines normalized.")

            # Get chunk to update
            old_chunk = None
            all_chunks = document.list_chunks()
            for chunk in all_chunks:
                if chunk.id == chunk_id:
                    old_chunk = chunk
                    break
            
            if not old_chunk:
                error_msg = f"Chunk with ID '{chunk_id}' not found in document '{document_id}'."
                logger.error(error_msg)
                return {"error": error_msg}

            # Normalize newlines in old_chunk.content to '\n' before replacement
            normalized_old_chunk_content = old_chunk.content.replace('\r\n', '\n').replace('\r', '\n')

            # Replace old chunk content with new content
            # Assumes normalized_old_chunk_content is an exact substring and replaces first occurrence
            modified_content = original_content.replace(normalized_old_chunk_content, content, 1)
            modified_content = modified_content.replace('\n', '\r\n')
            logger.success(modified_content)##############################################################################################################################
            modified_content_bytes = modified_content.encode('utf-8')
            logger.info("Document content modified and encoded.")

            result = _upload_parse_and_delete(dataset, document, document_id, modified_content_bytes, action_desc="modified")
            return {
                "status": "success",
                "operation": "document_replace",
                "new_document_id": result["new_document_id"],
                "new_document_name": result["new_document_name"],
                "message": "Original document replaced with updated content and parsed."
            }
        except Exception as e:
            logger.error(f"Error updating chunk in document {document_id}: {e}")
            return {"error": str(e)}
        
    # 2 QA
    elif dataset.chunk_method == "qa": # Q&A
        # Get chunk to update
        old_chunk = None
        try:
            all_chunks = document.list_chunks()
            for chunk in all_chunks:
                if chunk.id == chunk_id:
                    old_chunk = chunk
                    break
            
            if not old_chunk:
                error_msg = f"Chunk with ID '{chunk_id}' not found in document '{document_id}'."
                logger.error(error_msg)
                return {"error": error_msg}

            # Extract original QA content from RAGFlow's parsed chunk content
            ragflow_chunk_content = old_chunk.content
            if ragflow_chunk_content.startswith("Question: "):
                ragflow_chunk_content = ragflow_chunk_content[len("Question: "):]
            
            parts = ragflow_chunk_content.split('\tAnswer: ', 1)
            if len(parts) == 2:
                original_old_chunk_content = f"{parts[0]}\t{parts[1]}"
            else:
                original_old_chunk_content = ragflow_chunk_content # Fallback if format is unexpected
            
            # 分割content为question和answer
            processed_content = content.replace('\\t', '\t')
            if '\t' not in processed_content:
                error_msg = f"Content must contain '\\t' to separate question and answer: {processed_content}"
                logger.error(error_msg)
                return {"error": error_msg}

        except Exception as e:
            logger.error(f"Error processing old chunk content: {e}")
            return {"error": str(e)}

        doc_name = document.name

        # 2.1 txt: download -> modify -> upload -> parse -> if success: delete
        if doc_name.endswith(".txt"):
            try:
                logger.info("---QA dataset: txt file---")
                original_content_bytes = document.download()
                original_content = original_content_bytes.decode('utf-8')
                logger.info("Original document decoded.")
                
                # Replace old chunk content with new content
                # User provided content should be "question\tanswer" format
                modified_content = original_content.replace(original_old_chunk_content, processed_content, 1)
                modified_content_bytes = modified_content.encode('utf-8')
                logger.info("Original document updated and encoded.")

                result = _upload_parse_and_delete(dataset, document, document_id, modified_content_bytes, action_desc="modified")

                return {
                    "status": "success",
                    "operation": "document_replace",
                    "new_document_id": result["new_document_id"],
                    "new_document_name": result["new_document_name"],
                    "message": "Original document replaced with updated content and parsed."
                }
            except Exception as e:
                logger.error(f"Error updating chunk in document {document_id}: {e}")
                return {"error": str(e)}
        
        # 2.2 xlsx: download -> modify -> upload -> parse -> if success: delete
        elif doc_name.endswith(".xlsx"):
            try:
                logger.info("---QA dataset: xlsx file---")
                original_content_bytes = document.download()

                # Read Excel file into pandas DataFrame (no header)
                df = pd.read_excel(io.BytesIO(original_content_bytes), header=None)
                logger.info("Original xlsx document loaded into DataFrame (no headers).")
                
                # Split old and new content for QA format
                old_question, old_answer = original_old_chunk_content.split('\t', 1)
                new_question, new_answer = processed_content.split('\t', 1)

                # Find row index of the old chunk content
                row_index = -1
                for idx, row in df.iterrows():
                    # Ensure row has enough columns and content matches
                    if len(row) >= 2 and str(row[0]) == old_question and str(row[1]) == old_answer:
                        row_index = idx
                        break
                
                if row_index == -1:
                    error_msg = f"Could not find matching row for chunk ID '{chunk_id}' in Excel document."
                    logger.error(error_msg)
                    return {"error": error_msg}

                # Update specific row
                df.at[row_index, 0] = new_question
                df.at[row_index, 1] = new_answer
                logger.info(f"QA pair updated in DataFrame at row {row_index}.")
                
                # Encode modified DataFrame back to Excel binary format (no header)
                excel_buffer = io.BytesIO()
                df.to_excel(excel_buffer, index=False, header=False, engine='openpyxl')
                modified_content_bytes = excel_buffer.getvalue()
                logger.info("Modified DataFrame encoded to Excel bytes (no headers).")

                result = _upload_parse_and_delete(dataset, document, document_id, modified_content_bytes, action_desc="modified")
                return {
                    "status": "success",
                    "operation": "document_replace",
                    "new_document_id": result["new_document_id"],
                    "new_document_name": result["new_document_name"],
                    "message": "Original document replaced with updated content and parsed."
                }
            except Exception as e:
                logger.error(f"Error updating chunk in document {document_id}: {e}")
                return {"error": str(e)}

        # 2.3 wrong file type
        else: # 一般不会出现，RAGFlow的QA分块只支持txt/csv、xlsx
            logger.info("---QA dataset: other file---")
            error_msg = "For QA chunking knowledge base, updating chunks in .txt or .xlsx files is supported. Other file types are not."
            logger.error(error_msg)
            return {"error": error_msg}
        
    # Default return if no conditions met
    return {
        "status": "error",
        "message": "Chunk update operation failed. Check dataset and file types."
    }

@mcp.tool()
async def upload_qa_content_and_parse(
    api_key: str,
    base_url: str,
    dataset_id: str,
    content: str,
    document_name: str = "uploaded_content.txt"
) -> Dict:
    """
    Uploads string content as a txt/xlsx file to a QA knowledge base and triggers its parsing.

    Args:
        api_key: RAGFlow API key.
        base_url: RAGFlow instance base URL.
        dataset_id: The ID of the dataset to upload content to.
        content: The string content to be uploaded.
        document_name: The name of the document to be displayed in RAGFlow. Defaults to "uploaded_content.txt".

    Returns:
        A dictionary with a list of uploaded document details and parsing status, or an error message.
    """

    try:
        rag_flow = RAGFlow(api_key=api_key, base_url=base_url)
        dataset = rag_flow.list_datasets(id=dataset_id)[0]
    except Exception as e:
        logger.error(f"Error linking to RAGFlow: {e}")
        return {"error": str(e)}

    # 内容编码
    if document_name.endswith('.txt'):
        processed_content = content.replace('\\t', '\t')
        logger.info(f"Processed_content (repr): {repr(processed_content)}")
        
        # Encode the string content to UTF-8 bytes
        document_blob = processed_content.encode("utf-8")
        logger.info(f"Document_blob successfully encoded.")
    elif document_name.endswith('.xlsx'):
        # 内存中创建二进制缓冲区（虚拟Excel文件）以获取二进制写入内容
        processed_content = content.split("\t", 1)
        logger.info(f"Processed_content (repr): {repr(processed_content)}")

        # Encode the string content to UTF-8 bytes
        qa_data = [(processed_content[0], processed_content[1])]
        df = pd.DataFrame(qa_data)
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False, engine='openpyxl')
        document_blob = excel_buffer.getvalue()
        logger.info(f"Document_blob successfully encoded.")
    else:
        logger.error(f"Unsupported file type: {document_name}")
        return {"error": "Unsupported file type."}
        
    # 上传与解析
    try:
        document_list = [{"display_name": document_name, "blob": document_blob}]

        # Upload the document
        uploaded_docs = dataset.upload_documents(document_list)
        
        uploaded_doc_details = []
        document_ids_to_parse = []
        for doc in uploaded_docs:
            uploaded_doc_details.append({
                "id": doc.id,
                "name": doc.name,
                "status": doc.status,
            })
            document_ids_to_parse.append(doc.id)

        # Trigger parsing for the uploaded documents
        if document_ids_to_parse:
            dataset.async_parse_documents(document_ids_to_parse)
            parsing_status = f"Parsing triggered for documents: {document_ids_to_parse}"
        else:
            parsing_status = "No documents uploaded to parse."

        return {
            "uploaded_documents": uploaded_doc_details,
            "parsing_status": parsing_status
        }
    except Exception as e:
        logger.error(f"Error uploading content and parsing for dataset {dataset_id}: {e}")
        return {"error": str(e)}
