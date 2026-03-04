"""
RAG Generator — Generates answers from retrieved context using Ollama

Takes retrieved filing chunks / news articles as context and generates
a grounded answer with citations using a local LLM via Ollama.
"""

import os
import httpx
from loguru import logger


OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")

SYSTEM_PROMPT = """You are ATLAS, a financial intelligence assistant. You answer questions based ONLY on the provided context from SEC filings and news articles.

Rules:
1. Answer ONLY from the provided context. If the context doesn't contain the answer, say "I don't have enough information in the available filings/news to answer this."
2. Always cite your sources using [Source N] notation.
3. Be precise with numbers — quote exact figures from the filings.
4. Keep answers concise but thorough."""


def format_context(filings: list[dict], news: list[dict]) -> str:
    """Format retrieved documents into context string for the LLM."""
    parts = []

    for i, f in enumerate(filings, 1):
        parts.append(
            f"[Source {i}] SEC Filing — {f['ticker']} {f['form_type']} "
            f"(filed {f['filed_at']})\n{f['content'][:2000]}"
        )

    offset = len(filings)
    for i, n in enumerate(news, 1):
        parts.append(
            f"[Source {offset + i}] News — {n['source']} "
            f"({n['published_at'][:10] if n['published_at'] else 'recent'})\n"
            f"Title: {n['title']}\n{n.get('summary', '')[:1000]}"
        )

    return "\n\n---\n\n".join(parts)


def generate(
    question: str,
    context: str,
    model: str = "llama3.2:1b",
    stream: bool = True,
) -> str:
    """
    Generate an answer using Ollama.

    Args:
        question: User's question
        context: Formatted context from retriever
        model: Ollama model name
        stream: Whether to stream the response (prints tokens as they arrive)

    Returns:
        Complete answer string
    """
    prompt = f"""Context:
{context}

Question: {question}

Answer (cite sources using [Source N]):"""

    payload = {
        "model": model,
        "prompt": prompt,
        "system": SYSTEM_PROMPT,
        "stream": stream,
        "options": {
            "temperature": 0.1,
            "top_p": 0.9,
            "num_predict": 1024,
        },
    }

    try:
        if stream:
            answer_parts = []
            with httpx.Client(timeout=120.0) as client:
                with client.stream("POST", f"{OLLAMA_URL}/api/generate", json=payload) as resp:
                    resp.raise_for_status()
                    for line in resp.iter_lines():
                        if line:
                            import json
                            chunk = json.loads(line)
                            token = chunk.get("response", "")
                            print(token, end="", flush=True)
                            answer_parts.append(token)
                            if chunk.get("done"):
                                break
            print()  # newline after streaming
            return "".join(answer_parts)

        else:
            with httpx.Client(timeout=120.0) as client:
                resp = client.post(f"{OLLAMA_URL}/api/generate", json=payload)
                resp.raise_for_status()
                return resp.json().get("response", "")

    except httpx.ConnectError:
        logger.error(f"Cannot connect to Ollama at {OLLAMA_URL}. Is it running?")
        logger.info("Start Ollama with: ollama serve")
        return "Error: Ollama is not running."
    except Exception as e:
        logger.error(f"Generation failed: {e}")
        return f"Error: {e}"


def check_ollama(model: str = "llama3.2:1b") -> bool:
    """Check if Ollama is running and the model is available."""
    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.get(f"{OLLAMA_URL}/api/tags")
            if resp.status_code == 200:
                models = [m["name"] for m in resp.json().get("models", [])]
                if model in models or any(model.split(":")[0] in m for m in models):
                    return True
                logger.warning(f"Model {model} not found. Available: {models}")
                logger.info(f"Pull it with: ollama pull {model}")
                return False
    except Exception:
        return False
