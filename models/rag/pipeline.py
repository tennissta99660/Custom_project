"""
ATLAS RAG Pipeline — Ask questions about SEC filings and news

Interactive CLI for the Retrieval-Augmented Generation system.
Searches pgvector for relevant documents, feeds them to Ollama, and
generates cited answers.

Usage:
    python models/rag/pipeline.py
    python models/rag/pipeline.py --question "What are Apple's risk factors?"
    python models/rag/pipeline.py --model llama3.2:3b
"""

import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from loguru import logger
from models.rag.retriever import Retriever
from models.rag.generator import generate, format_context, check_ollama


def ask(
    question: str,
    retriever: Retriever,
    model: str = "llama3.2:1b",
    top_k: int = 5,
    show_sources: bool = True,
) -> str:
    """
    Full RAG pipeline: question → retrieve → generate → answer with citations.
    """
    # 1. Retrieve relevant documents
    results = retriever.search_all(question, top_k=top_k)
    filings = results["filings"]
    news = results["news"]

    if not filings and not news:
        return "No relevant documents found. Try rephrasing your question."

    # 2. Show sources if requested
    if show_sources:
        print("\n📚 Retrieved Sources:")
        print("─" * 60)
        for i, f in enumerate(filings, 1):
            print(f"  [{i}] {f['ticker']} {f['form_type']} ({f['filed_at']}) "
                  f"— similarity: {f['similarity']}")
        offset = len(filings)
        for i, n in enumerate(news, 1):
            print(f"  [{offset + i}] {n['source']}: {n['title'][:60]}... "
                  f"— similarity: {n['similarity']}")
        print("─" * 60)
        print()

    # 3. Format context
    context = format_context(filings, news)

    # 4. Generate answer
    print("🤖 ATLAS:\n")
    answer = generate(question, context, model=model, stream=True)

    return answer


def interactive_mode(retriever: Retriever, model: str):
    """Run interactive Q&A loop."""
    print("=" * 60)
    print("  ATLAS Filing Q&A — Powered by RAG + Ollama")
    print("=" * 60)
    print(f"  Model: {model}")
    print("  Type 'quit' to exit, 'help' for commands")
    print("=" * 60)

    while True:
        try:
            print()
            question = input("❓ Ask: ").strip()

            if not question:
                continue
            if question.lower() in ("quit", "exit", "q"):
                print("Goodbye!")
                break
            if question.lower() == "help":
                print("""
Commands:
  quit/exit/q  — Exit the program
  help         — Show this help
  
Example questions:
  What are Apple's biggest risk factors?
  What was Tesla's revenue last year?
  Are there any supply chain risks mentioned in filings?
  What geopolitical events could impact tech stocks?
""")
                continue

            ask(question, retriever, model=model)

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            logger.error(f"Error: {e}")


def main():
    parser = argparse.ArgumentParser(description="ATLAS RAG Pipeline")
    parser.add_argument("--question", "-q", type=str, help="Single question to answer")
    parser.add_argument("--model", "-m", type=str, default="llama3.2:1b", help="Ollama model")
    parser.add_argument("--top-k", "-k", type=int, default=5, help="Number of sources to retrieve")
    args = parser.parse_args()

    # Check Ollama
    if not check_ollama(args.model):
        logger.error(f"Ollama not running or model '{args.model}' not available")
        logger.info("Start Ollama: ollama serve")
        logger.info(f"Pull model: ollama pull {args.model}")
        return

    # Initialize retriever
    logger.info("Loading retriever...")
    retriever = Retriever()
    logger.info("Retriever ready\n")

    if args.question:
        ask(args.question, retriever, model=args.model, top_k=args.top_k)
    else:
        interactive_mode(retriever, args.model)


if __name__ == "__main__":
    main()
