# 🤖 Supply Chain Multi‑Agent System

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.136-green.svg)](https://fastapi.tiangolo.com/)
[![LangGraph](https://img.shields.io/badge/LangGraph-1.1-purple.svg)](https://langchain-ai.github.io/langgraph/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Multi‑agent supply chain orchestration** – event‑driven, fully containerised, and ready to demo. Built with Python 3.11, FastAPI, LangGraph, OpenAI (or Groq/OpenRouter), and Redis Streams.

---
<img width="1805" height="878" alt="image" src="https://github.com/user-attachments/assets/7d8bdd13-7d2d-4bdb-b131-4fdde1765e7d" />
<img width="1867" height="790" alt="image" src="https://github.com/user-attachments/assets/6f8f1fd4-1005-4b1c-9238-07f41e85caf3" />


## 🚀 Quick start (Windows / PowerShell)

```powershell
# 1. Copy environment template
copy .env.example .env
# Edit .env – add your OPENAI_API_KEY (supports Groq `gsk_...` or OpenRouter `sk-or-...`)

# 2. Build and start all services
docker compose up -d --build

# 3. Wait ~30s, then seed demo data
docker compose exec api python -m scripts.seed_demo_data

# 4. Fire test orders (manufactured, purchased, high‑value, stock‑low)
docker compose exec api python -m scripts.fire_test_order

# 5. Watch the orchestra
docker compose logs -f orchestrator
