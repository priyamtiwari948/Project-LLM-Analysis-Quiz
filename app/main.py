import asyncio
import sys

# 🔥 Windows Fix for Async Event Loop Issue (Avoids 500 internal errors)
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
from app.config import SECRET, EMAIL
from app.solver import solve_quiz_chain

app = FastAPI(title="LLM Analysis Quiz Solver")

class QuizRequest(BaseModel):
    email: str
    secret: str
    url: HttpUrl

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "name": "LLM Analysis Quiz Solver",
        "version": "1.0",
        "status": "running",
        "endpoints": {
            "/solve": "POST - Solve a quiz chain",
            "/docs": "GET - Interactive API documentation"
        },
        "usage": {
            "endpoint": "/solve",
            "method": "POST",
            "body": {
                "email": "your-email@example.com",
                "secret": "your-secret",
                "url": "https://quiz-url.com/quiz"
            }
        },
        "configured_email": EMAIL
    }

@app.post("/solve")
async def solve_quiz(req: QuizRequest):
    if req.secret != SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    try:
        result = await solve_quiz_chain(str(req.url), req.email, req.secret)
        return {"ok": True, "steps": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
