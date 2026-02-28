"""Social Scraper Platform — FastAPI backend."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from api.database import init_db
from api.routes.scrape import router as scrape_router
from api.routes.analysis import router as analysis_router
from api.routes.search import router as search_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[API] Initializing database tables...")
    init_db()
    print("[API] Social Scraper Platform ready")
    yield
    print("[API] Shutting down")


app = FastAPI(
    title="Social Scraper Platform",
    version="2.0.0",
    description="Full-platform social media scraping, storage, and NLP analysis",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(scrape_router, prefix="/api")
app.include_router(analysis_router, prefix="/api")
app.include_router(search_router, prefix="/api")


@app.get("/health")
def health():
    return {"status": "ok", "service": "social-scraper-platform"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
