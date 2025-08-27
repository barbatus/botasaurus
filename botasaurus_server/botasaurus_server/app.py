def run_server():
    import uvicorn

    from .fastapi_app import app as fastapi_app

    uvicorn.run(fastapi_app, host="0.0.0.0", port=8000, log_level="info")
