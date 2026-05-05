"""Entry point for: python -m temporal_light.api"""

import os

import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("API_PORT", "8080"))
    uvicorn.run(
        "temporal_light.api.server:app",
        host="0.0.0.0",
        port=port,
    )
