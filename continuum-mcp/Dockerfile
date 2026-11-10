# Continuum MCP server — streamable HTTP on :8091
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.6.14 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    CATALOG_DIR=/app/catalog \
    MCP_HOST=0.0.0.0 \
    MCP_PORT=8091

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY continuum_mcp ./continuum_mcp
COPY catalog ./catalog
RUN uv sync --frozen --no-dev

# Created products are written under /app/catalog/products — mount a volume to persist them.
VOLUME ["/app/catalog/products"]
EXPOSE 8091

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s \
    CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8091/healthz', timeout=3).status == 200 else 1)"]

CMD ["uv", "run", "--no-dev", "--frozen", "python", "-m", "continuum_mcp.server"]
