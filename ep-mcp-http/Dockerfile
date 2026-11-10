FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY ep_mcp_http ./ep_mcp_http
RUN pip install --no-cache-dir .
EXPOSE 8092
CMD ["ep-mcp-http"]
