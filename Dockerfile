# Build the browser app with Node, the server with Rust, and run only the
# Rust server: it serves dist/ and every /api route on $PORT.

FROM node:26-trixie-slim AS web
WORKDIR /app
ENV PUPPETEER_SKIP_DOWNLOAD=true
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts
COPY . .
RUN npm run build && node scripts/export-voice-tools.mjs

FROM rust:1-trixie AS server
WORKDIR /app/server-rs
COPY server-rs/ ./
RUN cargo build --release --locked --bins

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
# Bundled data the provider routes read at runtime.
COPY --from=web /app/package.json ./package.json
COPY --from=web /app/public ./public
COPY --from=web /app/src ./src
COPY --from=web /app/dist ./dist
COPY --from=web /app/server-rs/generated ./server-rs/generated
COPY --from=server /app/server-rs/target/release/camera-eye-server /app/server-rs/target/release/camera-eye-mcp /usr/local/bin/
ENV HOST=0.0.0.0 PORT=4173 CE_ROOT=/app
EXPOSE 4173
CMD ["camera-eye-server"]
