# Build the browser app with Node, the server with Rust, and run only the
# Rust server: it serves dist/ and every /api route on $PORT.

FROM node:24-bookworm-slim AS web
WORKDIR /app
ENV PUPPETEER_SKIP_DOWNLOAD=true
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts
COPY . .
RUN npm run build

FROM rust:1-bookworm AS server
WORKDIR /app/server-rs
COPY server-rs/Cargo.toml server-rs/Cargo.lock ./
# Cache dependencies separately from the server's own code.
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src
COPY server-rs/ ./
RUN touch src/main.rs && cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
# Bundled data the provider routes read at runtime.
COPY --from=web /app/public ./public
COPY --from=web /app/src ./src
COPY --from=web /app/dist ./dist
COPY --from=server /app/server-rs/target/release/camera-eye-server /usr/local/bin/camera-eye-server
ENV HOST=0.0.0.0 PORT=4173 CE_ROOT=/app
EXPOSE 4173
CMD ["camera-eye-server"]
