# Stage 1: Build
FROM rust:1.83-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin strata_server --bin strata_flight_server

# Stage 2: Runtime
FROM debian:bookworm-slim
LABEL org.opencontainers.image.source=https://github.com/batalabs/strata-query
WORKDIR /app
COPY --from=builder /app/target/release/strata_server .
COPY --from=builder /app/target/release/strata_flight_server .
# 3131 = REST API, 41415 = Arrow Flight SQL. docker-compose selects which to run.
EXPOSE 3131 41415
CMD ["./strata_server"]
