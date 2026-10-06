# The simulator fleet image: the web control panel plus the stations it manages, in one container.
# Published as eosvoltaps/ocpp-simulators by .github/workflows/build_image.yml; Spark's dev stack
# runs it behind its `simulator` compose profile. docker/entrypoint.sh sets the profile and log
# directories, optionally pre-creates stations from OCPP_SIM_STATIONS, and starts the panel, which
# spawns and supervises every station process (see README, "Web panel").
#
# Dockerfile.vcp is upstream's image for one headless station driven by its admin API.
FROM node:22-alpine
WORKDIR /app
RUN apk add --no-cache bash
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY . .
RUN mkdir -p logs profiles && chmod +x docker/entrypoint.sh
ENV SIM_PROFILES_DIR=/app/profiles \
    SIM_LOG_DIR=/app/logs \
    WEB_HOST=0.0.0.0 \
    WEB_PORT=8080 \
    WS_URL=ws://localhost:9000
EXPOSE 8080
CMD ["/app/docker/entrypoint.sh"]
