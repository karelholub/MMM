# syntax=docker/dockerfile:1.7
FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --prefer-offline \
    && arch="$(uname -m)" \
    && case "$arch" in \
        aarch64|arm64) rollup_pkg='@rollup/rollup-linux-arm64-musl@4.52.5' ;; \
        x86_64) rollup_pkg='@rollup/rollup-linux-x64-musl@4.52.5' ;; \
        *) echo "Unsupported architecture: $arch" && exit 1 ;; \
    esac \
    && npm install --no-audit --prefer-offline --no-save "$rollup_pkg"
COPY . /app
CMD ["npm", "run", "dev", "--", "--host"]
