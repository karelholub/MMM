FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci \
    && arch="$(uname -m)" \
    && case "$arch" in \
        aarch64|arm64) rollup_pkg='@rollup/rollup-linux-arm64-musl@4.52.5' ;; \
        x86_64) rollup_pkg='@rollup/rollup-linux-x64-musl@4.52.5' ;; \
        *) echo "Unsupported architecture: $arch" && exit 1 ;; \
    esac \
    && npm install --no-save "$rollup_pkg"
COPY . /app
CMD ["npm", "run", "dev", "--", "--host"]
