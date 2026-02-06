FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . /app
CMD ["npm", "run", "dev", "--", "--host"]