# syntax=docker/dockerfile:1.7
FROM node:22-alpine AS builder

WORKDIR /srv/notera/frontend

COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci

COPY frontend/ ./
RUN npm run build

FROM nginx:1.27-alpine

COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /srv/notera/frontend/dist /usr/share/nginx/html

EXPOSE 80
