# syntax=docker/dockerfile:1
#
# The SPARQL console: a static bundle served by nginx, configured at container start.
#
# Built with bun, which is both the package manager and the task runner here. The result is plain
# files -- there is no server-side anything, and deliberately so: this console holds no secret,
# only the token the user signed in with.

FROM oven/bun:1.3-alpine AS build
WORKDIR /app

# Dependencies first, so editing a component does not re-resolve the tree.
COPY package.json bun.lock* ./
RUN bun install --frozen-lockfile

COPY tsconfig.json vite.config.ts index.html ./
COPY src/ ./src/
RUN bun run build

FROM nginx:1.29-alpine

COPY --from=build /app/dist/ /usr/share/nginx/html/
COPY nginx.conf    /etc/nginx/conf.d/default.conf
COPY entrypoint.sh /docker-entrypoint.d/40-sparql-console-config.sh

RUN chmod +x /docker-entrypoint.d/40-sparql-console-config.sh

EXPOSE 80
