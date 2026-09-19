# syntax=docker/dockerfile:1
#
# Apache Polaris Console.
#
# The console lives in apache/polaris-tools (NOT apache/polaris, which has no UI at all), under
# console/. It is a Vite + React + TypeScript app. Upstream ships its own Dockerfile built on
# Red Hat UBI images and expects the console directory as its build context; this one fetches a
# pinned source tree instead, so the PoC repo does not have to vendor or submodule it, and uses
# smaller alpine bases.
#
# There are no console release tags in polaris-tools -- the only tags belong to the catalog
# migrator -- so this pins a commit.
ARG POLARIS_TOOLS_REF=8ffe1e52ed78579a815905d8414702d1082e5a18

FROM node:22-alpine AS builder
ARG POLARIS_TOOLS_REF
WORKDIR /src

RUN apk add --no-cache tar

# Fetching rather than COPYing keeps this repo free of a vendored copy of someone else's app.
RUN wget -qO- "https://github.com/apache/polaris-tools/archive/${POLARIS_TOOLS_REF}.tar.gz" \
      | tar xz --strip-components=1

WORKDIR /src/console

# Upstream uses `npm install` here too; the repo ships no lockfile to `npm ci` against.
RUN npm install --no-audit --no-fund

RUN npm run build


FROM nginx:1.27-alpine

COPY nginx.conf    /etc/nginx/conf.d/default.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

COPY --from=builder /src/console/dist /usr/share/nginx/html

# index.html loads /config.js, and src/lib/config.ts prefers window.APP_CONFIG over the values
# Vite inlined at build time. That is what lets one image be pointed at different Polaris and
# identity-provider URLs without rebuilding.
EXPOSE 80
ENTRYPOINT ["/entrypoint.sh"]
