ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.22-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go                      ./go
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-rockset     ./materialize-rockset

# Test and build the connector. These hit the Rockset API, so it needs a real key. You can pass
# these with Buildkit's `--secret` argument to `docker build`.
# To do this locally, set a ROCKSET_API_KEY env var, then add the following to your normal `docker build` command:
#   `--secret # id=rockset_api_key,env=ROCKSET_API_KEY`
# See https://andrei-calazans.com/posts/2021-06-23/passing-secrets-github-actions-docker for more
# info about build-time secrets.
RUN --mount=type=secret,id=rockset_api_key \
  export ROCKSET_API_KEY=$(cat /run/secrets/rockset_api_key) && \
  go test  -tags nozstd -v ./materialize-rockset/

RUN go build -tags nozstd -v -o ./connector ./materialize-rockset/cmd/connector/

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./materialize-rockset

# Avoid running the connector as root.
USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-rockset"]
