# ----------- Stage 1: Builder (Golang environment) -----------
FROM golang:1.24.7-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum (assuming they are in the root of the build context)
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire application source code. This brings 'src/' into '/app/src'.
COPY . .

# --- Build the process Service ---
# FIX: Use the full relative path to the 'main' package folder inside the container.
# Path: /app/src/service/process/main
RUN go build -o process-app ./src/service/process/main

# --- Build the Process Service ---
# FIX: Use the full relative path to the 'main' package folder inside the container.
# Path: /app/src/service/process/main
RUN go build -o process-app ./src/service/process/main


# ----------- Stage 2: Runtime for 'process' Service (Tiny Alpine Image) -----------
FROM alpine:3.20 AS process

# Copy the compiled 'process-app' binary from the builder stage
COPY --from=builder /app/process-app /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/process-app"]

