FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o proxy-service .

# Use a smaller image for the final application
FROM alpine:latest  

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/proxy-service .

# Expose the application port
EXPOSE 8010

# Command to run the executable
CMD ["./proxy-service"]