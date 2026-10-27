# Local consumer (polls SQS, writes DynamoDB) for docker compose; in AWS the same handler runs in Lambda
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/local

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/consumer /consumer
ENTRYPOINT ["/consumer"]
