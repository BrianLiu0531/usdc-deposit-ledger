// Command consumer is the Lambda function that credits transfer events from SQS to DynamoDB.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/handler"
	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/ledger"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Error("load AWS config", "err", err)
		os.Exit(1)
	}
	h := &handler.Handler{
		Ledger: ledger.New(dynamodb.NewFromConfig(cfg), mustEnv(log, "TABLE_NAME"), ledger.ParseAccounts(mustEnv(log, "WATCH_ADDRESSES"))),
		Log:    log,
	}
	lambda.Start(h.Handle)
}

func mustEnv(log *slog.Logger, key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Error("missing environment variable", "key", key)
		os.Exit(1)
	}
	return v
}
