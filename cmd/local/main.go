// Command local runs the consumer against SQS and DynamoDB without Lambda — normally ElasticMQ and
// DynamoDB Local from docker compose (set AWS_ENDPOINT_URL), but it works against AWS too.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/handler"
	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/ledger"
	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/poller"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil && err != context.Canceled {
		log.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	db := dynamodb.NewFromConfig(cfg)
	table := env("TABLE_NAME", "usdc-ledger")
	if err := ledger.CreateTable(ctx, db, table); err != nil {
		return err
	}
	h := &handler.Handler{Ledger: ledger.New(db, table, ledger.ParseAccounts(os.Getenv("WATCH_ADDRESSES"))), Log: log}
	p := &poller.Poller{SQS: sqs.NewFromConfig(cfg), QueueURL: os.Getenv("QUEUE_URL"), Handle: h.Handle, Wait: 10, Log: log}
	log.Info("polling", "queue", p.QueueURL, "table", table)
	return p.Run(ctx)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
