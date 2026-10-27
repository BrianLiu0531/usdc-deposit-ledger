// Package awstest connects tests to the local AWS stand-ins from docker compose.
package awstest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/ledger"
)

func cfg() aws.Config {
	return aws.Config{Region: "ap-southeast-2", Credentials: credentials.NewStaticCredentialsProvider("local", "local", "")}
}

func endpoint(t testing.TB, env, def string) string {
	t.Helper()
	ep := os.Getenv(env)
	if ep == "" {
		ep = def
	}
	u, _ := url.Parse(ep)
	conn, err := net.DialTimeout("tcp", u.Host, time.Second)
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("%s unreachable: %v", ep, err)
		}
		t.Skipf("%s unreachable; run `docker compose up -d`", ep)
	}
	conn.Close()
	return ep
}

// Name returns a unique resource name.
func Name(prefix string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}

// Table creates a ledger table in DynamoDB Local that is deleted when the test ends.
func Table(t testing.TB) (*dynamodb.Client, string) {
	t.Helper()
	ep := endpoint(t, "DYNAMODB_ENDPOINT", "http://localhost:8000")
	db := dynamodb.NewFromConfig(cfg(), func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(ep) })
	name := Name("ledger")
	if err := ledger.CreateTable(context.Background(), db, name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: &name}) })
	return db, name
}

// SQS returns a client for ElasticMQ.
func SQS(t testing.TB) *sqs.Client {
	t.Helper()
	ep := endpoint(t, "SQS_ENDPOINT", "http://localhost:9324")
	return sqs.NewFromConfig(cfg(), func(o *sqs.Options) { o.BaseEndpoint = aws.String(ep) })
}
