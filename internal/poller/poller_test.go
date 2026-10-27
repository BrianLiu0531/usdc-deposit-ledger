package poller_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/awstest"
	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/handler"
	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/ledger"
	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/poller"
)

const alice = "0x00000000000000000000000000000000000a11ce"

// fifoQueue creates a FIFO queue that dead-letters after 2 receives, like the Terraform setup.
func fifoQueue(t *testing.T, q *sqs.Client) (main, dlq string) {
	t.Helper()
	ctx := context.Background()
	name := awstest.Name("transfers")
	d, err := q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name + "-dlq.fifo"), Attributes: map[string]string{"FifoQueue": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	attrs, err := q.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: d.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	m, err := q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name + ".fifo"), Attributes: map[string]string{
		"FifoQueue":         "true",
		"VisibilityTimeout": "1",
		"RedrivePolicy":     fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"2"}`, attrs.Attributes["QueueArn"]),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return *m.QueueUrl, *d.QueueUrl
}

func send(t *testing.T, q *sqs.Client, url, dedupID, group, body string) {
	t.Helper()
	if _, err := q.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: &url, MessageBody: &body, MessageGroupId: &group, MessageDeduplicationId: &dedupID,
	}); err != nil {
		t.Fatal(err)
	}
}

func body(eventID, typ string, tx int, value int64) string {
	return fmt.Sprintf(`{"eventId":%q,"type":%q,"chainId":84532,"txHash":"0x%064x","logIndex":0,"blockNumber":%d,
		"blockHash":"0x%064x","from":"0x00000000000000000000000000000000000ca201","to":%q,"value":"%d"}`,
		eventID, typ, tx, tx, 1000+tx, alice, value)
}

func count(t *testing.T, q *sqs.Client, url string) int {
	t.Helper()
	out, err := q.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: &url,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	b, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	return a + b
}

// TestEndToEnd sends events through a real SQS API (ElasticMQ) to DynamoDB Local: a redelivered event
// is credited once, a reorg revert is applied, and a malformed message ends up in the dead-letter queue.
func TestEndToEnd(t *testing.T) {
	ctx := context.Background()
	q := awstest.SQS(t)
	db, table := awstest.Table(t)
	queue, dlq := fifoQueue(t, q)

	send(t, q, queue, "e1", "g1", body("e1", "transfer.confirmed", 1, 1_000_000))
	send(t, q, queue, "e1-again", "g1", body("e1", "transfer.confirmed", 1, 1_000_000)) // redelivered by the relay after SQS's 5-minute dedup window
	send(t, q, queue, "e2", "g2", body("e2", "transfer.confirmed", 2, 500_000))
	send(t, q, queue, "e2-rev", "g2", body("e2-rev", "transfer.reverted", 2, 500_000))
	send(t, q, queue, "poison", "g3", `{"eventId":"broken"}`)

	l := ledger.New(db, table, []string{alice})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := &poller.Poller{SQS: q, QueueURL: queue, Handle: (&handler.Handler{Ledger: l, Log: log}).Handle, Wait: 1, Log: log}

	deadline := time.Now().Add(30 * time.Second)
	for count(t, q, queue) > 0 && time.Now().Before(deadline) {
		if _, err := p.Once(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n := count(t, q, queue); n != 0 {
		t.Fatalf("%d messages left on the queue", n)
	}
	if n := count(t, q, dlq); n != 1 {
		t.Fatalf("dead-letter queue has %d messages, want the 1 poison message", n)
	}
	acc, err := l.Account(ctx, alice)
	if err != nil {
		t.Fatal(err)
	}
	if acc.NetCredited.Int64() != 1_000_000 || acc.Entries != 3 {
		t.Fatalf("alice credited %v over %d entries, want 1000000 over 3", acc.NetCredited, acc.Entries)
	}
}
