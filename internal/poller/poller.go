// Package poller feeds SQS messages to the handler outside Lambda, e.g. against ElasticMQ locally.
// It reproduces what the Lambda event source mapping does: receive, invoke, delete the successes.
package poller

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// SQS is the subset of the SQS client the poller uses.
type SQS interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
}

type HandleFunc func(context.Context, events.SQSEvent) (events.SQSEventResponse, error)

type Poller struct {
	SQS      SQS
	QueueURL string
	Handle   HandleFunc
	Wait     int32 // long-poll seconds
	Log      *slog.Logger
}

// Run polls until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if _, err := p.Once(ctx); err != nil && ctx.Err() == nil {
			p.Log.Warn("poll failed", "err", err)
			time.Sleep(2 * time.Second)
		}
	}
	return ctx.Err()
}

// Once receives one batch, handles it and deletes the messages that succeeded.
// It returns the number of messages received.
func (p *Poller) Once(ctx context.Context) (int, error) {
	out, err := p.SQS.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    &p.QueueURL,
		MaxNumberOfMessages:         10,
		WaitTimeSeconds:             p.Wait,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	})
	if err != nil || len(out.Messages) == 0 {
		return 0, err
	}
	batch := events.SQSEvent{Records: make([]events.SQSMessage, len(out.Messages))}
	for i, m := range out.Messages {
		batch.Records[i] = events.SQSMessage{MessageId: aws.ToString(m.MessageId), Body: aws.ToString(m.Body), Attributes: m.Attributes}
	}
	resp, err := p.Handle(ctx, batch)
	if err != nil {
		return len(out.Messages), err // nothing is deleted; every message becomes visible again
	}
	failed := map[string]bool{}
	for _, f := range resp.BatchItemFailures {
		failed[f.ItemIdentifier] = true
	}
	var done []types.DeleteMessageBatchRequestEntry
	for _, m := range out.Messages {
		if !failed[aws.ToString(m.MessageId)] {
			done = append(done, types.DeleteMessageBatchRequestEntry{Id: m.MessageId, ReceiptHandle: m.ReceiptHandle})
		}
	}
	if len(done) > 0 {
		if _, err := p.SQS.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: &p.QueueURL, Entries: done}); err != nil {
			return len(out.Messages), err
		}
	}
	return len(out.Messages), nil
}
