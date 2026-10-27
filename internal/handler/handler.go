// Package handler processes SQS batches of transfer events.
package handler

import (
	"context"
	"errors"
	"log/slog"
	"math/big"

	"github.com/aws/aws-lambda-go/events"

	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/event"
	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/ledger"
)

// Applier applies one event to the ledger.
type Applier interface {
	Apply(ctx context.Context, t event.Transfer, value *big.Int) (ledger.Outcome, error)
}

type Handler struct {
	Ledger Applier
	Log    *slog.Logger
}

// Handle applies each message and reports the ones that failed, so SQS retries only those
// (the event source mapping must enable ReportBatchItemFailures).
//
// On a FIFO queue, once a message fails, the later messages of the same message group are failed
// too without being attempted, so their order is preserved when they are retried.
// A message that can never succeed keeps failing until the queue moves it to the dead-letter queue.
func (h *Handler) Handle(ctx context.Context, batch events.SQSEvent) (events.SQSEventResponse, error) {
	var resp events.SQSEventResponse
	blocked := map[string]bool{}
	fail := func(r events.SQSMessage, group string) {
		resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: r.MessageId})
		if group != "" {
			blocked[group] = true
		}
	}

	for _, r := range batch.Records {
		group := r.Attributes["MessageGroupId"]
		if group != "" && blocked[group] {
			fail(r, group)
			continue
		}
		t, value, err := event.Parse(r.Body)
		if err != nil {
			h.Log.Error("invalid message; it will be retried and then dead-lettered", "message_id", r.MessageId, "err", err)
			fail(r, group)
			continue
		}
		outcome, err := h.Ledger.Apply(ctx, t, value)
		if err != nil {
			level := slog.LevelWarn
			if errors.Is(err, event.ErrInvalid) {
				level = slog.LevelError
			}
			h.Log.Log(ctx, level, "applying event failed", "event_id", t.EventID, "receive_count", r.Attributes["ApproximateReceiveCount"], "err", err)
			fail(r, group)
			continue
		}
		h.Log.Info("event processed", "event_id", t.EventID, "type", t.Type, "outcome", outcome.String(), "tx", t.TxHash)
	}
	return resp, nil
}
