package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/aws/aws-lambda-go/events"

	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/event"
	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/ledger"
)

type fakeLedger struct {
	failFor map[string]error
	applied []string
}

func (f *fakeLedger) Apply(_ context.Context, t event.Transfer, _ *big.Int) (ledger.Outcome, error) {
	if err := f.failFor[t.EventID]; err != nil {
		return 0, err
	}
	f.applied = append(f.applied, t.EventID)
	return ledger.Applied, nil
}

func msg(id, eventID, group string) events.SQSMessage {
	body := fmt.Sprintf(`{"eventId":%q,"type":"transfer.confirmed","chainId":84532,"txHash":"0x%064x","logIndex":0,"blockNumber":1,
		"blockHash":"0x%064x","from":"0x%040x","to":"0x%040x","value":"5"}`, eventID, 1, 2, 3, 4)
	return events.SQSMessage{MessageId: id, Body: body, Attributes: map[string]string{"MessageGroupId": group}}
}

func failures(r events.SQSEventResponse) []string {
	var ids []string
	for _, f := range r.BatchItemFailures {
		ids = append(ids, f.ItemIdentifier)
	}
	return ids
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestReportsOnlyFailedMessages(t *testing.T) {
	l := &fakeLedger{failFor: map[string]error{"e2": errors.New("throttled")}}
	h := &Handler{Ledger: l, Log: quiet()}
	bad := events.SQSMessage{MessageId: "m4", Body: `{"type":"transfer.confirmed"}`, Attributes: map[string]string{"MessageGroupId": "g4"}}

	resp, err := h.Handle(context.Background(), events.SQSEvent{Records: []events.SQSMessage{
		msg("m1", "e1", "g1"), msg("m2", "e2", "g2"), msg("m3", "e3", "g3"), bad,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(failures(resp)); got != "[m2 m4]" {
		t.Fatalf("failures %s, want [m2 m4]", got)
	}
	if got := fmt.Sprint(l.applied); got != "[e1 e3]" {
		t.Fatalf("applied %s", got)
	}
}

func TestKeepsFIFOOrderWithinAGroup(t *testing.T) {
	l := &fakeLedger{failFor: map[string]error{"e1": errors.New("conflict")}}
	h := &Handler{Ledger: l, Log: quiet()}

	// e1 fails, so e2 (same group, later) must not be applied before e1 is retried; e3 is unaffected
	resp, _ := h.Handle(context.Background(), events.SQSEvent{Records: []events.SQSMessage{
		msg("m1", "e1", "transfer-a"), msg("m2", "e2", "transfer-a"), msg("m3", "e3", "transfer-b"),
	}})
	if got := fmt.Sprint(failures(resp)); got != "[m1 m2]" {
		t.Fatalf("failures %s, want [m1 m2]", got)
	}
	if got := fmt.Sprint(l.applied); got != "[e3]" {
		t.Fatalf("applied %s, want [e3]", got)
	}
}
