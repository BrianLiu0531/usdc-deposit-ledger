package ledger

import (
	"context"
	"math/big"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/event"
)

// conflictingDB cancels the first n transactions with the given per-item reasons.
type conflictingDB struct {
	DB
	n       int
	reasons []string
	calls   int
}

func (c *conflictingDB) TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	c.calls++
	if c.calls <= c.n {
		ex := &types.TransactionCanceledException{Message: aws.String("Transaction cancelled")}
		for _, r := range c.reasons {
			ex.CancellationReasons = append(ex.CancellationReasons, types.CancellationReason{Code: aws.String(r)})
		}
		return nil, ex
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func deposit() (event.Transfer, *big.Int) {
	return event.Transfer{
		EventID: "e1", Type: event.Confirmed, TxHash: "0x01", BlockHash: "0x00000000000000000000",
		From: "0xca201", To: "0xa11ce", Value: "5",
	}, big.NewInt(5)
}

func TestRetriesTransactionConflicts(t *testing.T) {
	// Reasons are per item: the marker was fine, the balance update (item 2) conflicted
	db := &conflictingDB{n: 2, reasons: []string{"None", "None", "TransactionConflict"}}
	ev, v := deposit()
	out, err := New(db, "t", []string{"0xa11ce"}).Apply(context.Background(), ev, v)
	if err != nil || out != Applied || db.calls != 3 {
		t.Fatalf("got %v, %v after %d calls; want Applied after 3", out, err, db.calls)
	}
}

func TestGivesUpOnPersistentConflicts(t *testing.T) {
	db := &conflictingDB{n: 100, reasons: []string{"None", "TransactionConflict"}}
	ev, v := deposit()
	if _, err := New(db, "t", []string{"0xa11ce"}).Apply(context.Background(), ev, v); err == nil || db.calls != maxConflictAttempts {
		t.Fatalf("got %v after %d calls; want an error after %d (SQS then redelivers)", err, db.calls, maxConflictAttempts)
	}
}

func TestDuplicateIsDetectedWhicheverItemFailsTheCondition(t *testing.T) {
	db := &conflictingDB{n: 1, reasons: []string{"ConditionalCheckFailed", "None", "None"}}
	ev, v := deposit()
	if out, err := New(db, "t", []string{"0xa11ce"}).Apply(context.Background(), ev, v); err != nil || out != Duplicate || db.calls != 1 {
		t.Fatalf("got %v, %v after %d calls; want Duplicate after 1", out, err, db.calls)
	}
}
