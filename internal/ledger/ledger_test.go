package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/awstest"
	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/event"
	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/ledger"
)

const (
	alice = "0x00000000000000000000000000000000000a11ce" // watched
	bob   = "0x0000000000000000000000000000000000000b0b" // watched
	carol = "0x00000000000000000000000000000000000ca201"
)

// transfer builds an event the way the indexer does, with a deterministic ID.
func transfer(typ string, tx, block int, from, to string, value int64) (event.Transfer, *big.Int) {
	t := event.Transfer{
		Type: typ, ChainID: 84532, TxHash: fmt.Sprintf("0x%064x", tx), LogIndex: 0,
		BlockNumber: uint64(block), BlockHash: fmt.Sprintf("0x%064x", 1_000_000+block),
		From: from, To: to, Value: fmt.Sprint(value),
	}
	t.EventID = fmt.Sprintf("%s:%d:%s:%d:%s", t.Type, t.ChainID, t.TxHash, t.LogIndex, t.BlockHash)
	return t, big.NewInt(value)
}

func setup(t *testing.T) *ledger.Ledger {
	db, table := awstest.Table(t)
	return ledger.New(db, table, []string{alice, bob})
}

func balance(t *testing.T, l *ledger.Ledger, account string) (int64, int) {
	t.Helper()
	acc, err := l.Account(context.Background(), account)
	if err != nil {
		t.Fatal(err)
	}
	return acc.NetCredited.Int64(), acc.Entries
}

func TestCreditsOnceWhenRedelivered(t *testing.T) {
	ctx := context.Background()
	l := setup(t)
	ev, v := transfer(event.Confirmed, 1, 10, carol, alice, 2_500_000)

	for i, want := range []ledger.Outcome{ledger.Applied, ledger.Duplicate, ledger.Duplicate} {
		got, err := l.Apply(ctx, ev, v)
		if err != nil || got != want {
			t.Fatalf("delivery %d: %v, %v (want %v)", i+1, got, err, want)
		}
	}
	if bal, n := balance(t, l, alice); bal != 2_500_000 || n != 1 {
		t.Fatalf("balance %d over %d entries, want 2500000 over 1", bal, n)
	}
}

// TestConcurrentDuplicatesApplyOnce delivers the same event to 20 workers at once — the case where a
// read-then-write check lets several through and the account is credited several times.
func TestConcurrentDuplicatesApplyOnce(t *testing.T) {
	l := setup(t)
	ev, v := transfer(event.Confirmed, 2, 11, carol, alice, 100)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		applied int
	)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				out, err := l.Apply(context.Background(), ev, v)
				if err != nil { // e.g. TransactionConflict: SQS would redeliver, so retry
					continue
				}
				mu.Lock()
				if out == ledger.Applied {
					applied++
				}
				mu.Unlock()
				return
			}
		}()
	}
	wg.Wait()
	if applied != 1 {
		t.Fatalf("applied %d times, want exactly 1", applied)
	}
	if bal, _ := balance(t, l, alice); bal != 100 {
		t.Fatalf("balance %d, want 100", bal)
	}
}

func TestRevertsInAnyOrder(t *testing.T) {
	ctx := context.Background()
	l := setup(t)
	confirmed, v := transfer(event.Confirmed, 3, 12, carol, alice, 700)
	reverted, _ := transfer(event.Reverted, 3, 12, carol, alice, 700)
	reincluded, _ := transfer(event.Confirmed, 3, 14, carol, alice, 700) // same tx, new block after the reorg

	// The revert may arrive before the credit it undoes; the totals still converge
	for _, ev := range []event.Transfer{reverted, confirmed, reincluded, reverted} {
		if _, err := l.Apply(ctx, ev, v); err != nil {
			t.Fatal(err)
		}
	}
	if bal, n := balance(t, l, alice); bal != 700 || n != 3 {
		t.Fatalf("balance %d over %d entries, want 700 over 3", bal, n)
	}
}

func TestDebitsSendersAndIgnoresStrangers(t *testing.T) {
	ctx := context.Background()
	l := setup(t)
	steps := []struct {
		ev   event.Transfer
		v    *big.Int
		want ledger.Outcome
	}{}
	add := func(typ string, tx int, from, to string, value int64, want ledger.Outcome) {
		ev, v := transfer(typ, tx, 20+tx, from, to, value)
		steps = append(steps, struct {
			ev   event.Transfer
			v    *big.Int
			want ledger.Outcome
		}{ev, v, want})
	}
	add(event.Confirmed, 1, carol, alice, 1000, ledger.Applied) // deposit
	add(event.Confirmed, 2, alice, bob, 300, ledger.Applied)    // internal: both sides change
	add(event.Confirmed, 3, alice, alice, 50, ledger.Applied)   // self: recorded, net zero
	add(event.Confirmed, 4, carol, carol, 9, ledger.Ignored)    // not ours
	add(event.Confirmed, 5, carol, alice, 0, ledger.Ignored)    // zero value
	for i, s := range steps {
		if got, err := l.Apply(ctx, s.ev, s.v); err != nil || got != s.want {
			t.Fatalf("step %d: %v, %v (want %v)", i, got, err, s.want)
		}
	}
	if bal, _ := balance(t, l, alice); bal != 700 {
		t.Fatalf("alice %d, want 700", bal)
	}
	if bal, _ := balance(t, l, bob); bal != 300 {
		t.Fatalf("bob %d, want 300", bal)
	}
	entries, err := l.Entries(ctx, alice, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 || entries[0].BlockNumber != 23 || entries[0].Delta.Sign() != 0 || entries[1].Delta.Int64() != -300 || entries[1].Counterparty != bob {
		t.Fatalf("entries %+v", entries)
	}
}

func TestRejectsAmountsBeyondDynamoDBPrecision(t *testing.T) {
	l := setup(t)
	ev, _ := transfer(event.Confirmed, 9, 30, carol, alice, 1)
	huge, _ := new(big.Int).SetString("1"+fmt.Sprintf("%039d", 0), 10)
	if _, err := l.Apply(context.Background(), ev, huge); !errors.Is(err, event.ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}

// TestConcurrentEventsForOneAccount credits 20 different deposits to the same account at once, as
// parallel Lambda invocations do; transaction conflicts on the balance item must be retried, not failed.
func TestConcurrentEventsForOneAccount(t *testing.T) {
	l := setup(t)
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev, v := transfer(event.Confirmed, 100+i, 50+i, carol, alice, 10)
			if out, err := l.Apply(context.Background(), ev, v); err != nil || out != ledger.Applied {
				errs <- fmt.Errorf("event %d: %v, %v", i, out, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if bal, n := balance(t, l, alice); bal != 200 || n != 20 {
		t.Fatalf("balance %d over %d entries, want 200 over 20", bal, n)
	}
}
