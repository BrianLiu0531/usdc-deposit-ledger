// Package ledger credits USDC transfers to accounts in DynamoDB, exactly once per event.
//
// Single-table design:
//
//	PK                SK                                        item
//	EVENT#<eventId>   EVENT                                     idempotency marker (expires after 30 days)
//	ACCT#<address>    BALANCE                                   netCredited, entries, updatedAt
//	ACCT#<address>    ENTRY#<block>#<logIndex>#<type>#<hash>    one ledger line per event and account
//
// Every event is applied with one TransactWriteItems call that writes the marker on the condition
// that it does not exist yet. A redelivered event fails that condition and changes nothing, so no
// read-then-write race is possible, even when the same message is processed concurrently.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/BrianLiu0531/usdc-deposit-ledger/internal/event"
)

// DB is the subset of the DynamoDB client the ledger uses.
type DB interface {
	TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// Outcome says what Apply did.
type Outcome int

const (
	Applied   Outcome = iota // balances and ledger updated
	Duplicate                // the event was already applied; nothing changed
	Ignored                  // no watched account is involved, or the value is zero
)

func (o Outcome) String() string {
	return [...]string{"applied", "duplicate", "ignored"}[o]
}

// MarkerTTL is how long idempotency markers are kept. It must exceed the queue's retention
// period (SQS allows at most 14 days), after which a message can no longer be redelivered.
const MarkerTTL = 30 * 24 * time.Hour

type Ledger struct {
	db       DB
	table    string
	accounts map[string]bool
	now      func() time.Time
}

// ParseAccounts parses a comma-separated address list into lower-case addresses.
func ParseAccounts(list string) []string {
	var out []string
	for _, a := range strings.Split(list, ",") {
		if a = strings.ToLower(strings.TrimSpace(a)); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// New returns a ledger for the given accounts (lower-case 0x addresses).
func New(db DB, table string, accounts []string) *Ledger {
	m := make(map[string]bool, len(accounts))
	for _, a := range accounts {
		m[a] = true
	}
	return &Ledger{db: db, table: table, accounts: m, now: time.Now}
}

// Apply credits or debits the watched accounts involved in t, at most once per t.EventID.
// A confirmed transfer credits the receiver and debits the sender; a reverted one does the opposite.
// All changes are additions, so events for the same account may be applied in any order.
func (l *Ledger) Apply(ctx context.Context, t event.Transfer, value *big.Int) (Outcome, error) {
	sign := int64(1)
	if t.Type == event.Reverted {
		sign = -1
	}
	deltas := map[string]*big.Int{}
	add := func(account string, v *big.Int) {
		if !l.accounts[account] {
			return
		}
		if deltas[account] == nil {
			deltas[account] = new(big.Int)
		}
		deltas[account].Add(deltas[account], v)
	}
	signed := new(big.Int).Mul(value, big.NewInt(sign))
	add(t.To, signed)
	add(t.From, new(big.Int).Neg(signed)) // a self-transfer nets to zero on one account
	if len(deltas) == 0 || value.Sign() == 0 {
		return Ignored, nil
	}

	now := l.now().UTC()
	items := []types.TransactWriteItem{{
		Put: &types.Put{
			TableName: &l.table,
			Item: map[string]types.AttributeValue{
				"PK":          str("EVENT#" + t.EventID),
				"SK":          str("EVENT"),
				"type":        str(t.Type),
				"processedAt": str(now.Format(time.RFC3339Nano)),
				"expiresAt":   num(strconv.FormatInt(now.Add(MarkerTTL).Unix(), 10)),
			},
			ConditionExpression: aws.String("attribute_not_exists(PK)"),
		},
	}}

	// One ledger line and one balance update per account, in a stable order
	accounts := make([]string, 0, len(deltas))
	for a := range deltas {
		accounts = append(accounts, a)
	}
	sort.Strings(accounts)
	for _, a := range accounts {
		d := deltas[a]
		if len(d.String()) > 38 {
			return 0, fmt.Errorf("%w: amount %s exceeds DynamoDB number precision", event.ErrInvalid, d)
		}
		counterparty := t.From
		if a == t.From {
			counterparty = t.To
		}
		items = append(items, types.TransactWriteItem{
			Put: &types.Put{
				TableName: &l.table,
				Item: map[string]types.AttributeValue{
					"PK":           str("ACCT#" + a),
					"SK":           str(entryKey(t)),
					"eventId":      str(t.EventID),
					"type":         str(t.Type),
					"delta":        num(d.String()),
					"txHash":       str(t.TxHash),
					"blockNumber":  num(strconv.FormatUint(t.BlockNumber, 10)),
					"counterparty": str(counterparty),
					"createdAt":    str(now.Format(time.RFC3339Nano)),
				},
				ConditionExpression: aws.String("attribute_not_exists(PK)"),
			},
		})
		if d.Sign() == 0 {
			continue
		}
		items = append(items, types.TransactWriteItem{
			Update: &types.Update{
				TableName:        &l.table,
				Key:              map[string]types.AttributeValue{"PK": str("ACCT#" + a), "SK": str("BALANCE")},
				UpdateExpression: aws.String("ADD netCredited :d, entries :one SET updatedAt = :now"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":d": num(d.String()), ":one": num("1"), ":now": str(now.Format(time.RFC3339Nano)),
				},
			},
		})
	}

	// The marker's condition is the idempotency guarantee. (A ClientRequestToken would not help: the
	// items carry timestamps, so a concurrent redelivery would fail with a parameter mismatch instead.)
	input := &dynamodb.TransactWriteItemsInput{TransactItems: items}
	for attempt := 1; ; attempt++ {
		_, err := l.db.TransactWriteItems(ctx, input)
		switch reason := cancellation(err); {
		case err == nil:
			return Applied, nil
		case reason == "ConditionalCheckFailed":
			return Duplicate, nil
		case reason == "TransactionConflict" && attempt < maxConflictAttempts:
			// Another invocation is updating the same account's balance item right now. Such conflicts
			// clear within milliseconds, so retry here instead of waiting out the queue's visibility
			// timeout. Each attempt is the whole transaction, so a retry can never apply twice.
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(conflictBackoff(attempt)):
			}
		default:
			return 0, fmt.Errorf("apply %s: %w", t.EventID, err)
		}
	}
}

const maxConflictAttempts = 6

// conflictBackoff is exponential with full jitter: up to 20ms, 40ms, 80ms, ...
func conflictBackoff(attempt int) time.Duration {
	return time.Duration(rand.Int64N(int64(10*time.Millisecond) << attempt))
}

// cancellation returns the first non-"None" reason a transaction was cancelled for, or "".
// Reasons are listed per item; the marker is item 0, so a duplicate reports ConditionalCheckFailed there.
func cancellation(err error) string {
	var cancelled *types.TransactionCanceledException
	if !errors.As(err, &cancelled) {
		return ""
	}
	for _, r := range cancelled.CancellationReasons {
		if code := aws.ToString(r.Code); code != "" && code != "None" {
			return code
		}
	}
	return ""
}

func entryKey(t event.Transfer) string {
	kind := "C"
	if t.Type == event.Reverted {
		kind = "R"
	}
	return fmt.Sprintf("ENTRY#%012d#%06d#%s#%s", t.BlockNumber, t.LogIndex, kind, t.BlockHash[2:14])
}

// Account is an account's running total.
type Account struct {
	NetCredited *big.Int
	Entries     int
}

// Account returns the total credited to address (zero if it has no entries yet).
func (l *Ledger) Account(ctx context.Context, address string) (Account, error) {
	out, err := l.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &l.table,
		Key:            map[string]types.AttributeValue{"PK": str("ACCT#" + address), "SK": str("BALANCE")},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return Account{}, err
	}
	acc := Account{NetCredited: new(big.Int)}
	if v, ok := out.Item["netCredited"].(*types.AttributeValueMemberN); ok {
		acc.NetCredited.SetString(v.Value, 10)
	}
	if v, ok := out.Item["entries"].(*types.AttributeValueMemberN); ok {
		acc.Entries, _ = strconv.Atoi(v.Value)
	}
	return acc, nil
}

// Entry is one ledger line.
type Entry struct {
	EventID, Type, TxHash, Counterparty string
	Delta                               *big.Int
	BlockNumber                         uint64
}

// Entries returns an account's ledger lines, newest first.
func (l *Ledger) Entries(ctx context.Context, address string, limit int32) ([]Entry, error) {
	out, err := l.db.Query(ctx, &dynamodb.QueryInput{
		TableName:              &l.table,
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :entry)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": str("ACCT#" + address), ":entry": str("ENTRY#"),
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(limit),
		ConsistentRead:   aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(out.Items))
	for _, it := range out.Items {
		e := Entry{Delta: new(big.Int)}
		e.EventID, e.Type, e.TxHash, e.Counterparty = s(it["eventId"]), s(it["type"]), s(it["txHash"]), s(it["counterparty"])
		e.Delta.SetString(n(it["delta"]), 10)
		e.BlockNumber, _ = strconv.ParseUint(n(it["blockNumber"]), 10, 64)
		entries = append(entries, e)
	}
	return entries, nil
}

// CreateTable creates the ledger table (used locally and in tests; Terraform owns it in AWS).
func CreateTable(ctx context.Context, db *dynamodb.Client, table string) error {
	_, err := db.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   &table,
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
	})
	var exists *types.ResourceInUseException
	if errors.As(err, &exists) {
		return nil
	}
	return err
}

func str(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func num(v string) types.AttributeValue { return &types.AttributeValueMemberN{Value: v} }

func s(v types.AttributeValue) string {
	if x, ok := v.(*types.AttributeValueMemberS); ok {
		return x.Value
	}
	return ""
}

func n(v types.AttributeValue) string {
	if x, ok := v.(*types.AttributeValueMemberN); ok {
		return x.Value
	}
	return "0"
}
