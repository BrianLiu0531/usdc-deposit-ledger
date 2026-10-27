// Package event defines the transfer events published by base-usdc-indexer.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"
)

const (
	Confirmed = "transfer.confirmed" // the transfer reached the indexer's confirmation level
	Reverted  = "transfer.reverted"  // a reorg removed a transfer that had been confirmed
)

// Transfer is the message body. EventID is deterministic: the same event always has the same ID,
// however many times it is delivered.
type Transfer struct {
	EventID     string `json:"eventId"`
	Type        string `json:"type"`
	ChainID     uint64 `json:"chainId"`
	TxHash      string `json:"txHash"`
	LogIndex    uint64 `json:"logIndex"`
	BlockNumber uint64 `json:"blockNumber"`
	BlockHash   string `json:"blockHash"`
	From        string `json:"from"`
	To          string `json:"to"`
	Value       string `json:"value"` // base units as a decimal string (USDC has 6 decimals)
}

var (
	hexAddress = regexp.MustCompile(`^0x[0-9a-f]{40}$`)
	hexHash    = regexp.MustCompile(`^0x[0-9a-f]{64}$`)
)

// ErrInvalid marks a message that can never be processed, however often it is retried.
var ErrInvalid = errors.New("invalid event")

// Parse decodes and validates a message body. Addresses and hashes are normalised to lower case.
func Parse(body string) (Transfer, *big.Int, error) {
	var t Transfer
	dec := json.NewDecoder(strings.NewReader(body))
	if err := dec.Decode(&t); err != nil {
		return t, nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	t.From, t.To = strings.ToLower(t.From), strings.ToLower(t.To)
	t.TxHash, t.BlockHash = strings.ToLower(t.TxHash), strings.ToLower(t.BlockHash)

	var problems []string
	if t.EventID == "" {
		problems = append(problems, "eventId is required")
	}
	if t.Type != Confirmed && t.Type != Reverted {
		problems = append(problems, fmt.Sprintf("unknown type %q", t.Type))
	}
	if !hexAddress.MatchString(t.From) || !hexAddress.MatchString(t.To) {
		problems = append(problems, "from and to must be addresses")
	}
	if !hexHash.MatchString(t.TxHash) || !hexHash.MatchString(t.BlockHash) {
		problems = append(problems, "txHash and blockHash must be 32-byte hashes")
	}
	value, ok := new(big.Int).SetString(t.Value, 10)
	if !ok || value.Sign() < 0 {
		problems = append(problems, "value must be a non-negative integer")
	}
	if len(problems) > 0 {
		return t, nil, fmt.Errorf("%w: %s", ErrInvalid, strings.Join(problems, "; "))
	}
	return t, value, nil
}
