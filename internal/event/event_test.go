package event

import (
	"errors"
	"strings"
	"testing"
)

const valid = `{"eventId":"transfer.confirmed:84532:0xabc:1:0xdef","type":"transfer.confirmed","chainId":84532,
"txHash":"0xAAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","logIndex":1,"blockNumber":7,
"blockHash":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
"from":"0x00000000000000000000000000000000000CA201","to":"0x00000000000000000000000000000000000a11ce","value":"2500000"}`

func TestParseNormalisesAndValidates(t *testing.T) {
	tr, v, err := Parse(valid)
	if err != nil {
		t.Fatal(err)
	}
	if v.Int64() != 2_500_000 || tr.From != "0x00000000000000000000000000000000000ca201" || !strings.HasPrefix(tr.TxHash, "0xaa") {
		t.Fatalf("parsed %+v %v", tr, v)
	}
}

func TestParseRejectsBadMessages(t *testing.T) {
	for name, body := range map[string]string{
		"not json":       `{"eventId":`,
		"unknown type":   strings.Replace(valid, `"type":"transfer.confirmed"`, `"type":"transfer.mystery"`, 1),
		"no event id":    strings.Replace(valid, `"eventId":"transfer.confirmed:84532:0xabc:1:0xdef",`, "", 1),
		"bad address":    strings.Replace(valid, "0x00000000000000000000000000000000000a11ce", "alice", 1),
		"negative value": strings.Replace(valid, `"2500000"`, `"-1"`, 1),
		"decimal value":  strings.Replace(valid, `"2500000"`, `"2.5"`, 1),
	} {
		if _, _, err := Parse(body); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %v, want ErrInvalid", name, err)
		}
	}
}
