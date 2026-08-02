package retype

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetActiveRulesOrder(t *testing.T) {
	cmd := Cmd{Int96ToTimestamp: true, GeoToBinary: true}
	rules := cmd.getActiveRules()
	if len(rules) != 2 {
		t.Fatalf("getActiveRules() returned %d rules, want 2", len(rules))
	}
	if rules[0] != RuleRegistry[RuleInt96ToTimestamp] || rules[1] != RuleRegistry[RuleGeoToBinary] {
		t.Fatal("getActiveRules() did not preserve registry order")
	}
}

func TestRuleConvertersRejectInvalidInput(t *testing.T) {
	testCases := map[string]struct {
		rule  RuleID
		value any
		err   string
	}{
		"float16-type": {
			rule:  RuleFloat16ToFloat32,
			value: 1,
			err:   "expected string for FLOAT16",
		},
		"variant-json": {
			rule:  RuleVariantToString,
			value: func() {},
			err:   "failed to marshal VARIANT to JSON",
		},
		"uuid-type": {
			rule:  RuleUuidToString,
			value: 1,
			err:   "expected string for UUID",
		},
		"repeated-type": {
			rule:  RuleRepeatedToList,
			value: 1,
			err:   "expected slice for repeated field",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			value, err := RuleRegistry[tc.rule].ConvertData(tc.value)
			require.Nil(t, value)
			require.ErrorContains(t, err, tc.err)
		})
	}
}
