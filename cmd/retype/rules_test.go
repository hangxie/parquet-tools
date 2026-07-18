package retype

import "testing"

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
