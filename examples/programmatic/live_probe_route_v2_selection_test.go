package main

import (
	"testing"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestV2LiveEvidenceKeepsVerifiedRouteWhenFinalAnswerFails(t *testing.T) {
	for _, route := range []executionroute.SelectedRoute{
		executionroute.SelectedRouteDirect,
		executionroute.SelectedRoutePTC,
	} {
		t.Run(string(route), func(t *testing.T) {
			record := v2LiveEvidenceFromRun(
				v2LiveArm{name: "auto_probe_once", routeMode: programmatic.RouteAutoProbeOnce},
				"test-model",
				core.TurnResult{Status: core.RunCompleted, Answer: "wrong final answer"},
				nil, nil, nil,
				executionroute.SelectedRouteObservation{Route: route, Status: executionroute.SelectionStatusVerified},
				false, nil,
			)
			if record.SelectedRoute != string(route) || record.SelectionStatus != string(executionroute.SelectionStatusVerified) {
				t.Fatalf("route observation was lost: %+v", record)
			}
			if record.Coverage != (v2LiveCoverageEvidence{CoverageStatus: v2LiveCoverageUnavailable}) {
				t.Fatalf("coverage without a frozen fixture=%+v", record.Coverage)
			}
			if record.AcceptancePassed || record.FailureCategory != v2LiveFailureAcceptance || record.Effects.AnswerMatches {
				t.Fatalf("wrong answer did not remain a separate quality failure: %+v", record)
			}
		})
	}
}
