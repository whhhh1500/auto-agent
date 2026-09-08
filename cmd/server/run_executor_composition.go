package main

import (
	"database/sql"
	"fmt"

	graphapproval "github.com/whhhh1500/auto-agent/pkg/adapter/graphapproval"
	graphtelemetry "github.com/whhhh1500/auto-agent/pkg/adapter/graphtelemetry"
	graphadapter "github.com/whhhh1500/auto-agent/pkg/adapter/runexecutor/graph"
	graphcheckpoint "github.com/whhhh1500/auto-agent/pkg/adapter/sql/graphcheckpoint"
	graphsegment "github.com/whhhh1500/auto-agent/pkg/adapter/sql/graphsegment"
	sqlkit "github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	runexecutor "github.com/whhhh1500/auto-agent/pkg/app/runexecutor"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

func newRunExecutorRegistry(db *sql.DB, dialect storage.SQLDialect, approvals graphapproval.ApprovalReader, telemetry core.Telemetry) (*runexecutor.Registry, error) {
	var d sqlkit.Dialect
	switch dialect {
	case storage.SQLDialectSQLite:
		d = sqlkit.SQLite
	case storage.SQLDialectPostgres:
		d = sqlkit.Postgres
	default:
		return nil, fmt.Errorf("unsupported SQL dialect")
	}
	checkpoints, err := graphcheckpoint.New(db, d)
	if err != nil {
		return nil, err
	}
	authority, err := graphsegment.New(db, d)
	if err != nil {
		return nil, err
	}
	durable, err := graphapproval.New(graphapproval.Options{Approvals: approvals, Checkpoints: checkpoints})
	if err != nil {
		return nil, err
	}
	def, err := graphadapter.NewDefaultDefinition()
	if err != nil {
		return nil, err
	}
	gr, err := graphadapter.Registration(graphadapter.Options{Definition: def, Store: checkpoints, Authority: authority, Planner: graphadapter.ReferencePlanner{}, Approval: durable, Decisions: durable, Observer: graphtelemetry.Observer{Telemetry: telemetry}})
	if err != nil {
		return nil, err
	}
	seq, err := runexecutor.SequentialRegistration()
	if err != nil {
		return nil, err
	}
	return runexecutor.NewRegistry(runexecutor.DefaultMaxExecutors, seq, gr)
}
