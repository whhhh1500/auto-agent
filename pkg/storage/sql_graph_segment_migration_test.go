package storage

import (
	"context"
	"database/sql"
	"strconv"
	"testing"

	_ "modernc.org/sqlite"
)

func TestSQLiteGraphSegmentSchemaV39FreshHistoricalRepeatAndFutureRefusal(t *testing.T) {
	for _, tc := range []struct {
		name, marker string
		wantOpen     bool
	}{
		{"fresh", "", true}, {"v38_to_v39", "38", true}, {"future", strconv.Itoa(SQLSchemaVersion + 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("sqlite", "file:graph-segment-v39-"+tc.name+"?mode=memory&cache=shared")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if tc.marker != "" {
				// Bootstrap a valid cumulative pre-v39 database, then rewind only
				// the marker and remove the v39 object to exercise the opener.
				if _, err = OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite); err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec(`DROP TABLE graph_segment_leases`); err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec(`DELETE FROM store_meta WHERE key='schema_version'`); err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec(`INSERT INTO store_meta(key,value) VALUES ('schema_version',?)`, tc.marker); err != nil {
					t.Fatal(err)
				}
			}
			_, err = OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite)
			if tc.wantOpen != (err == nil) {
				t.Fatalf("open err=%v", err)
			}
			var count int
			err = db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='graph_segment_leases'`).Scan(&count)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantOpen && count != 1 {
				t.Fatalf("table count=%d", count)
			}
			if !tc.wantOpen && count != 0 {
				t.Fatalf("future refusal created table")
			}
			if tc.wantOpen {
				expected := []struct {
					name, typ   string
					notnull, pk int
				}{{"tenant_id", "TEXT", 1, 1}, {"session_id", "TEXT", 1, 2}, {"run_id", "TEXT", 1, 3}, {"segment_id", "TEXT", 1, 0}, {"holder_id", "TEXT", 1, 0}, {"host_generation", "BIGINT", 1, 0}, {"expires_at", "BIGINT", 1, 0}, {"released", "INTEGER", 1, 0}, {"created_at", "BIGINT", 1, 0}, {"updated_at", "BIGINT", 1, 0}}
				rows, qerr := db.Query(`SELECT cid,name,type,"notnull",pk FROM pragma_table_info('graph_segment_leases') ORDER BY cid`)
				if qerr != nil {
					t.Fatal(qerr)
				}
				defer rows.Close()
				for i, want := range expected {
					if !rows.Next() {
						t.Fatalf("missing column %d", i)
					}
					var cid int
					var name, typ string
					var notnull, pk int
					if err = rows.Scan(&cid, &name, &typ, &notnull, &pk); err != nil {
						t.Fatal(err)
					}
					if cid != i || name != want.name || typ != want.typ || notnull != want.notnull || pk != want.pk {
						t.Fatalf("column %d=%s/%s/%d/%d", i, name, typ, notnull, pk)
					}
				}
				if rows.Next() {
					t.Fatal("extra column")
				}
				if err = rows.Err(); err != nil {
					t.Fatal(err)
				}
				if _, err = OpenSQLSessionStore(context.Background(), db, SQLDialectSQLite); err != nil {
					t.Fatalf("repeat open=%v", err)
				}
			}
		})
	}
}
