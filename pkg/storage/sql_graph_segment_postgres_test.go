package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresGraphSegmentSchemaV39FreshHistoricalRepeatAndFutureRefusal(t *testing.T) {
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN not configured")
	}
	for _, tc := range []struct {
		name, marker string
		wantOpen     bool
	}{{"fresh", "", true}, {"v38_to_v39", "38", true}, {"future", strconv.Itoa(SQLSchemaVersion + 1), false}} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("pgx", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			schema := fmt.Sprintf("graph_segment_%d", time.Now().UnixNano())
			if _, err = db.Exec(`CREATE SCHEMA ` + schema); err != nil {
				t.Fatal(err)
			}
			defer db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
			if _, err = db.Exec(`SET search_path TO ` + schema); err != nil {
				t.Fatal(err)
			}
			if tc.marker != "" {
				if _, err = OpenSQLSessionStore(context.Background(), db, SQLDialectPostgres); err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec(`DROP TABLE graph_segment_leases`); err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec(`DELETE FROM store_meta WHERE key='schema_version'`); err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec(`INSERT INTO store_meta(key,value) VALUES ('schema_version',$1)`, tc.marker); err != nil {
					t.Fatal(err)
				}
			}
			_, err = OpenSQLSessionStore(context.Background(), db, SQLDialectPostgres)
			if tc.wantOpen != (err == nil) {
				t.Fatalf("open err=%v", err)
			}
			var count int
			err = db.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name='graph_segment_leases'`).Scan(&count)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantOpen && count != 1 {
				t.Fatalf("table count=%d", count)
			}
			if !tc.wantOpen && count != 0 {
				t.Fatal("future refusal created table")
			}
			if tc.wantOpen {
				expected := []struct{ name, typ, nullable, dflt string }{{"tenant_id", "text", "NO", ""}, {"session_id", "text", "NO", ""}, {"run_id", "text", "NO", ""}, {"segment_id", "text", "NO", ""}, {"holder_id", "text", "NO", ""}, {"host_generation", "bigint", "NO", ""}, {"expires_at", "bigint", "NO", ""}, {"released", "integer", "NO", "0"}, {"created_at", "bigint", "NO", ""}, {"updated_at", "bigint", "NO", ""}}
				rows, qerr := db.Query(`SELECT column_name,data_type,is_nullable,COALESCE(column_default,'') FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='graph_segment_leases' ORDER BY ordinal_position`)
				if qerr != nil {
					t.Fatal(qerr)
				}
				defer rows.Close()
				for i, want := range expected {
					if !rows.Next() {
						t.Fatalf("missing column %d", i)
					}
					var name, typ, nullable, dflt string
					if err = rows.Scan(&name, &typ, &nullable, &dflt); err != nil {
						t.Fatal(err)
					}
					if name != want.name || typ != want.typ || nullable != want.nullable || dflt != want.dflt {
						t.Fatalf("column %d=%s/%s/%s", i, name, typ, nullable)
					}
				}
				if rows.Next() {
					t.Fatal("extra column")
				}
				if err = rows.Err(); err != nil {
					t.Fatal(err)
				}
				var pk []string
				rows2, qerr := db.Query(`SELECT a.attname FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum,ord) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum WHERE i.indrelid='graph_segment_leases'::regclass AND i.indisprimary ORDER BY k.ord`)
				if qerr != nil {
					t.Fatal(qerr)
				}
				defer rows2.Close()
				for rows2.Next() {
					var n string
					if err = rows2.Scan(&n); err != nil {
						t.Fatal(err)
					}
					pk = append(pk, n)
				}
				if len(pk) != 3 || pk[0] != "tenant_id" || pk[1] != "session_id" || pk[2] != "run_id" {
					t.Fatalf("primary key=%v", pk)
				}
				if err = rows2.Err(); err != nil {
					t.Fatal(err)
				}
				if _, err = OpenSQLSessionStore(context.Background(), db, SQLDialectPostgres); err != nil {
					t.Fatalf("repeat open=%v", err)
				}
			}
		})
	}
}
