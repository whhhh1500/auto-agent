package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"strings"
	"time"
)

// Observability watch rules: administrative patterns that mark runs for
// review. A rule matches either a tool name (kind "tool", exact match on the
// invoked capability id) or a keyword (kind "keyword", case-insensitive
// substring over user and assistant message text). Every hit records the
// session and run ids, so the conversation can be pulled up and replayed
// later — the backtest workflow builds on exactly that.
type ObsRule struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Kind      string    `json:"kind"` // tool | keyword
	Pattern   string    `json:"pattern"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
}

// ObsHit is one recorded match.
type ObsHit struct {
	ID        string    `json:"id"`
	Time      time.Time `json:"time"`
	RuleID    string    `json:"rule_id"`
	RuleName  string    `json:"rule_name"`
	Kind      string    `json:"kind"`
	Pattern   string    `json:"pattern"`
	SessionID string    `json:"session_id"`
	RunID     string    `json:"run_id,omitempty"`
	Actor     string    `json:"actor,omitempty"`
	TenantID  string    `json:"tenant,omitempty"`
	Snippet   string    `json:"snippet,omitempty"`
}

// ObsHitFilter narrows a hit query.
type ObsHitFilter struct {
	RuleID    string
	SessionID string
	TenantID  string
	Limit     int
	Offset    int
}

// ObsStore persists watch rules and their hits.
type ObsStore interface {
	CreateRule(ctx context.Context, rule ObsRule) (ObsRule, error)
	UpdateRule(ctx context.Context, rule ObsRule) error
	DeleteRule(ctx context.Context, id string) error
	ListRules(ctx context.Context) ([]ObsRule, error)
	RecordHit(ctx context.Context, hit ObsHit) error
	ListHits(ctx context.Context, filter ObsHitFilter) ([]ObsHit, int, error)
}

var (
	sqlInsertObsRule = sqlQuery{"INSERT INTO obs_rules (id, name, kind, pattern, enabled, created_at) VALUES (?, ?, ?, ?, ?, ?)"}
	sqlUpdateObsRule = sqlQuery{"UPDATE obs_rules SET name = ?, kind = ?, pattern = ?, enabled = ? WHERE id = ?"}
	sqlDeleteObsRule = sqlQuery{"DELETE FROM obs_rules WHERE id = ?"}
	sqlListObsRules  = sqlQuery{"SELECT id, name, kind, pattern, enabled, created_at FROM obs_rules ORDER BY created_at"}
	sqlInsertObsHit  = sqlQuery{"INSERT INTO obs_hits (id, time, rule_id, rule_name, kind, pattern, session_id, run_id, actor, tenant, snippet) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"}
	sqlCountObsRules = sqlQuery{"SELECT COUNT(*) FROM obs_rules"}
	sqlCountObsHits  = sqlQuery{"SELECT COUNT(*) FROM obs_hits"}
)

const (
	MaxObsRules        = 256
	MaxObsHits         = 8192
	MaxObsSnippetBytes = 4096
)

// SQLObsStore implements ObsStore on the shared schema.
type SQLObsStore struct {
	db       *sql.DB
	dialect  SQLDialect
	maxRules int
	maxHits  int
}

func NewSQLObsStore(db *sql.DB, dialect SQLDialect) (*SQLObsStore, error) {
	if db == nil {
		return nil, fmt.Errorf("sql obs store requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return nil, err
	}
	return &SQLObsStore{db: db, dialect: dialect}, nil
}

func (s *SQLObsStore) ruleCap() int {
	if s != nil && s.maxRules > 0 {
		return s.maxRules
	}
	return MaxObsRules
}

func (s *SQLObsStore) hitCap() int {
	if s != nil && s.maxHits > 0 {
		return s.maxHits
	}
	return MaxObsHits
}

func (s *SQLObsStore) CreateRule(ctx context.Context, rule ObsRule) (ObsRule, error) {
	if err := validateObsRule(rule); err != nil {
		return ObsRule{}, err
	}
	if rule.ID == "" {
		id, err := core.NewID("obs_")
		if err != nil {
			return ObsRule{}, err
		}
		rule.ID = id
	}
	if rule.CreatedAt.IsZero() {
		rule.CreatedAt = time.Now().UTC()
	}
	var count int
	if err := s.db.QueryRowContext(ctx, sqlCountObsRules.bind(s.dialect)).Scan(&count); err != nil {
		return ObsRule{}, err
	}
	if count >= s.ruleCap() {
		return ObsRule{}, fmt.Errorf("observability rules exceed maximum of %d", s.ruleCap())
	}
	enabled := 0
	if rule.Enabled {
		enabled = 1
	}
	_, err := s.db.ExecContext(ctx, sqlInsertObsRule.bind(s.dialect),
		rule.ID, rule.Name, rule.Kind, rule.Pattern, enabled, rule.CreatedAt.UnixMilli(),
	)
	return rule, duplicateAsConflict(rule.ID, err)
}

func (s *SQLObsStore) UpdateRule(ctx context.Context, rule ObsRule) error {
	if err := validateObsRule(rule); err != nil {
		return err
	}
	if rule.ID == "" {
		return fmt.Errorf("obs rule id is empty")
	}
	enabled := 0
	if rule.Enabled {
		enabled = 1
	}
	result, err := s.db.ExecContext(ctx, sqlUpdateObsRule.bind(s.dialect),
		rule.Name, rule.Kind, rule.Pattern, enabled, rule.ID,
	)
	if err != nil {
		return err
	}
	return requireAffected(result, "obs rule "+rule.ID+" not found")
}

func (s *SQLObsStore) DeleteRule(ctx context.Context, id string) error {
	if err := validateSQLTextFilter("obs rule id", id); err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("obs rule id is empty")
	}
	result, err := s.db.ExecContext(ctx, sqlDeleteObsRule.bind(s.dialect), id)
	if err != nil {
		return err
	}
	return requireAffected(result, "obs rule "+id+" not found")
}

func (s *SQLObsStore) ListRules(ctx context.Context) ([]ObsRule, error) {
	rows, err := s.db.QueryContext(ctx, sqlListObsRules.bind(s.dialect))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ObsRule{}
	for rows.Next() {
		var rule ObsRule
		var enabled int
		var createdMillis int64
		if err := rows.Scan(&rule.ID, &rule.Name, &rule.Kind, &rule.Pattern, &enabled, &createdMillis); err != nil {
			return nil, err
		}
		rule.Enabled = enabled == 1
		rule.CreatedAt = time.UnixMilli(createdMillis).UTC()
		out = append(out, rule)
	}
	return out, rows.Err()
}

func (s *SQLObsStore) RecordHit(ctx context.Context, hit ObsHit) error {
	if err := validateObsHit(hit); err != nil {
		return err
	}
	if hit.ID == "" {
		id, err := core.NewID("hit_")
		if err != nil {
			return err
		}
		hit.ID = id
	}
	if hit.Time.IsZero() {
		hit.Time = time.Now().UTC()
	}
	var stored int
	if err := s.db.QueryRowContext(ctx, sqlCountObsHits.bind(s.dialect)).Scan(&stored); err != nil {
		return err
	}
	if stored >= s.hitCap() {
		return fmt.Errorf("observability hits exceed maximum of %d", s.hitCap())
	}
	_, err := s.db.ExecContext(ctx, sqlInsertObsHit.bind(s.dialect),
		hit.ID, hit.Time.UnixMilli(), hit.RuleID, hit.RuleName, hit.Kind, hit.Pattern,
		hit.SessionID, hit.RunID, hit.Actor, hit.TenantID, hit.Snippet,
	)
	return err
}

func (s *SQLObsStore) ListHits(ctx context.Context, filter ObsHitFilter) ([]ObsHit, int, error) {
	if err := validateSQLTextFilter("obs rule_id", filter.RuleID); err != nil {
		return nil, 0, err
	}
	if filter.SessionID != "" {
		if err := core.ValidateSessionID(filter.SessionID); err != nil {
			return nil, 0, err
		}
	}
	if err := validateSQLTextFilter("obs tenant_id", filter.TenantID); err != nil {
		return nil, 0, err
	}
	where := []string{}
	args := []any{}
	if filter.RuleID != "" {
		where = append(where, "rule_id = ?")
		args = append(args, filter.RuleID)
	}
	if filter.SessionID != "" {
		where = append(where, "session_id = ?")
		args = append(args, filter.SessionID)
	}
	if filter.TenantID != "" {
		where = append(where, "tenant = ?")
		args = append(args, filter.TenantID)
	}
	clause := " WHERE 1=1"
	for _, condition := range where {
		clause += " AND " + condition
	}
	countQuery := sqlQuery{"SELECT COUNT(*) FROM obs_hits" + clause}
	var total int
	if err := s.db.QueryRowContext(ctx, countQuery.bind(s.dialect), args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	pageQuery := sqlQuery{"SELECT id, time, rule_id, rule_name, kind, pattern, session_id, run_id, actor, tenant, snippet FROM obs_hits" +
		clause + " ORDER BY time DESC LIMIT ? OFFSET ?"}
	pageArgs := append(append([]any(nil), args...), limit, offset)
	rows, err := s.db.QueryContext(ctx, pageQuery.bind(s.dialect), pageArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := []ObsHit{}
	for rows.Next() {
		var hit ObsHit
		var timeMillis int64
		if err := rows.Scan(&hit.ID, &timeMillis, &hit.RuleID, &hit.RuleName, &hit.Kind, &hit.Pattern, &hit.SessionID, &hit.RunID, &hit.Actor, &hit.TenantID, &hit.Snippet); err != nil {
			return nil, 0, err
		}
		hit.Time = time.UnixMilli(timeMillis).UTC()
		out = append(out, hit)
	}
	return out, total, rows.Err()
}

// MatchEvent evaluates the enabled rules against one session event. It is
// the single matching rule set applied by the transport adapter: tool rules
// match tool/call capability ids exactly; keyword rules match user and
// assistant message text case-insensitively. A nil result means no match.
func MatchEvent(rules []ObsRule, event core.SessionEvent) *ObsHit {
	switch event.Type {
	case core.EvToolCall:
		var data core.ToolCallData
		if json.Unmarshal(event.Data, &data) != nil {
			return nil
		}
		for _, rule := range rules {
			if rule.Kind == "tool" && rule.Pattern == data.Name {
				snippet := data.Name
				if encoded, err := json.Marshal(data.Args); err == nil && string(encoded) != "null" {
					snippet += " " + string(encoded)
				}
				return &ObsHit{
					RuleID: rule.ID, RuleName: rule.Name, Kind: rule.Kind, Pattern: rule.Pattern,
					RunID: event.RunID, Snippet: truncateText(snippet, 200),
				}
			}
		}
	case core.EvUserMessage, core.EvAssistantMessage:
		var text string
		if event.Type == core.EvUserMessage {
			var data core.UserMessageData
			if json.Unmarshal(event.Data, &data) == nil {
				text = data.Text
			}
		} else {
			var data core.AssistantMessageData
			if json.Unmarshal(event.Data, &data) == nil {
				text = data.Text
			}
		}
		lower := strings.ToLower(text)
		for _, rule := range rules {
			if rule.Kind == "keyword" && strings.Contains(lower, strings.ToLower(rule.Pattern)) {
				return &ObsHit{
					RuleID: rule.ID, RuleName: rule.Name, Kind: rule.Kind, Pattern: rule.Pattern,
					RunID: event.RunID, Snippet: truncateText(extractSnippet(text, rule.Pattern), 200),
				}
			}
		}
	}
	return nil
}

// extractSnippet returns the text around the first match occurrence.
func extractSnippet(text, pattern string) string {
	index := strings.Index(strings.ToLower(text), strings.ToLower(pattern))
	if index < 0 {
		return truncateText(text, 200)
	}
	start := index - 60
	if start < 0 {
		start = 0
	}
	end := index + len(pattern) + 120
	if end > len(text) {
		end = len(text)
	}
	return text[start:end]
}

func truncateText(text string, n int) string {
	runes := []rune(text)
	if len(runes) <= n {
		return text
	}
	return string(runes[:n]) + "…"
}

func validateObsRule(rule ObsRule) error {
	if strings.TrimSpace(rule.Name) == "" || strings.TrimSpace(rule.Pattern) == "" {
		return fmt.Errorf("obs rule requires name and pattern")
	}
	switch rule.Kind {
	case "tool", "keyword":
	default:
		return fmt.Errorf("obs rule kind must be tool or keyword, got %q", rule.Kind)
	}
	if err := validateSQLTextFilter("obs rule id", rule.ID); err != nil {
		return err
	}
	if err := validateSQLTextFilter("obs rule name", rule.Name); err != nil {
		return err
	}
	if strings.ContainsRune(rule.Pattern, '\x00') {
		return fmt.Errorf("obs rule pattern contains NUL")
	}
	return nil
}

func validateObsHit(hit ObsHit) error {
	if err := validateSQLTextFilter("obs hit id", hit.ID); err != nil {
		return err
	}
	if err := validateSQLTextFilter("obs rule_id", hit.RuleID); err != nil {
		return err
	}
	if err := validateSQLTextFilter("obs rule name", hit.RuleName); err != nil {
		return err
	}
	if hit.Kind != "" && hit.Kind != "tool" && hit.Kind != "keyword" {
		return fmt.Errorf("obs hit kind must be tool or keyword, got %q", hit.Kind)
	}
	if strings.ContainsRune(hit.Pattern, '\x00') || strings.ContainsRune(hit.Snippet, '\x00') {
		return fmt.Errorf("obs hit pattern or snippet contains NUL")
	}
	if len(hit.Snippet) > MaxObsSnippetBytes {
		return fmt.Errorf("obs hit snippet exceeds %d bytes", MaxObsSnippetBytes)
	}
	if hit.SessionID != "" {
		if err := core.ValidateSessionID(hit.SessionID); err != nil {
			return err
		}
	}
	if hit.RunID != "" {
		if err := core.ValidateRunID(hit.RunID); err != nil {
			return err
		}
	}
	if err := validateSQLTextFilter("obs actor", hit.Actor); err != nil {
		return err
	}
	return validateSQLTextFilter("obs tenant_id", hit.TenantID)
}
