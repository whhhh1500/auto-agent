package contextassembly

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	// DefaultExtractiveSummaryMaxBytes is the hard durable output budget used
	// by the default server composition. It deliberately leaves ample room for
	// the enclosing context/summary event and later model-context assembly.
	DefaultExtractiveSummaryMaxBytes        = 12 << 10
	DefaultExtractiveSummaryMaxMessageBytes = 1024
	DefaultExtractiveSummaryMaxToolBytes    = 2048
	maxExtractiveSourceMessages             = 4096
	maxExtractiveSummaryItems               = 4
	minimumExtractiveSummaryBytes           = 512
)

// ExtractiveSummarizerConfig controls the fixed local extraction budget. Zero
// values select the bounded defaults; values cannot enlarge the durable
// summary past DefaultExtractiveSummaryMaxBytes.
type ExtractiveSummarizerConfig struct {
	MaxBytes        int
	MaxMessageBytes int
	MaxToolBytes    int
}

// ExtractiveSummarizer is a deterministic ContextSummarizer that does not
// call a model. It records only source material that is already present in the
// projected history, labels omitted material, and never invents conclusions.
//
// It is intentionally a small default. Deployments that need semantic
// compression can replace it through contextassembly.ContextSummarizer.
type ExtractiveSummarizer struct {
	maxBytes, maxMessageBytes, maxToolBytes int
}

// NewExtractiveSummarizer creates a bounded deterministic summarizer.
func NewExtractiveSummarizer(config ExtractiveSummarizerConfig) (*ExtractiveSummarizer, error) {
	if config.MaxBytes == 0 {
		config.MaxBytes = DefaultExtractiveSummaryMaxBytes
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = DefaultExtractiveSummaryMaxMessageBytes
	}
	if config.MaxToolBytes == 0 {
		config.MaxToolBytes = DefaultExtractiveSummaryMaxToolBytes
	}
	if config.MaxBytes < minimumExtractiveSummaryBytes || config.MaxBytes > DefaultExtractiveSummaryMaxBytes ||
		config.MaxMessageBytes <= 0 || config.MaxMessageBytes > config.MaxBytes ||
		config.MaxToolBytes <= 0 || config.MaxToolBytes > config.MaxBytes {
		return nil, fmt.Errorf("%w: extractive summarizer configuration", ErrInvalid)
	}
	return &ExtractiveSummarizer{maxBytes: config.MaxBytes, maxMessageBytes: config.MaxMessageBytes, maxToolBytes: config.MaxToolBytes}, nil
}

type extractiveTool struct {
	id, name string
	args     map[string]any
	result   *core.ChatMessage
}

type extractiveUserMessage struct {
	sourceIndex int
	message     core.ChatMessage
}

// Summarize emits existing durable summaries, the latest user goals, and tool
// call/result evidence first. It then uses any remaining bounded envelope for
// earlier user records in source order. It walks the source once and only
// writes bounded fragments, so a giant transcript or tool result is never
// concatenated.
func (s *ExtractiveSummarizer) Summarize(ctx context.Context, messages []core.ChatMessage) (string, error) {
	if s == nil || ctx == nil || len(messages) == 0 || len(messages) > maxExtractiveSourceMessages {
		return "", ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	summaries := make([]core.ChatMessage, 0, maxExtractiveSummaryItems)
	users := make([]extractiveUserMessage, 0, maxExtractiveSummaryItems)
	tools := make([]extractiveTool, 0)
	byID := make(map[string]int)
	unpairedResults := make([]core.ChatMessage, 0)
	// nested tracks protected child results by their active model-visible
	// parent. They are audit evidence, not a second result the model should see;
	// the parent result is the only model-facing program outcome. A nested
	// result without a later parent completion is unsafe to summarize because
	// it can make an interrupted program look complete.
	nested := make(map[string]bool)
	for i := range messages {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		message := messages[i]
		if message.Provenance != nil && message.Provenance.Kind == "summary" {
			summaries = appendLatest(summaries, message, maxExtractiveSummaryItems)
			continue
		}
		if message.Role == core.RoleUser {
			users = append(users, extractiveUserMessage{sourceIndex: i, message: message})
		}
		if message.Role == core.RoleAssistant {
			calls := message.ToolCalls
			if message.ToolCall != nil {
				calls = append([]core.ToolCall{*message.ToolCall}, calls...)
			}
			for _, call := range calls {
				if call.ID == "" || call.Name == "" {
					continue
				}
				if _, exists := byID[call.ID]; exists {
					continue
				}
				byID[call.ID] = len(tools)
				tools = append(tools, extractiveTool{id: call.ID, name: call.Name, args: call.Args})
			}
		}
		if message.Role == core.RoleTool {
			if index, exists := byID[message.ToolCallID]; exists && tools[index].result == nil {
				copyOf := message
				tools[index].result = &copyOf
				delete(byID, message.ToolCallID) // IDs can be reused after a completed call, across turns.
				delete(nested, message.ToolCallID)
				continue
			}
			parent, found := activeToolParent(message.ToolCallID, byID)
			if !found {
				if strings.Contains(message.ToolCallID, "/") {
					return "", ErrInvalid
				}
				unpairedResults = append(unpairedResults, message)
				continue
			}
			nested[parent] = true
		}
	}
	if len(nested) != 0 {
		return "", ErrInvalid
	}

	// Reserve a small suffix for the explicit omission declaration before any
	// source content is copied. This makes the output cap exact even when every
	// selected record would otherwise fill the builder.
	reserve := 192
	builder := boundedSummaryBuilder{limit: s.maxBytes - reserve, hardLimit: s.maxBytes}
	builder.record(fmt.Sprintf("[extractive summary source_messages=%d]", len(messages)))
	recentStart := max(0, len(users)-maxExtractiveSummaryItems)
	// Build recent user records once before the prior-summary pass. In
	// particular, messageValue may hash an oversized current update; caching
	// means the reservation calculation and the later write cannot repeat that
	// work or produce different evidence.
	recentRecords := make([]string, 0, len(users)-recentStart)
	recentReservedBytes := 0
	for _, user := range users[recentStart:] {
		record := "recent_user_goal_or_constraint: " + s.messageValue(user.message.Content, s.maxMessageBytes)
		recentRecords = append(recentRecords, record)
		// canFitRecordLength deliberately accounts for a newline even for the
		// first record. Use the same conservative accounting for the
		// reservation so every cached record that collectively fits is still
		// eligible after the prior-summary pass.
		recentReservedBytes += len(record) + 1
	}

	// Preserve the established output order (prior summary before recent user
	// records), while preventing a nearly-full prior summary -- including its
	// omission marker -- from consuming all room needed by current updates.
	// If the current records alone exceed the envelope, leave the temporary
	// limit at the header and restore the normal "write what fits" behavior
	// below rather than growing the 12 KiB cap or dropping all current records.
	originalLimit := builder.limit
	builder.limit = max(builder.builder.Len(), originalLimit-recentReservedBytes)
	for _, message := range summaries {
		// A prior durable summary has a different budget from a single raw
		// message. Preserve it whole when it fits, otherwise label its omission.
		remaining := builder.limit - builder.builder.Len() - len("prior_summary: ") - 1
		builder.record("prior_summary: " + s.messageValue(message.Content, remaining))
	}
	builder.limit = originalLimit
	for _, record := range recentRecords {
		builder.record(record)
	}
	for _, tool := range tools {
		call := "tool_call id=" + s.identifier(tool.id) + " name=" + s.identifier(tool.name)
		if tool.args != nil {
			call += " args=" + s.toolArgs(tool.args)
		}
		if tool.result == nil {
			builder.record(call + " status=unresolved")
			continue
		}
		// Keep the call and its result as one record: a bounded summary either
		// retains the causal pair or labels the whole pair omitted, never leaves
		// a dangling call that appears unresolved when it was not.
		builder.record(call + "\ntool_result id=" + s.identifier(tool.result.ToolCallID) + " result=" + s.messageValue(tool.result.Content, s.maxToolBytes))
	}
	for _, result := range unpairedResults {
		builder.record("tool_result id=" + s.identifier(result.ToolCallID) + " status=unpaired result=" + s.messageValue(result.Content, s.maxToolBytes))
	}
	// The recent goals, existing summaries, and tool causal evidence above are
	// the fixed priority. Any remaining bounded envelope is then used for older
	// user records in original source order. The label makes their historical
	// position explicit instead of presenting them as current instructions.
	omittedEarlierUsers := 0
	for _, user := range users[:recentStart] {
		prefix := "earlier_user_history source_index=" + strconv.Itoa(user.sourceIndex) + ": "
		content := user.message.Content
		// Earlier records are opportunistic. Never hash or scan an oversized
		// source merely to discover that it cannot be retained: that would turn
		// a full historical prefix into unbounded CPU work after the envelope is
		// already full. The established recent-user and tool paths keep their
		// existing evidence/hash behavior.
		if len(content) > s.maxMessageBytes || !builder.canFitRecordLength(len(prefix)+len(content)) || !utf8.ValidString(content) {
			omittedEarlierUsers++
			continue
		}
		if !builder.tryRecord(prefix + content) {
			// canFitRecordLength above makes this unreachable without a future
			// builder change, but retain fail-closed omission accounting.
			omittedEarlierUsers++
		}
	}
	if builder.omitted > 0 || omittedEarlierUsers > 0 {
		builder.forceRecord(fmt.Sprintf("[extractive omitted_records=%d omitted_earlier_users=%d]", builder.omitted, omittedEarlierUsers))
	}
	if builder.builder.Len() == 0 {
		return "", ErrInvalid
	}
	return builder.builder.String(), nil
}

func activeToolParent(callID string, active map[string]int) (string, bool) {
	parent := callID
	for slash := strings.LastIndexByte(parent, '/'); slash > 0; slash = strings.LastIndexByte(parent, '/') {
		parent = parent[:slash]
		if _, found := active[parent]; found {
			return parent, true
		}
	}
	return "", false
}

func (s *ExtractiveSummarizer) toolArgs(args map[string]any) string {
	budget, nodes := s.maxMessageBytes/6, 2048
	if !summaryJSONFits(args, &budget, &nodes, 0) {
		return "[omitted arguments]"
	}
	encoded, err := json.Marshal(args)
	if err != nil || len(encoded) > s.maxMessageBytes {
		return "[omitted arguments]"
	}
	return string(encoded)
}

func appendLatest(messages []core.ChatMessage, message core.ChatMessage, limit int) []core.ChatMessage {
	if len(messages) < limit {
		return append(messages, message)
	}
	copy(messages, messages[1:])
	messages[len(messages)-1] = message
	return messages
}

func (s *ExtractiveSummarizer) messageValue(value string, limit int) string {
	if utf8.ValidString(value) && len(value) <= limit {
		return value
	}
	sum := sha256Prefix(value)
	return fmt.Sprintf("[omitted bytes=%d sha256=%s]", len(value), hex.EncodeToString(sum))
}

func sha256Prefix(value string) []byte {
	hash := sha256.New()
	// Copy through a small fixed buffer rather than converting the whole string
	// to []byte. A 1 MiB tool result therefore never causes a second 1 MiB
	// allocation merely to compute its evidence fingerprint.
	var buffer [4096]byte
	for len(value) > 0 {
		n := len(value)
		if n > len(buffer) {
			n = len(buffer)
		}
		copy(buffer[:n], value[:n])
		_, _ = hash.Write(buffer[:n])
		value = value[n:]
	}
	return hash.Sum(nil)[:8]
}

func (s *ExtractiveSummarizer) identifier(value string) string {
	return s.messageValue(value, s.maxMessageBytes)
}

type boundedSummaryBuilder struct {
	builder          strings.Builder
	limit, hardLimit int
	omitted          int
}

func (b *boundedSummaryBuilder) record(value string) {
	if !b.tryRecord(value) {
		b.omitted++
	}
}

func (b *boundedSummaryBuilder) tryRecord(value string) bool {
	if value == "" || !b.canFitRecordLength(len(value)) {
		return false
	}
	if b.builder.Len() > 0 {
		b.builder.WriteByte('\n')
	}
	b.builder.WriteString(value)
	return true
}

func (b *boundedSummaryBuilder) canFitRecordLength(length int) bool {
	return length > 0 && b.builder.Len()+length+1 <= b.limit
}

func (b *boundedSummaryBuilder) forceRecord(value string) {
	extra := len(value)
	if b.builder.Len() > 0 {
		extra++
	}
	if b.builder.Len()+extra > b.hardLimit {
		return
	}
	if b.builder.Len() > 0 {
		b.builder.WriteByte('\n')
	}
	b.builder.WriteString(value)
}
