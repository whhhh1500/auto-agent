package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestConsolePlaygroundStreamPreservesPartialOutputAndIsolatesSessions(t *testing.T) {
	data, err := staticFS.ReadFile("static/js/playground.js")
	if err != nil {
		t.Fatalf("read playground module: %v", err)
	}
	dir := t.TempDir()
	modulePath := filepath.Join(dir, "playground.js")
	if err := os.WriteFile(modulePath, data, 0o600); err != nil {
		t.Fatalf("write playground module: %v", err)
	}
	quotedModule, err := json.Marshal(modulePath)
	if err != nil {
		t.Fatalf("quote playground module path: %v", err)
	}
	script := `
const assert = require('node:assert/strict')
global.window = {}
require(` + string(quotedModule) + `)
const playground = window.HarnessConsolePlayground
const encode = new TextEncoder()

function response(steps) {
  return {
    ok: true,
    status: 200,
    body: {
      getReader() {
        let next = 0
        return {
          async read() {
            const step = steps[next++]
            if (step === undefined) return { done: true }
            if (step instanceof Error) throw step
            return { done: false, value: encode.encode(step) }
          },
        }
      },
    },
  }
}

function model(steps) {
  const state = playground.state()
  const result = Object.assign(state, playground.methods, {
    notices: [],
    headers() { return { Authorization: 'Bearer test' } },
    notify(text, error) { this.notices.push({ text, error: Boolean(error) }) },
  })
  result.pg.sessionId = 'session-a'
  result.pg.input = 'hello'
  let posts = 0
  global.fetch = async () => { posts++; return response(steps) }
  result.posts = () => posts
  return result
}

async function send(result) {
  await result.pgSend()
  assert.equal(result.posts(), 1, 'stream handling must never submit a second run')
  assert.equal(result.pg.streaming, false, 'current stream must finish in the UI')
}

(async () => {
  {
    const result = model(['event: assistant/chunk\ndata: {"data":{"text":"partial"}}\n\n'])
    await send(result)
    assert.deepEqual(result.pg.messages, [
      { role: 'user', text: 'hello' },
      { role: 'assistant', text: 'partial', incomplete: true },
      { role: 'assistant', text: '运行流已中断，结果未确认。请查看此会话的事件记录确认结果。', incomplete: true },
    ])
    assert.equal(result.pg.pendingText, '')
    assert.deepEqual(result.notices, [{ text: '运行流在收到终态前结束；已保留收到的内容。', error: true }])
  }
  {
    const result = model(['event: assistant/chunk\ndata: {"data":{"text":"complete"}}\n\n', 'event: run/end\ndata: {}\n\n'])
    await send(result)
    assert.deepEqual(result.pg.messages, [
      { role: 'user', text: 'hello' },
      { role: 'assistant', text: 'complete' },
    ])
    assert.deepEqual(result.notices, [])
  }
  {
    const result = model([
      'event: assistant/chunk\ndata: {"data":{"text":"draft"}}\n\n',
      'event: assistant/message\ndata: {"data":{"text":"final text"}}\n\n',
      'event: run/end\ndata: {}\n\n',
    ])
    await send(result)
    assert.deepEqual(result.pg.messages, [
      { role: 'user', text: 'hello' },
      { role: 'assistant', text: 'final text' },
    ])
    assert.deepEqual(result.notices, [])
  }
  {
    const result = model(['event: assistant/message\ndata: {"data":{"text":"final text"}}\n\n'])
    await send(result)
    assert.deepEqual(result.pg.messages, [
      { role: 'user', text: 'hello' },
      { role: 'assistant', text: 'final text' },
      { role: 'assistant', text: '运行流已中断，结果未确认。请查看此会话的事件记录确认结果。', incomplete: true },
    ])
  }
  {
    const result = model(['event: run/error\ndata: {"data":{"error":"model failed"}}\n\n'])
    await send(result)
    assert.equal(result.pg.messages.at(-1).incomplete, true, 'run/error without run/end does not confirm the stream')
    assert.equal(result.pg.messages.at(-1).text, '运行流已中断，结果未确认。请查看此会话的事件记录确认结果。')
  }
  {
    const result = model(['event: approval/requested\ndata: {}\n\n'])
    await send(result)
    assert.deepEqual(result.pg.messages, [{ role: 'user', text: 'hello' }])
    assert.deepEqual(result.notices, [{ text: '运行正在等待审批。', error: false }])
  }
  {
    for (const event of ['store/error', 'control/error']) {
      const result = model(['event: ' + event + '\ndata: {"error":"durability failed"}\n\n'])
      await send(result)
      assert.deepEqual(result.pg.messages, [
        { role: 'user', text: 'hello' },
        { role: 'assistant', text: '运行失败：durability failed', failure: true },
      ])
      assert.deepEqual(result.notices, [{ text: '运行失败：' + event, error: true }])
    }
  }
  {
    const result = model(['event: assistant/chunk\ndata: {"data":{"text":"partial"}}\n\n', new Error('reader disconnected')])
    await send(result)
    assert.equal(result.pg.messages[1].text, 'partial')
    assert.equal(result.pg.messages[1].incomplete, true)
    assert.equal(result.pg.messages[2].incomplete, true)
  }
  {
    const result = model(['event: run/end\ndata: {}\n\n', new Error('reader disconnected after terminal')])
    await send(result)
    assert.deepEqual(result.pg.messages, [{ role: 'user', text: 'hello' }])
    assert.deepEqual(result.notices, [])
  }
  {
    let release
    const delayed = new Promise(resolve => { release = resolve })
    const state = playground.state()
    const result = Object.assign(state, playground.methods, {
      notices: [],
      headers() { return {} },
      notify(text, error) { this.notices.push({ text, error: Boolean(error) }) },
    })
    result.pg.sessionId = 'session-a'
    result.pg.input = 'hello'
    global.fetch = async () => ({ ok: true, status: 200, body: { getReader() {
      let read = 0
      return { async read() {
        if (read++ === 0) return delayed
        return { done: true }
      } }
    } } })
    const running = result.pgSend()
    await Promise.resolve()
    result.pg.sessionId = 'session-b'
    result.pg.generation++
    result.pg.messages = [{ role: 'user', text: 'new session' }]
    result.pg.pendingText = ''
    result.pg.streaming = false
    release({ done: false, value: encode.encode('event: assistant/chunk\ndata: {"data":{"text":"stale"}}\n\n') })
    await running
    assert.deepEqual(result.pg.messages, [{ role: 'user', text: 'new session' }], 'old stream must not update a new session')
    assert.equal(result.pg.streaming, false, 'old stream must not change new session state')
  }
  {
    let releaseCreate
    const delayedCreate = new Promise(resolve => { releaseCreate = resolve })
    const state = playground.state()
    const result = Object.assign(state, playground.methods, {
      notices: [],
      headers() { return {} },
      async api() { return delayedCreate },
      notify(text, error) { this.notices.push({ text, error: Boolean(error) }) },
    })
    result.pg.sessionId = 'old-session'
    const creating = result.pgNewSession()
    await Promise.resolve()
    result.pg.input = 'hello'
    global.fetch = async () => response(['event: run/end\ndata: {}\n\n'])
    await result.pgSend()
    releaseCreate({ id: 'late-session' })
    await creating
    assert.equal(result.pg.sessionId, 'old-session', 'late new-session response must not replace an active generation')
    assert.deepEqual(result.pg.messages, [{ role: 'user', text: 'hello' }])
  }
})().catch(error => { console.error(error); process.exitCode = 1 })
`
	scriptPath := filepath.Join(dir, "playground-stream-regression.js")
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf("write playground regression: %v", err)
	}
	if output, err := exec.Command("node", scriptPath).CombinedOutput(); err != nil {
		t.Fatalf("playground stream regression failed: %v\n%s", err, output)
	}
}

func TestConsolePlaygroundDisablesSessionSwitchWhileStreaming(t *testing.T) {
	data, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("read console index: %v", err)
	}
	page := string(data)
	for _, expected := range []string{
		`x-model="pg.profile" :disabled="pg.streaming"`,
		`x-model="pg.sessionId" size="34" placeholder="sess_..." :disabled="pg.streaming"`,
		`@click="pgNewSession()" :disabled="pg.streaming"`,
	} {
		if !strings.Contains(page, expected) {
			t.Fatalf("playground must disable session switch while streaming: missing %q", expected)
		}
	}
}
