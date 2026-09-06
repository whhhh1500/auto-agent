// Playground domain for the embedded Console, including Run/SSE streaming.
(function () {
  'use strict'

  window.HarnessConsolePlayground = {
    state() {
      return {
        pg: { profile: '', sessionId: '', input: '', messages: [], streaming: false, pendingText: '' },
      }
    },
    methods: {
      async pgNewSession() {
        const data = await this.api('POST', '/v1/sessions', { profile_id: this.pg.profile })
        this.pg.sessionId = data.id
        this.pg.messages = []
        this.notify('会话已创建 ' + data.id)
      },
      async pgSend() {
        const text = this.pg.input.trim()
        if (!text || !this.pg.sessionId || this.pg.streaming) return
        this.pg.input = ''
        this.pg.messages.push({ role: 'user', text })
        this.pg.streaming = true
        this.pg.pendingText = ''
        try {
          const response = await fetch('/v1/sessions/' + this.pg.sessionId + '/runs', {
            method: 'POST', headers: this.headers(), body: JSON.stringify({ message: text }),
          })
          if (!response.ok || !response.body) {
            this.notify('运行失败 HTTP ' + response.status, true)
            this.pg.streaming = false
            return
          }
          const reader = response.body.getReader()
          const decoder = new TextDecoder()
          let buffer = ''
          let eventType = ''
          for (;;) {
            const { done, value } = await reader.read()
            if (done) break
            buffer += decoder.decode(value, { stream: true })
            let idx
            while ((idx = buffer.indexOf('\n\n')) >= 0) {
              const frame = buffer.slice(0, idx)
              buffer = buffer.slice(idx + 2)
              let event = '', data = ''
              for (const line of frame.split('\n')) {
                if (line.startsWith('event: ')) event = line.slice(7)
                if (line.startsWith('data: ')) data = line.slice(6)
              }
              if (!event || !data) continue
              let payload = {}
              try { payload = JSON.parse(data) } catch (e) { continue }
              eventType = event
              if (event === 'assistant/chunk') this.pg.pendingText += (payload.data?.text || '')
              if (event === 'tool/call') this.pg.messages.push({ role: 'tool', text: '⚙ ' + (payload.data?.name || '') + ' ' + JSON.stringify(payload.data?.args || {}) })
              if (event === 'tool/result') this.pg.messages.push({ role: 'tool', text: '↳ ' + String(payload.data?.content || '').slice(0, 300) })
              if (event === 'assistant/message') {
                const finalText = payload.data?.text || ''
                if (finalText) { this.pg.messages.push({ role: 'assistant', text: finalText }); this.pg.pendingText = '' }
              }
              if (event === 'run/end') {
                if (this.pg.pendingText) this.pg.messages.push({ role: 'assistant', text: this.pg.pendingText })
                this.pg.pendingText = ''
              }
            }
          }
        } catch (e) { this.notify(String(e), true) }
        this.pg.streaming = false
      },
    },
  }
})()
