// Playground domain for the embedded Console, including Run/SSE streaming.
(function () {
  'use strict'

  window.HarnessConsolePlayground = {
    state() {
      return {
        pg: { profile: '', sessionId: '', input: '', messages: [], streaming: false, pendingText: '', generation: 0 },
      }
    },
    methods: {
      async pgNewSession() {
        if (this.pg.streaming) return
        const generation = this.pg.generation
        const sessionID = this.pg.sessionId
        const profile = this.pg.profile
        const data = await this.api('POST', '/v1/sessions', { profile_id: this.pg.profile })
        if (this.pg.streaming || this.pg.generation !== generation || this.pg.sessionId !== sessionID || this.pg.profile !== profile) return
        this.pg.generation++
        this.pg.sessionId = data.id
        this.pg.messages = []
        this.notify('会话已创建 ' + data.id)
      },
      async pgSend() {
        const text = this.pg.input.trim()
        if (!text || !this.pg.sessionId || this.pg.streaming) return
        const sessionID = this.pg.sessionId
        const generation = this.pg.generation + 1
        this.pg.generation = generation
        const isCurrent = () => this.pg.generation === generation && this.pg.sessionId === sessionID
        let sawTerminal = false
        let incompleteReported = false
        const finishIncomplete = (detail) => {
          if (!isCurrent() || incompleteReported) return
          incompleteReported = true
          if (this.pg.pendingText) this.pg.messages.push({ role: 'assistant', text: this.pg.pendingText, incomplete: true })
          this.pg.pendingText = ''
          this.pg.messages.push({ role: 'assistant', text: '运行流已中断，结果未确认。请查看此会话的事件记录确认结果。', incomplete: true })
          this.notify(detail, true)
        }
        const finishPending = () => {
          if (this.pg.pendingText) this.pg.messages.push({ role: 'assistant', text: this.pg.pendingText })
          this.pg.pendingText = ''
        }
        this.pg.input = ''
        this.pg.messages.push({ role: 'user', text })
        this.pg.streaming = true
        this.pg.pendingText = ''
        try {
          const response = await fetch('/v1/sessions/' + sessionID + '/runs', {
            method: 'POST', headers: this.headers(), body: JSON.stringify({ message: text }),
          })
          if (!response.ok || !response.body) {
            if (isCurrent()) {
              this.notify('运行失败 HTTP ' + response.status, true)
              this.pg.streaming = false
            }
            return
          }
          const reader = response.body.getReader()
          const decoder = new TextDecoder()
          let buffer = ''
          for (;;) {
            const { done, value } = await reader.read()
            if (done) break
            if (!isCurrent()) return
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
              if (event === 'assistant/chunk') this.pg.pendingText += (payload.data?.text || '')
              if (event === 'tool/call') this.pg.messages.push({ role: 'tool', text: '⚙ ' + (payload.data?.name || '') + ' ' + JSON.stringify(payload.data?.args || {}) })
              if (event === 'tool/result') this.pg.messages.push({ role: 'tool', text: '↳ ' + String(payload.data?.content || '').slice(0, 300) })
              if (event === 'assistant/message') {
                const finalText = payload.data?.text || ''
                if (finalText) { this.pg.messages.push({ role: 'assistant', text: finalText }); this.pg.pendingText = '' }
              }
              if (event === 'run/end') {
                sawTerminal = true
                finishPending()
              }
              if (event === 'approval/requested') {
                sawTerminal = true
                finishPending()
                this.notify('运行正在等待审批。')
              }
              if (event === 'store/error' || event === 'control/error') {
                sawTerminal = true
                finishPending()
                this.pg.messages.push({ role: 'assistant', text: '运行失败：' + (payload.error || payload.data?.error || payload.data?.message || event), failure: true })
                this.notify('运行失败：' + event, true)
              }
            }
          }
          if (!sawTerminal) finishIncomplete('运行流在收到终态前结束；已保留收到的内容。')
        } catch (e) {
          if (!sawTerminal) finishIncomplete('运行流中断；已保留收到的内容。')
        }
        if (isCurrent()) this.pg.streaming = false
      },
    },
  }
})()
