(function () {
  'use strict'
  window.HarnessConsoleCapabilities = {
    state() {
      return {
        caps: [],
        runtimes: [],
        capNew: { id: '', version: '1.0.0', kind: 'connector', runtime: 'http@1', runtimeJSON: '{}', description: '', scopeText: '', url: '', method: 'POST', header: '', childProfile: '', maxDepth: '', maxChildren: '', maxToolCalls: '', replace: false, manifestError: '', serverError: '', manifestJSON: '{\n  "input_schema": {"type": "object", "properties": {"query": {"type": "string"}}},\n  "tool": {"parameters": {"type": "object", "properties": {"query": {"type": "string"}}}}\n}' },
      }
    },
    methods: {
    capabilityRuntimeID() {
      return String(this.capNew.runtime || '').split('@')[0]
    },
    async loadCapabilityRuntimes() {
      try {
        const data = await this.api('GET', '/v1/admin/capability-runtimes')
        const seen = new Set()
        this.runtimes = (data.runtimes || []).filter(runtime => {
          const key = runtime.id + '@' + runtime.version
          if (seen.has(key)) return false
          seen.add(key)
          return runtime.id && runtime.version
        }).map(runtime => ({ ...runtime, label: ({ http: 'HTTP', runner: 'Private Runner', subagent: 'Subagent' }[runtime.id] || (runtime.id + '@' + runtime.version)) }))
        if (!this.runtimes.length) this.runtimes = [{ id: 'http', version: '1', label: 'HTTP' }, { id: 'runner', version: '1', label: 'Private Runner' }, { id: 'subagent', version: '1', label: 'Subagent' }]
      } catch (_) {
        this.runtimes = [{ id: 'http', version: '1', label: 'HTTP' }, { id: 'runner', version: '1', label: 'Private Runner' }, { id: 'subagent', version: '1', label: 'Subagent' }]
      }
    },
    async createCapability() {
      let manifest
      try {
        manifest = JSON.parse(this.capNew.manifestJSON || '{}')
        if (!manifest || Array.isArray(manifest) || typeof manifest !== 'object') throw new Error('Manifest 必须是 JSON 对象')
        if (manifest.tool && manifest.tool.parameters !== undefined && typeof manifest.tool.parameters !== 'object') throw new Error('tool.parameters 必须是 JSON Schema 对象')
        this.capNew.manifestError = ''
      } catch (e) {
        this.capNew.manifestError = 'Manifest JSON 解析失败: ' + e.message
        this.notify(this.capNew.manifestError, true)
        return
      }
      manifest.id = this.capNew.id
      manifest.version = this.capNew.version
      manifest.name = manifest.name || this.capNew.id
      manifest.description = this.capNew.description || manifest.description || ''
      manifest.kind = this.capNew.kind
      const runtimeID = this.capabilityRuntimeID()
      if (runtimeID === 'subagent') {
        manifest.kind = 'agent'
        manifest.metadata = { ...(manifest.metadata || {}) }
        const limits = [['subagent.max_depth', this.capNew.maxDepth], ['subagent.max_children', this.capNew.maxChildren], ['subagent.max_tool_calls', this.capNew.maxToolCalls]]
        for (const [key, value] of limits) {
          if (String(value || '').trim()) manifest.metadata[key] = String(value).trim()
          else delete manifest.metadata[key]
        }
        // Preserve a usable schema in simple-form mode; never replace an
        // advanced manifest's parameters with an empty object.
        manifest.tool = manifest.tool || {}
        if (!manifest.tool.parameters) manifest.tool.parameters = { type: 'object', properties: { prompt: { type: 'string' }, session_id: { type: 'string' }, followup: { type: 'string' } } }
      }
      let execution
      if (!['http', 'runner', 'subagent'].includes(runtimeID)) {
        try { execution = JSON.parse(this.capNew.runtimeJSON || '{}'); execution.runtime = this.capNew.runtime; if (!execution.runtime.includes('@')) throw new Error('插件 runtime 必须使用 id@version') } catch (e) { this.capNew.serverError = e.message; return }
      } else execution = runtimeID === 'runner'
        ? { runtime: this.capNew.runtime }
        : runtimeID === 'subagent'
          ? { runtime: this.capNew.runtime, entrypoint: this.capNew.childProfile }
          : (() => {
            const headers = {}
            if (this.capNew.header) {
              const [name] = this.capNew.header.split(':')
              headers[name.trim()] = this.capNew.header.slice(this.capNew.header.indexOf(':') + 1).trim()
            }
            return { runtime: this.capNew.runtime, entrypoint: this.capNew.url, method: this.capNew.method, headers }
          })()
      this.capNew.serverError = ''
      try {
        await this.api('POST', '/v1/admin/capabilities/bind', {
          scope: this.parseScope(this.capNew.scopeText), replace: this.capNew.replace,
          manifest,
          execution,
        })
        this.notify('能力已挂载（可在动态绑定中解除）')
        this.load('capabilities')
      } catch (e) { this.capNew.serverError = String(e.message || e) }
    },
    async disableCapability(id) {
      await this.api('POST', '/v1/admin/capabilities/' + id + '/disable', { scope: this.parseScope('') })
      this.notify('已禁用 ' + id)
      this.load('capabilities')
    },
    },
  }
})()
