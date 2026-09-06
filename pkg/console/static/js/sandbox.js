// Sandbox provider discovery domain for the embedded Console.
// This is intentionally read-only: it reports registered provider guarantees
// and current probe availability, but cannot start a process or a session.
(function () {
  'use strict'

  window.HarnessConsoleSandbox = {
    state() {
      return { sandbox: { providers: [], loading: false, status: '' } }
    },
    methods: {
      async loadSandboxProviders() {
        this.sandbox.loading = true
        this.sandbox.status = ''
        try {
          const data = await this.api('GET', '/v1/admin/sandbox/providers')
          this.sandbox.providers = Array.isArray(data.providers)
            ? data.providers.map(provider => this.normalizeSandboxProvider(provider))
            : []
          this.sandbox.status = '已读取 ' + this.sandbox.providers.length + ' 个已注册 provider。'
        } catch (error) {
          this.sandbox.providers = []
          this.sandbox.status = '无法读取 sandbox provider 状态。'
          throw error
        } finally {
          this.sandbox.loading = false
        }
      },
      sandboxAssurance(assurance) {
        if (!assurance) return '-'
        const level = { 0: 'none', 1: 'process', 2: 'container', 3: 'vm' }[assurance.level] || 'unknown'
        return level + (assurance.network_isolation ? ' / network isolated' : '')
      },
      normalizeSandboxProvider(provider) {
        const value = provider || {}
        const probe = value.probe || {}
        const status = ['ready', 'not_installed', 'repair_required', 'unavailable'].includes(probe.status)
          ? probe.status
          : (probe.available ? 'ready' : 'unavailable')
        return { ...value, probe: { ...probe, status } }
      },
      sandboxStatus(provider) {
        const probe = provider && provider.probe
        if (!probe) return 'unavailable'
        return probe.status || (probe.available ? 'ready' : 'unavailable')
      },
    },
  }
})()
