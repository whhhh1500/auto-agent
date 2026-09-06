// Policies domain for the embedded Console.
(function () {
  'use strict'

  window.HarnessConsolePolicies = {
    state() {
      return {
        policies: [],
        pol: { scopeText: '', allow: '', deny: '', maxSteps: '', maxToolCalls: '' },
      }
    },
    methods: {
      async loadPolicies() {
        this.policies = (await this.api('GET', '/v1/admin/policies')).policies || []
      },
      async bindPolicy() {
        const body = {
          scope: this.parseScope(this.pol.scopeText),
          allow_permissions: this.csv(this.pol.allow),
          deny_permissions: this.csv(this.pol.deny),
        }
        if (this.pol.maxSteps) body.max_steps = parseInt(this.pol.maxSteps, 10)
        if (this.pol.maxToolCalls) body.max_tool_calls = parseInt(this.pol.maxToolCalls, 10)
        await this.api('POST', '/v1/admin/policies', body)
        this.notify('策略层已绑定')
        this.load('policies')
      },
    },
  }
})()
