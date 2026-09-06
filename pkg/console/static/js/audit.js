// Audit domain for the embedded Console.
(function () {
  'use strict'

  window.HarnessConsoleAudit = {
    state() {
      return {
        auditEvents: [],
        au: { actor: '', action: '', limit: 50, offset: 0, total: 0 },
      }
    },
    methods: {
      async loadAudit() {
        const params = new URLSearchParams({
          actor: this.au.actor, action: this.au.action,
          limit: String(this.au.limit), offset: String(this.au.offset),
        })
        const data = await this.api('GET', '/v1/admin/audit?' + params)
        this.auditEvents = data.events || []
        this.au.total = data.total ?? this.auditEvents.length
      },
    },
  }
})()
