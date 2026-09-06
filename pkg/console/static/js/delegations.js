(function () {
  'use strict'
  window.HarnessConsoleDelegations = {
    state() {
      return {
        delegations: [], delegationQuery: { parent_session_id: '', parent_run_id: '', child_session_id: '', tenant: '', limit: 100, offset: 0 },
      }
    },
    methods: {
    async loadDelegations() {
      const params = new URLSearchParams({
        parent_session_id: this.delegationQuery.parent_session_id,
        parent_run_id: this.delegationQuery.parent_run_id,
        child_session_id: this.delegationQuery.child_session_id,
        tenant: this.delegationQuery.tenant,
        limit: String(this.delegationQuery.limit),
        offset: String(this.delegationQuery.offset),
      })
      const data = await this.api('GET', '/v1/admin/delegations?' + params)
      this.delegations = data.delegations || []
    },
    openDelegationSession(id) {
      if (!id) return
      this.sess.prefix = id
      this.go('sessions')
    },
    },
  }
})()
