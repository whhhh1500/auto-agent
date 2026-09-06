(function () {
  'use strict'
  window.HarnessConsoleApprovals = {
    state() {
      return {
        approvals: [], approvalDetail: null, approvalFilter: 'pending', approvalRunMatch: null,
      }
    },
    methods: {
    async approvalList(status) {
      const params = new URLSearchParams({ status, limit: '100' })
      return (await this.api('GET', '/v1/admin/approvals?' + params)).approvals || []
    },
    async loadApprovals() {
      this.approvalDetail = null
      this.approvalRunMatch = null
      try {
        if (this.approvalFilter === 'processed') {
          const sets = await Promise.all([this.approvalList('approved'), this.approvalList('denied')])
          this.approvals = sets.flat().sort((a, b) => String(b.requested_at || '').localeCompare(String(a.requested_at || '')))
        } else this.approvals = await this.approvalList(this.approvalFilter)
      } catch (e) { this.approvals = [] }
    },
    async viewApproval(id) {
      this.approvalDetail = await this.api('GET', '/v1/admin/approvals/' + encodeURIComponent(id))
      this.approvalRunMatch = null
    },
    async decideApproval(id, decision) {
      const previousRunID = this.approvalDetail?.id === id ? this.approvalDetail.run_id : (this.approvals.find(a => a.id === id)?.run_id || '')
      const record = await this.api('POST', '/v1/admin/approvals/' + encodeURIComponent(id) + '/decision', { decision })
      this.approvalDetail = record
      this.approvalRunMatch = !!previousRunID && previousRunID === record.run_id
      this.notify(decision === 'approved' ? '已批准，run_id 保持不变' : '已拒绝')
      await this.loadApprovals()
      this.approvalDetail = record
    },
    },
  }
})()
