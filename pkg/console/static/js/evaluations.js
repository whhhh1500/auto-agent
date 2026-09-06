(function () {
  'use strict'
  window.HarnessConsoleEvaluations = {
    state() {
      return {
        evaluationRuns: [], evaluationDetail: null,
        evidenceRecords: [], evidenceStats: null, evidenceQuery: { composition: '', assignment: '', profile: '', subject: '', kind: '', status: '', after: '', before: '', limit: 100 },
        evidenceNav: { cursor: '', next: '', stack: [] },
        evidenceDetail: null, evidenceDetailKind: '', evidenceDetailID: '',
        ev: { dataset: '', composition: '', assignment: '', limit: 50 },
      }
    },
    methods: {
    async loadEvaluations() {
      const params = new URLSearchParams({
        dataset_id: this.ev.dataset, composition_revision: this.ev.composition,
        assignment_revision: this.ev.assignment, limit: String(this.ev.limit),
      })
      const data = await this.api('GET', '/v1/admin/evaluations/runs?' + params)
      this.evaluationRuns = data.runs || []
      this.evaluationDetail = null
    },
    async viewEvaluation(id) {
      this.evaluationDetail = await this.api('GET', '/v1/admin/evaluations/runs/' + id)
    },
    evidenceParams(pagination = false) {
      const params = new URLSearchParams({
        composition_revision: this.evidenceQuery.composition,
        assignment_revision: this.evidenceQuery.assignment,
        profile_id: this.evidenceQuery.profile,
        subject_id: this.evidenceQuery.subject,
        kind: this.evidenceQuery.kind,
        status: this.evidenceQuery.status,
        created_after: this.evidenceQuery.after ? new Date(this.evidenceQuery.after).toISOString() : '',
        created_before: this.evidenceQuery.before ? new Date(this.evidenceQuery.before).toISOString() : '',
      })
      if (pagination) {
        params.set('limit', String(this.evidenceQuery.limit))
        params.set('cursor', this.evidenceNav.cursor)
      }
      return params
    },
    async loadEvidenceStats() {
      const data = await this.api('GET', '/v1/admin/evidence/stats?' + this.evidenceParams())
      this.evidenceStats = data.runs || null
    },
    async loadEvidence(reset = false) {
      if (reset) {
        this.evidenceNav = { cursor: '', next: '', stack: [] }
        this.evidenceStats = null
      }
      const stats = reset ? this.loadEvidenceStats().catch(() => { this.evidenceStats = null }) : null
      const data = await this.api('GET', '/v1/admin/evidence?' + this.evidenceParams(true))
      this.evidenceRecords = data.evidence || []
      this.evidenceNav.next = data.next_cursor || ''
      this.evidenceDetail = null
      if (stats) await stats
    },
    async nextEvidence() {
      if (!this.evidenceNav.next) return
      this.evidenceNav.stack.push(this.evidenceNav.cursor)
      this.evidenceNav.cursor = this.evidenceNav.next
      await this.loadEvidence(false)
    },
    async previousEvidence() {
      if (!this.evidenceNav.stack.length) return
      this.evidenceNav.cursor = this.evidenceNav.stack.pop()
      await this.loadEvidence(false)
    },
    async viewEvidence(record) {
      if (!record.detail_path) return
      this.evidenceDetail = await this.api('GET', record.detail_path)
      this.evidenceDetailKind = record.kind
      this.evidenceDetailID = record.id
    },
    },
  }
})()
