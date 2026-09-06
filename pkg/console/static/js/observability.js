// Observability domain for the embedded Console: rules, hits, and backtests.
(function () {
  'use strict'

  window.HarnessConsoleObservability = {
    state() {
      return {
        obs: {
          rules: [], hits: [], total: 0, ruleFilter: '', hitRule: '',
          form: { name: '', kind: 'keyword', pattern: '' },
        },
        bt: { source: '', profile: '', message: '', cutoff: '', result: null },
      }
    },
    methods: {
      async loadObs() {
        this.obs.rules = (await this.api('GET', '/v1/admin/obs/rules')).rules || []
        await this.loadHits()
      },
      async loadHits() {
        const params = new URLSearchParams({ rule_id: this.obs.hitRule, limit: '50' })
        const data = await this.api('GET', '/v1/admin/obs/hits?' + params)
        this.obs.hits = data.hits || []
        this.obs.total = data.total ?? 0
      },
      async createObsRule() {
        await this.api('POST', '/v1/admin/obs/rules', this.obs.form)
        this.notify('观测规则已创建')
        this.obs.form = { name: '', kind: this.obs.form.kind, pattern: '' }
        this.loadObs()
      },
      async toggleObsRule(rule) {
        await this.api(
          'PUT',
          '/v1/admin/obs/rules/' + rule.id,
          { name: rule.name, kind: rule.kind, pattern: rule.pattern, enabled: !rule.enabled },
        )
        this.loadObs()
      },
      async deleteObsRule(rule) {
        await this.api('DELETE', '/v1/admin/obs/rules/' + rule.id)
        this.notify('规则已删除')
        this.loadObs()
      },
      async runBacktest() {
        this.bt.result = null
        const body = { source_session_id: this.bt.source, profile_id: this.bt.profile, message: this.bt.message }
        if (this.bt.cutoff) body.cutoff_seq = parseInt(this.bt.cutoff, 10)
        this.bt.result = await this.api('POST', '/v1/admin/backtests', body)
        this.notify('回测完成')
      },
    },
  }
})()
