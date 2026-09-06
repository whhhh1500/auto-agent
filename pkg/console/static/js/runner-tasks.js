(function () {
  'use strict'
  window.HarnessConsoleRunnerTasks = {
    state() {
      return {
        runnerTasks: [],
        runnerTaskQuery: { tenant: '', subject_id: '', scope: '', capability: '', worker_id: '', state: '', limit: 50 },
        runnerTaskPage: { total: 0, offset: 0 },
        runnerTaskDetail: null,
        runnerTaskActions: null, runnerRetry: { reason: '', confirm: false }, runnerRetryResult: null,
      }
    },
    methods: {
    async loadRunnerTasks() {
      const params = new URLSearchParams({
        tenant: this.runnerTaskQuery.tenant,
        subject_id: this.runnerTaskQuery.subject_id,
        scope: this.runnerTaskQuery.scope,
        capability: this.runnerTaskQuery.capability,
        worker_id: this.runnerTaskQuery.worker_id,
        state: this.runnerTaskQuery.state,
        limit: String(this.runnerTaskQuery.limit),
        offset: String(this.runnerTaskPage.offset),
      })
      const data = await this.api('GET', '/v1/admin/runners/tasks?' + params)
      this.runnerTasks = data.tasks || []
      this.runnerTaskPage.total = data.total ?? this.runnerTasks.length
      this.runnerTaskPage.offset = Number(data.offset ?? this.runnerTaskPage.offset)
      this.runnerTaskQuery.limit = Number(data.limit ?? this.runnerTaskQuery.limit)
      this.runnerTaskDetail = null
      this.runnerTaskActions = null
      this.runnerRetryResult = null
    },
    async resetRunnerTasks() {
      this.runnerTaskPage.offset = 0
      await this.loadRunnerTasks()
    },
    async paginateRunnerTasks(direction) {
      const limit = Number(this.runnerTaskQuery.limit) || 1
      const next = this.runnerTaskPage.offset + direction * limit
      if (next < 0 || next >= this.runnerTaskPage.total) return
      this.runnerTaskPage.offset = next
      await this.loadRunnerTasks()
    },
    async viewRunnerTask(id) {
      const data = await this.api('GET', '/v1/admin/runners/tasks/' + encodeURIComponent(id))
      this.runnerTaskDetail = data.task || null
      this.runnerTaskActions = data.actions || null
      this.runnerRetry = { reason: '', confirm: false }
      this.runnerRetryResult = null
    },
    async retryRunnerTask() {
      if (!this.runnerTaskDetail || !this.runnerTaskActions?.retry?.allowed || !this.runnerRetry.reason.trim() || !this.runnerRetry.confirm) return
      const originalID = this.runnerTaskDetail.id
      const originalSummary = { ...this.runnerTaskDetail }
      const data = await this.api('POST', '/v1/admin/runners/tasks/' + encodeURIComponent(originalID) + '/retry', { confirm: true, reason: this.runnerRetry.reason.trim() })
      const original = data.original || originalSummary
      const retry = data.retry || {}
      const result = { original_id: original.id || originalID, retry_id: retry.id || '-', created: data.created === true }
      this.notify('已提交安全重试')
      await this.loadRunnerTasks()
      // Refresh the catalog but retain the safe detail and linkage in view.
      this.runnerTaskDetail = original
      this.runnerTaskActions = { retry: { allowed: false, description: '已提交重试；请从列表重新读取详情' } }
      this.runnerRetryResult = result
    },
    async cancelRunnerTask(task) {
      await this.api('POST', '/v1/admin/runners/tasks/' + encodeURIComponent(task.id) + '/cancel')
      this.notify('已请求取消运行器任务 ' + task.id)
      await this.loadRunnerTasks()
    },
    async recoverRunnerTasks() {
      const data = await this.api('POST', '/v1/admin/runners/recover')
      const requeued = Number(data.requeued ?? 0)
      const terminal = Number(data.terminal ?? 0)
      this.notify('已恢复过期运行器任务：重新排队 ' + requeued + '，已终态 ' + terminal)
      await this.loadRunnerTasks()
    },
    },
  }
})()
