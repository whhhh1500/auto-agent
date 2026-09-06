(function () {
  'use strict'
  window.HarnessConsoleShell = {
    state() {
      return {
    view: 'overview',
    toast: { show: false, err: false, text: '', timer: null },
    nav: [
      { id: 'overview',     label: '开始使用',      icon: '◈' },
      { id: 'playground',   label: '对话',          icon: '💬' },
      { id: 'capabilities', label: '能力',          icon: '▦', advancedOnly: true },
      { id: 'profiles',     label: 'Profile / 发布', icon: '◈', advancedOnly: true },
      { id: 'approvals',    label: 'Approval Inbox / 审批', icon: '✓', advancedOnly: true },
      { id: 'evaluations',  label: '评测关联',      icon: '⌁', advancedOnly: true },
      { id: 'evidence',     label: '统一 Evidence', icon: '◎', advancedOnly: true },
      { id: 'delegations',  label: 'Delegations / 委派', icon: '↳', advancedOnly: true },
      { id: 'runner-tasks', label: 'Runner Tasks / 运行器任务', icon: '▤', advancedOnly: true },
      { id: 'rag-projection', label: 'RAG Projection / RAG 投影', icon: '⌗', adminOnly: true, advancedOnly: true },
      { id: 'memory-projection', label: 'Memory Projection / Memory 投影', icon: '⌗', adminOnly: true, advancedOnly: true },
      { id: 'policies',     label: '策略',          icon: '⛨', advancedOnly: true },
      { id: 'credentials',  label: '凭证',          icon: '🔑', advancedOnly: true },
      { id: 'notification-targets', label: '通知目标', icon: '◉', operatorOnly: true, advancedOnly: true },
      { id: 'sandbox', label: 'Sandbox 状态', icon: '◇', adminOnly: true, advancedOnly: true },
      { id: 'resources',    label: '资源',          icon: '🗂', advancedOnly: true },
      { id: 'sessions',     label: '会话',          icon: '▤', advancedOnly: true },
      { id: 'accounts',     label: '用户 / 租户',   icon: '👥', operatorOnly: true, advancedOnly: true },
      { id: 'bindings',     label: '动态绑定',       icon: '⚓', advancedOnly: true },
      { id: 'audit',        label: '操作日志',       icon: '🕘', advancedOnly: true },
      { id: 'observability', label: '观测监控',      icon: '📡', advancedOnly: true },
    ],
    ov: {},
    advancedOpen: localStorage.getItem('hc_console_advanced_nav') === 'true',
      }
    },
    methods: {
    toggleAdvancedNav() {
      this.advancedOpen = !this.advancedOpen
      localStorage.setItem('hc_console_advanced_nav', String(this.advancedOpen))
    },
    async load(view) {
      try {
        if (typeof this.ensureIdentityTenant === 'function') this.ensureIdentityTenant()
        if (view === 'overview') {
          this.ov = await this.api('GET', '/v1/admin/overview')
          this.profiles = (await this.api('GET', '/v1/profiles')).profiles || []
          await this.loadStorageSettings()
          await this.loadModelSettings()
        }
        if (view === 'playground') {
          if (!this.profiles.length) this.profiles = (await this.api('GET', '/v1/profiles')).profiles || []
          if (!this.pg.profile && this.profiles.length) {
            this.pg.profile = this.profiles.includes('general') ? 'general' : this.profiles[0]
          }
        }
        if (view === 'capabilities') this.caps = (await this.api('GET', '/v1/capabilities')).capabilities || []
        if (view === 'profiles')     await this.loadProfiles()
        if (view === 'approvals')   await this.loadApprovals()
        if (view === 'evaluations') await this.loadEvaluations()
        if (view === 'evidence') await this.loadEvidence(true)
        if (view === 'delegations') await this.loadDelegations()
        if (view === 'runner-tasks') await this.loadRunnerTasks()
        if (view === 'rag-projection') await this.loadRagProjection()
        if (view === 'memory-projection') await this.loadMemoryProjection()
        if (view === 'policies')     await this.loadPolicies()
        if (view === 'credentials')  await this.loadCredentials()
        if (view === 'notification-targets') await this.loadNotificationTargets()
        if (view === 'sandbox') await this.loadSandboxProviders()
        if (view === 'resources')    await this.loadResources()
        if (view === 'sessions')     await this.loadSessions()
        if (view === 'accounts')     await this.loadAccounts()
        if (view === 'bindings') await this.loadBindings()
        if (view === 'audit')    await this.loadAudit()
        if (view === 'observability') await this.loadObs()
      } catch (e) { /* toast already shown */ }
    },
    },
  }
})()
