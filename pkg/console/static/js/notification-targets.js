// Notification target administration domain for the embedded Console.
//
// Target configuration is write-only. List responses deliberately omit it;
// this module never derives, stores, or resubmits a previous configuration.
(function () {
  'use strict'

  const defaultChannelID = 'webhook'
  const defaultChannelVersion = '1'

  function splitFormats(value) {
    const seen = new Set()
    return String(value || '').split(',').map(item => item.trim()).filter(item => {
      if (!item || seen.has(item)) return false
      seen.add(item)
      return true
    })
  }

  window.HarnessConsoleNotificationTargets = {
    state() {
      return {
        notificationTargets: {
          targets: [], tenantID: '', targetRef: '', channelID: defaultChannelID,
          channelVersion: defaultChannelVersion, label: '', formatsText: '', enabled: true,
          expectedRevision: '', originalChannelID: '', originalChannelVersion: '',
          editing: false, configText: '', status: '', loading: false,
        },
      }
    },
    methods: {
      notificationTargetEnsureTenant() {
        const target = this.notificationTargets
        const identityTenant = String(this.identity?.tenant || '').trim()
        if (!target.tenantID.trim() && identityTenant && identityTenant !== '-') target.tenantID = identityTenant
      },
      notificationTargetTenantQuery() {
        const tenantID = this.notificationTargets.tenantID.trim()
        return tenantID ? '?tenant_id=' + encodeURIComponent(tenantID) : ''
      },
      notificationTargetTenantBody(body) {
        const tenantID = this.notificationTargets.tenantID.trim()
        if (tenantID) body.tenant_id = tenantID
        return body
      },
      notificationTargetConfigRequired() {
        const target = this.notificationTargets
        return !target.editing || target.channelID !== target.originalChannelID || target.channelVersion !== target.originalChannelVersion
      },
      notificationTargetConfig() {
        const text = this.notificationTargets.configText.trim()
        if (!text) {
          if (this.notificationTargetConfigRequired()) throw new Error('创建或切换频道时必须输入完整配置')
          return undefined
        }
        let configuration
        try {
          configuration = JSON.parse(text)
        } catch (_) {
          throw new Error('配置必须是合法 JSON')
        }
        if (!configuration || Array.isArray(configuration) || typeof configuration !== 'object') {
          throw new Error('配置必须是 JSON 对象')
        }
        return configuration
      },
      notificationTargetBody(includeRevision) {
        const target = this.notificationTargets
        const body = this.notificationTargetTenantBody({
          target_ref: target.targetRef.trim(), channel_id: target.channelID.trim(),
          channel_version: target.channelVersion.trim(), label: target.label.trim(),
          formats: splitFormats(target.formatsText), enabled: !!target.enabled,
        })
        if (includeRevision) body.expected_revision = target.expectedRevision
        const configuration = this.notificationTargetConfig()
        if (configuration !== undefined) body.config = configuration
        return body
      },
      resetNotificationTargetForm() {
        const target = this.notificationTargets
        target.targetRef = ''
        target.channelID = defaultChannelID
        target.channelVersion = defaultChannelVersion
        target.label = ''
        target.formatsText = ''
        target.enabled = true
        target.expectedRevision = ''
        target.originalChannelID = ''
        target.originalChannelVersion = ''
        target.editing = false
        // Never carry a configuration across reloads, edits, or channels.
        target.configText = ''
      },
      editNotificationTarget(record) {
        const target = this.notificationTargets
        target.targetRef = record.target_ref || ''
        target.channelID = record.channel_id || ''
        target.channelVersion = record.channel_version || ''
        target.label = record.label || ''
        target.formatsText = (record.formats || []).join(', ')
        target.enabled = !!record.enabled
        target.expectedRevision = record.revision || ''
        target.originalChannelID = target.channelID
        target.originalChannelVersion = target.channelVersion
        target.editing = true
        target.configText = ''
        target.status = '正在编辑 ' + target.targetRef + '；同一频道留空配置将保留已有值。'
      },
      async loadNotificationTargets() {
        const target = this.notificationTargets
        this.notificationTargetEnsureTenant()
        target.loading = true
        target.status = ''
        // A GET response has no configuration. Clear local write-only input
        // before and after it so a failed reload cannot leave stale secrets.
        target.configText = ''
        try {
          const data = await this.api('GET', '/v1/admin/notification-targets' + this.notificationTargetTenantQuery())
          target.targets = Array.isArray(data.targets) ? data.targets : []
          target.status = '已加载 ' + target.targets.length + ' 个通知目标。'
        } catch (error) {
          target.targets = []
          target.status = '无法加载通知目标。'
          throw error
        } finally {
          target.configText = ''
          target.loading = false
        }
      },
      async saveNotificationTarget() {
        const target = this.notificationTargets
        const editing = target.editing
        const body = this.notificationTargetBody(editing)
        if (!editing && !body.config) throw new Error('创建通知目标必须输入完整配置')
        await this.api(editing ? 'PUT' : 'POST', '/v1/admin/notification-targets', body)
        this.resetNotificationTargetForm()
        await this.loadNotificationTargets()
        this.notify(editing ? '通知目标已更新' : '通知目标已创建')
      },
      async deleteNotificationTarget(record) {
        if (!record || !record.target_ref || !record.revision) throw new Error('通知目标缺少并发修订')
        if (!window.confirm('删除通知目标 “' + record.target_ref + '”？此操作不可撤销。')) return
        const body = this.notificationTargetTenantBody({ target_ref: record.target_ref, expected_revision: record.revision })
        await this.api('DELETE', '/v1/admin/notification-targets', body)
        if (this.notificationTargets.editing && this.notificationTargets.targetRef === record.target_ref) this.resetNotificationTargetForm()
        await this.loadNotificationTargets()
        this.notify('通知目标已删除')
      },
    },
  }
})()
