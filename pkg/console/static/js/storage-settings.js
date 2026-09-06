// Storage Settings domain for the embedded Console.
(function () {
  'use strict'

  const entries = {
    resources: {
      type: 'resType', path: 'resPath', endpoint: 'resEndpoint', region: 'resRegion',
      bucket: 'resBucket', access: 'resAccess', accessPreview: 'resAccessPreview', secret: 'resSecret', hasSecret: 'resHasSecret',
      preview: 'resSecretPreview', clear: 'resClearSecret', pathStyle: 'resPathStyle',
      disableConditionalWrites: null, desiredRevision: 'resDesiredRevision',
      configSource: 'resConfigSource', configStatus: 'resConfigStatus',
      activeType: 'resActiveType', activeRevision: 'resActiveRevision',
      restartPending: 'resRestartPending', errorCode: 'resErrorCode',
      migration: 'resMigration',
    },
    sessions: {
      type: 'sessType', path: 'sessPath', endpoint: 'sessEndpoint', region: 'sessRegion',
      bucket: 'sessBucket', access: 'sessAccess', accessPreview: 'sessAccessPreview', secret: 'sessSecret', hasSecret: 'sessHasSecret',
      preview: 'sessSecretPreview', clear: 'sessClearSecret', pathStyle: 'sessPathStyle',
      disableConditionalWrites: 'sessDisableConditionalWrites', desiredRevision: 'sessDesiredRevision',
      configSource: 'sessConfigSource', configStatus: 'sessConfigStatus',
      activeType: 'sessActiveType', activeRevision: 'sessActiveRevision',
      restartPending: 'sessRestartPending', errorCode: 'sessErrorCode',
      migration: 'sessMigration',
    },
  }

  function resetEntry(state, entry) {
    state[entry.type] = 'embedded'
    state[entry.path] = ''
    state[entry.endpoint] = ''
    state[entry.region] = ''
    state[entry.bucket] = ''
    state[entry.access] = ''
    state[entry.accessPreview] = ''
    state[entry.pathStyle] = false
    if (entry.disableConditionalWrites) state[entry.disableConditionalWrites] = false
    state[entry.secret] = ''
    state[entry.hasSecret] = false
    state[entry.preview] = ''
    state[entry.clear] = false
    state[entry.desiredRevision] = ''
    state[entry.configSource] = ''
    state[entry.configStatus] = ''
    state[entry.activeType] = ''
    state[entry.activeRevision] = ''
    state[entry.restartPending] = false
    state[entry.errorCode] = ''
    state[entry.migration] = null
  }

  window.HarnessConsoleStorageSettings = {
    state() {
      return {
        storage: {
          resType: 'embedded', resPath: '', resEndpoint: '', resRegion: '', resBucket: '', resAccess: '', resSecret: '',
          resPathStyle: false, resAccessPreview: '', resHasSecret: false, resSecretPreview: '', resClearSecret: false,
          resDesiredRevision: '', resConfigSource: '', resConfigStatus: '', resActiveType: '', resActiveRevision: '',
          resRestartPending: false, resErrorCode: '', resMigration: null, resourcesActive: '',
          sessType: 'embedded', sessPath: '', sessEndpoint: '', sessRegion: '', sessBucket: '', sessAccess: '', sessSecret: '',
          sessPathStyle: false, sessDisableConditionalWrites: false, sessAccessPreview: '', sessHasSecret: false, sessSecretPreview: '', sessClearSecret: false,
          sessDesiredRevision: '', sessConfigSource: '', sessConfigStatus: '', sessActiveType: '', sessActiveRevision: '',
          sessRestartPending: false, sessErrorCode: '', sessMigration: null, sessionsActive: '',
        },
      }
    },
    methods: {
      // loadStorageSettings() may also receive one domain for a CAS refresh.
      async loadStorageSettings(which) {
        const names = which ? [which] : Object.keys(entries)
        for (const name of names) {
          const entry = entries[name]
          resetEntry(this.storage, entry)
          try {
            const data = await this.api('GET', '/v1/admin/storage/' + name)
            const config = data.found && data.config ? data.config : {}
            this.storage[entry.type] = config.type || 'embedded'
            this.storage[entry.path] = config.path || ''
            this.storage[entry.endpoint] = config.endpoint || ''
            this.storage[entry.region] = config.region || ''
            this.storage[entry.bucket] = config.bucket || ''
            this.storage[entry.access] = ''
            this.storage[entry.accessPreview] = config.access_key_preview || ''
            this.storage[entry.pathStyle] = !!config.path_style
            if (entry.disableConditionalWrites) this.storage[entry.disableConditionalWrites] = !!config.disable_conditional_writes
            this.storage[entry.hasSecret] = !!config.has_secret
            this.storage[entry.preview] = config.secret_preview || ''
            this.storage[entry.desiredRevision] = config.desired_revision || data.desired_revision || ''
            this.storage[entry.configSource] = config.config_source || ''
            this.storage[entry.configStatus] = config.config_status || ''
            this.storage[entry.activeType] = data.active_type || ''
            this.storage[entry.activeRevision] = data.active_revision || ''
            this.storage[entry.restartPending] = !!data.restart_pending
            this.storage[entry.errorCode] = data.error_code || ''
            this.storage[entry.migration] = data.migration || null
            this.storage[name + 'Active'] = data.active_type || data.active || ''
          } catch (e) { /* not configured yet */ }
        }
      },
      storageBody(typeField, endpointField, bucketField, accessField, secretField, clearField) {
        const session = typeField.indexOf('sess') === 0
        const name = session ? 'sessions' : 'resources'
        const entry = entries[name]
        const body = { type: this.storage[typeField] }
        const revision = this.storage[entry.desiredRevision]
        if (revision) body.expected_revision = revision
        if (this.storage[clearField]) body.clear_secret = true
        if (body.type === 'file') {
          body.path = this.storage[entry.path]
        }
        if (body.type === 's3') {
          body.endpoint = this.storage[endpointField]
          body.region = this.storage[entry.region]
          body.bucket = this.storage[bucketField]
          if (this.storage[accessField] !== '') body.access_key = this.storage[accessField]
          if (!body.clear_secret && this.storage[secretField] !== '') {
            body.secret_key = this.storage[secretField]
          }
          body.path_style = !!this.storage[entry.pathStyle]
          if (entry.disableConditionalWrites) body.disable_conditional_writes = !!this.storage[entry.disableConditionalWrites]
        }
        return body
      },
      async testStorage(which) {
        const body = which === 'resources'
          ? this.storageBody('resType', 'resEndpoint', 'resBucket', 'resAccess', 'resSecret', 'resClearSecret')
          : this.storageBody('sessType', 'sessEndpoint', 'sessBucket', 'sessAccess', 'sessSecret', 'sessClearSecret')
        const data = await this.api('POST', '/v1/admin/storage/test', body)
        this.notify('连接测试: ' + data.status)
      },
      async saveStorage(which) {
        const body = which === 'resources'
          ? this.storageBody('resType', 'resEndpoint', 'resBucket', 'resAccess', 'resSecret', 'resClearSecret')
          : this.storageBody('sessType', 'sessEndpoint', 'sessBucket', 'sessAccess', 'sessSecret', 'sessClearSecret')
        let response
        try {
          response = await this.api('PUT', '/v1/admin/storage/' + which, body)
        } catch (e) {
          const status = e && (e.status || (e.response && e.response.status))
          if (status === 409 || (e && /\b409\b/.test(e.message || ''))) {
            await this.loadStorageSettings(which)
            this.notify('配置已被其他管理员更新，已重新加载，请确认后再保存', true)
            return
          }
          throw e
        }
        const entry = entries[which]
        if (which === 'resources') {
          this.storage.resSecret = ''
          this.storage.resClearSecret = false
        } else {
          this.storage.sessSecret = ''
          this.storage.sessClearSecret = false
        }
        await this.loadStorageSettings(which)
        let message
        if (which === 'resources') {
          message = response && response.migration && response.migration.state !== 's3_active'
            ? '资源配置已保存，后台迁移中' : '资源存储已保存并生效'
        } else {
          message = '会话存储已保存，重启后生效'
        }
        this.notify(message)
        this.load('overview')
      },
    },
  }
})()
