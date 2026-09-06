// Model Settings domain for the embedded Console.
//
// A secret preview is display-only. This module never hydrates a key into the
// form or sends a preview back as a write value.
(function () {
  'use strict'

  window.HarnessConsoleModelSettings = {
    state() {
      return {
        modelSettings: {
          baseURL: '', model: '', allowedModels: '', apiKey: '',
          hasAPIKey: false, apiKeyPreview: '', clearAPIKey: false,
          provider: '', protocol: '', executable: false, executionStatus: '', executionReason: '',
        },
        modelCatalog: { providers: [], protocols: [], compatibilities: [], defaults: [], loaded: false, error: '' },
      }
    },
    methods: {
      modelProviderOptions() {
        const values = (this.modelCatalog.providers || []).map(p => p.ref ? p.ref.id : p.id).filter(Boolean)
        if (this.modelSettings.provider && !values.includes(this.modelSettings.provider)) values.push(this.modelSettings.provider)
        return [...new Set(values)].map(value => ({ value, label: value === this.modelSettings.provider && !(this.modelCatalog.providers || []).some(p => (p.ref ? p.ref.id : p.id) === value) ? value + ' · external/unloaded (saved)' : value }))
      },
      modelProtocolOptions() {
        const pairs = this.modelCatalog.compatibilities || []
        const values = pairs.filter(p => p.provider && p.provider.id === this.modelSettings.provider).map(p => p.protocol.id)
        if (this.modelSettings.protocol && !values.includes(this.modelSettings.protocol)) values.push(this.modelSettings.protocol)
        return [...new Set(values)].map(value => ({ value, label: !(this.modelCatalog.protocols || []).some(p => (p.ref ? p.ref.id : p.id) === value) && value === this.modelSettings.protocol ? value + ' · external/unloaded (saved)' : value }))
      },
      modelProviderChanged() {
        const options = this.modelProtocolOptions()
        if (options.length && !options.some(option => option.value === this.modelSettings.protocol)) this.modelSettings.protocol = options[0].value
      },
      async loadModelSettings() {
        // Start from an empty view so a missing or failed reload cannot leave
        // stale editable configuration presented as the saved state. A secret
        // preview remains display-only and is only restored by a successful
        // response.
        Object.assign(this.modelSettings, {
          baseURL: '', model: '', allowedModels: '', apiKey: '',
          hasAPIKey: false, apiKeyPreview: '', clearAPIKey: false,
          provider: '', protocol: '', executable: false,
          executionStatus: '', executionReason: '',
        })
        this.modelCatalog.error = ''
        try {
          const catalog = await this.api('GET', '/v1/admin/model-runtimes')
          this.modelCatalog = { ...catalog, loaded: true, error: '' }
        } catch (_) { this.modelCatalog.loaded = false; this.modelCatalog.error = '模型目录不可用；保留当前配置，不自动回退。' }
        try {
          const data = await this.api('GET', '/v1/admin/model-settings/llm')
          if (!data.found) {
            const provider = (this.modelCatalog.defaults || [])[0]
            this.modelSettings.baseURL = ''
            this.modelSettings.model = ''
            this.modelSettings.allowedModels = ''
            this.modelSettings.provider = provider?.provider?.id || ''
            this.modelSettings.protocol = provider?.protocol?.id || ''
            this.modelSettings.executable = false
            this.modelSettings.executionStatus = ''
            this.modelSettings.executionReason = ''
            return
          }
          this.modelSettings.baseURL = data.base_url || ''
          this.modelSettings.model = data.model || ''
          this.modelSettings.provider = data.provider || ''
          this.modelSettings.protocol = data.protocol || ''
          this.modelSettings.allowedModels = (data.allowed_models || []).join(', ')
          this.modelSettings.hasAPIKey = !!data.has_api_key
          this.modelSettings.apiKeyPreview = data.api_key_preview || ''
          this.modelSettings.executable = !!data.executable
          this.modelSettings.executionStatus = data.execution_status || ''
          this.modelSettings.executionReason = data.execution_reason || ''
        } catch (e) { /* not configured yet */ }
      },
      modelSettingsBody() {
        const state = this.modelSettings
        const body = {
          base_url: state.baseURL,
          model: state.model,
          allowed_models: state.allowedModels.split(',').map(value => value.trim()).filter(Boolean),
          provider: state.provider, protocol: state.protocol,
        }
        if (state.clearAPIKey) body.clear_api_key = true
        else if (state.apiKey !== '') body.api_key = state.apiKey
        return body
      },
      async saveModelSettings() {
        await this.api('PUT', '/v1/admin/model-settings/llm', this.modelSettingsBody())
        this.modelSettings.apiKey = ''
        this.modelSettings.clearAPIKey = false
        await this.loadModelSettings()
        this.notify('LLM 配置已保存并对后续请求生效')
      },
    },
  }
})()
