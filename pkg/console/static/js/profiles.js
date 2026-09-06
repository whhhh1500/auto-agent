(function () {
  'use strict'
  window.HarnessConsoleProfiles = {
    state() {
      return {
        profiles: [],
        releases: [],
        profileEdit: { id: 'general', copyID: '', scopeText: 'global:global', name: '', description: '', provider: '', model: '', executionMode: 'sequential', addCapabilities: '', removeCapabilities: '', fragmentsJSON: '[]', layerJSON: '', effective: null, error: '' },
        pub: { profile: '', scopeText: '', fragId: '', section: 'instructions', content: '', rollbackTo: 0 },
      }
    },
    methods: {
    async loadProfiles() {
      this.profiles = (await this.api('GET', '/v1/profiles')).profiles || []
      if (!this.pub.profile && this.profiles.length) this.pub.profile = this.profiles[0]
      if (this.profiles.includes(this.profileEdit.id)) await this.loadProfileEditor()
      await this.loadReleases()
    },
    async loadReleases() {
      if (!this.pub.profile) { this.releases = []; return }
      this.releases = (await this.api('GET', '/v1/profiles/' + this.pub.profile + '/releases')).releases || []
    },
    profileScopeQuery() {
      return encodeURIComponent(JSON.stringify(this.parseScope(this.profileEdit.scopeText)))
    },
    profileLayerFromFields() {
      try {
        const layer = JSON.parse(this.profileEdit.layerJSON || '{}')
        if (!layer || Array.isArray(layer) || typeof layer !== 'object') throw new Error('Layer 必须是 JSON 对象')
        layer.profile_id = this.profileEdit.id
        if (this.profileEdit.name !== '') layer.name = this.profileEdit.name
        else delete layer.name
        if (this.profileEdit.description !== '') layer.description = this.profileEdit.description
        else delete layer.description
        if (this.profileEdit.provider || this.profileEdit.model) {
          layer.model = { ...(layer.model || {}) }
          if (this.profileEdit.provider) layer.model.provider = this.profileEdit.provider
          else delete layer.model.provider
          if (this.profileEdit.model) layer.model.model = this.profileEdit.model
          else delete layer.model.model
        } else delete layer.model
        const add = this.csv(this.profileEdit.addCapabilities), remove = this.csv(this.profileEdit.removeCapabilities)
        if (add.length) layer.add_capabilities = add
        else delete layer.add_capabilities
        if (remove.length) layer.remove_capabilities = remove
        else delete layer.remove_capabilities
        const fragments = JSON.parse(this.profileEdit.fragmentsJSON || '[]')
        if (!Array.isArray(fragments)) throw new Error('put_fragments 必须是数组')
        if (fragments.length) layer.put_fragments = fragments
        else delete layer.put_fragments
        this.profileSetExecutionMetadata(layer)
        this.profileEdit.layerJSON = JSON.stringify(layer, null, 2)
        this.profileEdit.error = ''
      } catch (e) {
        this.profileEdit.error = 'Layer 字段解析失败: ' + e.message
        this.notify(this.profileEdit.error, true)
      }
    },
    profileFieldsFromLayer(layer) {
      const model = layer.model || {}
      this.profileEdit.name = layer.name || ''
      this.profileEdit.description = layer.description || ''
      this.profileEdit.provider = model.provider || ''
      this.profileEdit.model = model.model || ''
      this.profileEdit.addCapabilities = (layer.add_capabilities || []).join(', ')
      this.profileEdit.removeCapabilities = (layer.remove_capabilities || []).join(', ')
      this.profileEdit.fragmentsJSON = JSON.stringify(layer.put_fragments || [], null, 2)
      if (layer.metadata !== undefined && (!layer.metadata || Array.isArray(layer.metadata) || typeof layer.metadata !== 'object')) throw new Error('metadata 必须是 JSON 对象')
      const metadata = layer.metadata || {}
      if (metadata['harness.executor.id'] === 'graph-core-turn' && metadata['harness.executor.version'] === '1') this.profileEdit.executionMode = 'graph'
      else if (metadata['harness.executor.id'] === undefined && metadata['harness.executor.version'] === undefined) this.profileEdit.executionMode = 'sequential'
      else this.profileEdit.executionMode = 'custom'
    },
    profileSetExecutionMetadata(layer) {
      if (layer.metadata !== undefined && (!layer.metadata || Array.isArray(layer.metadata) || typeof layer.metadata !== 'object')) throw new Error('metadata 必须是 JSON 对象')
      layer.metadata = { ...(layer.metadata || {}) }
      if (this.profileEdit.executionMode === 'graph') {
        layer.metadata['harness.executor.id'] = 'graph-core-turn'
        layer.metadata['harness.executor.version'] = '1'
      } else if (this.profileEdit.executionMode === 'sequential') {
        delete layer.metadata['harness.executor.id']
        delete layer.metadata['harness.executor.version']
      }
      if (!Object.keys(layer.metadata).length) delete layer.metadata
    },
    profileApplyExecutionMode() {
      try {
        const layer = JSON.parse(this.profileEdit.layerJSON || '{}')
        if (!layer || Array.isArray(layer) || typeof layer !== 'object') throw new Error('Layer 必须是 JSON 对象')
        this.profileSetExecutionMetadata(layer)
        this.profileEdit.layerJSON = JSON.stringify(layer, null, 2)
        this.profileEdit.error = ''
      } catch (e) {
        this.profileEdit.error = 'Layer JSON 解析失败: ' + e.message
        this.notify(this.profileEdit.error, true)
      }
    },
    async loadProfileEditor() {
      this.profileEdit.error = ''
      try {
        const data = await this.api('GET', '/v1/admin/profiles/' + encodeURIComponent(this.profileEdit.id) + '?scope=' + this.profileScopeQuery())
        const layer = data.layer || { profile_id: this.profileEdit.id }
        this.profileEdit.layerJSON = JSON.stringify(layer, null, 2)
        this.profileEdit.effective = data.effective || null
        this.profileFieldsFromLayer(layer)
      } catch (e) { this.profileEdit.error = String(e.message || e) }
    },
    copyProfileEditor() {
      try {
        const layer = JSON.parse(this.profileEdit.layerJSON || '{}')
        layer.profile_id = this.profileEdit.copyID
        this.profileEdit.id = this.profileEdit.copyID
        this.profileEdit.layerJSON = JSON.stringify(layer, null, 2)
        this.profileFieldsFromLayer(layer)
        this.profileEdit.effective = null
        this.profileEdit.error = ''
      } catch (e) { this.profileEdit.error = 'Layer JSON 解析失败: ' + e.message; this.notify(this.profileEdit.error, true) }
    },
    async saveProfileEditor() {
      this.profileEdit.error = ''
      try {
        const layer = JSON.parse(this.profileEdit.layerJSON || '{}')
        if (!layer || Array.isArray(layer) || typeof layer !== 'object') throw new Error('Layer 必须是 JSON 对象')
        if (layer.profile_id && layer.profile_id !== this.profileEdit.id) throw new Error('profile_id 必须与路径 ID 一致')
        layer.profile_id = this.profileEdit.id
        const data = await this.api('PUT', '/v1/admin/profiles/' + encodeURIComponent(this.profileEdit.id), { scope: this.parseScope(this.profileEdit.scopeText), layer })
        this.profileEdit.layerJSON = JSON.stringify(data.layer || layer, null, 2)
        this.profileEdit.effective = data.effective || null
        this.profileFieldsFromLayer(data.layer || layer)
        this.notify('Profile Layer 已保存')
        await this.loadProfiles()
      } catch (e) { this.profileEdit.error = String(e.message || e); this.notify(this.profileEdit.error, true) }
    },
    async publish() {
      const layer = { profile_id: this.pub.profile }
      if (this.pub.fragId && this.pub.content) {
        layer.put_fragments = [{ id: this.pub.fragId, section: this.pub.section, content: this.pub.content }]
      }
      await this.api('POST', '/v1/profiles/' + this.pub.profile + '/publish', { scope: this.parseScope(this.pub.scopeText), layer })
      this.notify('已发布新版本')
      this.loadReleases()
    },
    async rollback() {
      await this.api('POST', '/v1/profiles/' + this.pub.profile + '/rollback', { to_version: parseInt(this.pub.rollbackTo || '0', 10) })
      this.notify('已回滚')
      this.loadReleases()
    },
    },
  }
})()
