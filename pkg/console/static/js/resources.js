// Resources domain for the embedded Console.
(function () {
  'use strict'

  window.HarnessConsoleResources = {
    state() {
      return {
        resources: [],
        res: { prefix: '', prefixInput: '', uploadKey: '' },
      }
    },
    methods: {
      async loadResources() {
        if (this.res.prefixInput && this.res.prefixInput !== this.res.prefix) this.res.prefix = this.res.prefixInput
        const prefixParam = this.res.prefix ? '?prefix=' + encodeURIComponent(this.res.prefix) : ''
        const data = await this.api('GET', '/v1/resources' + prefixParam)
        this.resources = data.resources || []
      },
      async uploadResource(event) {
        const file = event.target.files[0]
        if (!file) return
        const key = (this.res.uploadKey || this.res.prefix + file.name).replace(/^\/+/, '')
        const response = await fetch('/v1/resources/' + key, {
          method: 'PUT', headers: this.headers(), body: file,
        })
        if (!response.ok) { this.notify('上传失败 HTTP ' + response.status, true); return }
        this.notify('已上传 ' + key)
        this.res.prefix = key.includes('/') ? key.slice(0, key.lastIndexOf('/') + 1) : ''
        this.loadResources()
      },
      async downloadResource(key) {
        const response = await fetch('/v1/resources/' + key, { headers: this.headers() })
        if (!response.ok) { this.notify('下载失败', true); return }
        const blob = await response.blob()
        const link = document.createElement('a')
        link.href = URL.createObjectURL(blob)
        link.download = key.split('/').pop() || 'download'
        link.click()
        URL.revokeObjectURL(link.href)
      },
      async deleteResource(key) {
        await this.api('DELETE', '/v1/resources/' + key)
        this.notify('已删除 ' + key)
        this.loadResources()
      },
    },
  }
})()
