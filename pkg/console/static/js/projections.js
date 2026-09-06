(function () {
  'use strict'
  window.HarnessConsoleProjections = {
    state() {
      return {
        ragProjection: null,
        ragProjectionRebuilding: false,
        memoryProjection: null,
        memoryProjectionRebuilding: false,
      }
    },
    methods: {
    async loadRagProjection() {
      if (this.identity.role !== 'admin') return
      const data = await this.api('GET', '/v1/admin/rag/projection')
      this.ragProjection = data.projection || null
    },
    async rebuildRagProjection() {
      if (this.identity.role !== 'admin' || this.ragProjectionRebuilding) return
      this.ragProjectionRebuilding = true
      try {
        const data = await this.api('POST', '/v1/admin/rag/projection/rebuild')
        this.ragProjection = data.projection || null
        this.notify('RAG 投影已从 canonical documents 重建')
        await this.loadRagProjection()
      } finally {
        this.ragProjectionRebuilding = false
      }
    },
    // ---- Memory projection maintenance (aggregate counts only) ----
    async loadMemoryProjection() {
      if (this.identity.role !== 'admin') return
      const data = await this.api('GET', '/v1/admin/memory/projection')
      this.memoryProjection = data.projection || null
    },
    async rebuildMemoryProjection() {
      if (this.identity.role !== 'admin' || this.memoryProjectionRebuilding) return
      this.memoryProjectionRebuilding = true
      try {
        const data = await this.api('POST', '/v1/admin/memory/projection/rebuild')
        this.memoryProjection = data.projection || null
        this.notify('Memory 投影已从 canonical entries 重建')
        await this.loadMemoryProjection()
      } finally {
        this.memoryProjectionRebuilding = false
      }
    },
    },
  }
})()
