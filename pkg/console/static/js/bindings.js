// Dynamic bindings domain for the embedded Console.
(function () {
  'use strict'

  window.HarnessConsoleBindings = {
    state() {
      return { bindings: [] }
    },
    methods: {
      async loadBindings() {
        this.bindings = (await this.api('GET', '/v1/admin/bindings')).bindings || []
      },
      async unbind(id) {
        await this.api('DELETE', '/v1/admin/bindings/' + id)
        this.notify('绑定已解除')
        this.load('bindings')
      },
    },
  }
})()
