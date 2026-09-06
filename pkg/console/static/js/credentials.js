// Credentials domain for the embedded Console.
(function () {
  'use strict'

  window.HarnessConsoleCredentials = {
    state() {
      return {
        credentials: [],
        cred: { scopeText: '', ref: '', kind: 'static', value: '' },
      }
    },
    methods: {
      async loadCredentials() {
        this.credentials = (await this.api('GET', '/v1/admin/credentials')).credentials || []
      },
      async bindCredential() {
        await this.api('POST', '/v1/admin/credentials', {
          scope: this.parseScope(this.cred.scopeText), ref: this.cred.ref,
          kind: this.cred.kind, value: this.cred.value, mode: 'provide',
        })
        // Clear the secret immediately; it is never persisted or rendered back.
        this.cred.value = ''
        this.notify('凭证已绑定（值不回显）')
        this.load('credentials')
      },
    },
  }
})()
