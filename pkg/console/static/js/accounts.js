// Accounts/Tenants domain for the embedded Console.
(function () {
  'use strict'

  window.HarnessConsoleAccounts = {
    state() {
      return {
        accounts: [],
        tenants: [],
        acct: { account: '', email: '', password: '', role: 'user', tenant: 'default' },
        tn: { id: '', name: '' },
      }
    },
    methods: {
      async loadAccounts() {
        this.accounts = (await this.api('GET', '/v1/admin/accounts')).accounts || []
        this.tenants = (await this.api('GET', '/v1/admin/tenants')).tenants || []
      },
      async createAccount() {
        await this.api('POST', '/v1/admin/accounts', {
          account: this.acct.account, email: this.acct.email, password: this.acct.password,
          role: this.acct.role, tenant_id: this.acct.tenant,
        })
        this.notify('账号已创建')
        this.load('accounts')
      },
      async toggleAccount(account) {
        await this.api(
          'POST',
          '/v1/admin/accounts/' + encodeURIComponent(account.account) + '/status',
          { status: account.status === 'active' ? 'disabled' : 'active' },
        )
        this.load('accounts')
      },
      async resetPassword(account) {
        const password = prompt('为 ' + account.account + ' 设置新密码（至少 8 位）')
        if (!password) return
        await this.api(
          'POST',
          '/v1/admin/accounts/' + encodeURIComponent(account.account) + '/password',
          { password },
        )
        this.notify('密码已更新')
      },
      async createTenant() {
        await this.api('POST', '/v1/admin/tenants', { id: this.tn.id, name: this.tn.name })
        this.notify('租户已创建')
        this.load('accounts')
      },
    },
  }
})()
