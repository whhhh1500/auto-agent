// Authentication domain for the embedded Console.
(function () {
  'use strict'

  window.HarnessConsoleAuth = {
    state() {
      return {
        token: localStorage.getItem('hc_token') || '',
        mustChangePassword: localStorage.getItem('hc_must_change_password') === 'true',
        identity: {
          account: localStorage.getItem('hc_account') || localStorage.getItem('hc_email') || '',
          role: localStorage.getItem('hc_role') || '-',
          tenant: localStorage.getItem('hc_tenant') ||
            (localStorage.getItem('hc_role') === 'admin' ? 'system' : '-'),
        },
        login: { account: '', password: '', error: '' },
        activation: { password: '', confirm: '', error: '' },
      }
    },
    methods: {
      ensureIdentityTenant() {
        // Platform administrators own the system notification namespace. This
        // repairs sessions created by older Console builds that persisted a
        // bearer token without persisting its tenant field.
        const tenant = String(this.identity?.tenant || '').trim()
        if (this.identity?.role === 'admin' && (!tenant || tenant === '-')) {
          this.identity.tenant = 'system'
          localStorage.setItem('hc_tenant', 'system')
        }
      },
      async doLogin() {
        try {
          const { response, data } = await this.requestUnauthenticatedJSON(
            'POST', '/v1/auth/login',
            { account: this.login.account, password: this.login.password },
          )
          if (!response.ok) { this.login.error = data.error || '登录失败'; return }
          this.token = data.token
          this.mustChangePassword = !!data.must_change_password
          this.identity = { account: data.account, role: data.role, tenant: data.tenant }
          localStorage.setItem('hc_token', this.token)
          localStorage.setItem('hc_account', data.account)
          localStorage.setItem('hc_must_change_password', String(this.mustChangePassword))
          localStorage.setItem('hc_role', data.role)
          localStorage.setItem('hc_tenant', data.tenant || (data.role === 'admin' ? 'system' : '-'))
          this.login = { account: '', password: '', error: '' }
          this.boot()
        } catch (e) { this.login.error = String(e) }
      },
      async activateAccount() {
        this.activation.error = ''
        if (this.activation.password.length < 12) { this.activation.error = '新密码至少 12 位'; return }
        if (this.activation.password !== this.activation.confirm) { this.activation.error = '两次输入的密码不一致'; return }
        try {
          // Keep the UI-only error out of the wire payload.
          const payload = { password: this.activation.password, confirm: this.activation.confirm }
          const data = await this.api('POST', '/v1/auth/activate', payload)
          this.token = data.token
          this.mustChangePassword = false
          localStorage.setItem('hc_token', this.token)
          localStorage.setItem('hc_must_change_password', 'false')
          this.activation = { password: '', confirm: '', error: '' }
          this.boot()
        } catch (e) { this.activation.error = String(e) }
      },
      async logout() {
        try { await this.api('POST', '/v1/auth/logout') } catch (e) {}
        this.token = ''
        this.mustChangePassword = false
        localStorage.removeItem('hc_token')
        localStorage.removeItem('hc_account')
        localStorage.removeItem('hc_email')
        localStorage.removeItem('hc_must_change_password')
        localStorage.removeItem('hc_role')
        localStorage.removeItem('hc_tenant')
      },
    },
  }
})()
