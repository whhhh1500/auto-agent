// Shared Console API client. This file is vendored and embedded with the
// console; it deliberately has no remote imports or runtime dependencies.
(function () {
  'use strict'

  window.HarnessConsoleAPI = {
    // Login is intentionally unauthenticated, but still goes through the
    // shared JSON transport so headers and response parsing stay centralized.
    async requestUnauthenticatedJSON(method, path, body) {
      const response = await fetch(path, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
      })
      const data = await response.json().catch(() => ({}))
      return { response, data }
    },
    headers() {
      const h = { 'Content-Type': 'application/json', Authorization: 'Bearer ' + this.token }
      const subject = localStorage.getItem('hc_subject')
      const tenant = localStorage.getItem('hc_tenant')
      if (subject) h['X-Harness-Subject'] = subject
      if (tenant) h['X-Harness-Tenant'] = tenant
      return h
    },
    async api(method, path, body) {
      const res = await fetch(path, {
        method, headers: this.headers(), body: body === undefined ? undefined : JSON.stringify(body),
      })
      if (res.status === 401) {
        this.token = ''
        localStorage.removeItem('hc_token')
        throw new Error('登录过期')
      }
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        const message = (data && data.error) || ('HTTP ' + res.status)
        this.notify(message, true)
        throw new Error(message)
      }
      return data
    },
    notify(text, isErr) {
      this.toast = { show: true, err: !!isErr, text, timer: this.toast.timer }
      clearTimeout(this.toast.timer)
      this.toast.timer = setTimeout(() => { this.toast.show = false }, 2800)
    },
  }
})()
