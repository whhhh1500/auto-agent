// Console composition root. Each domain owns its state/methods; all keys are
// checked in one namespace so a later domain cannot silently overwrite one.
(function () {
  'use strict'

  // Explicit contracts: window.HarnessConsoleAuth.state(), window.HarnessConsoleAccounts.state(),
  // window.HarnessConsoleAudit.state(), window.HarnessConsoleObservability.state(),
  // window.HarnessConsolePolicies.state(), window.HarnessConsoleCredentials.state(),
  // window.HarnessConsoleResources.state(), window.HarnessConsoleSessions.state(),
  // window.HarnessConsolePlayground.state(), window.HarnessConsoleBindings.state(),
  // window.HarnessConsoleStorageSettings.state(), window.HarnessConsoleModelSettings.state(),
  // window.HarnessConsoleNotificationTargets.state(), window.HarnessConsoleSandbox.state().
  const domains = [
    window.HarnessConsoleAuth,
    window.HarnessConsoleShell,
    window.HarnessConsoleCapabilities,
    window.HarnessConsoleProfiles,
    window.HarnessConsoleApprovals,
    window.HarnessConsoleEvaluations,
    window.HarnessConsoleDelegations,
    window.HarnessConsoleRunnerTasks,
    window.HarnessConsoleProjections,
    window.HarnessConsoleAccounts,
    window.HarnessConsoleAudit,
    window.HarnessConsoleObservability,
    window.HarnessConsolePolicies,
    window.HarnessConsoleCredentials,
    window.HarnessConsoleResources,
    window.HarnessConsoleSessions,
    window.HarnessConsolePlayground,
    window.HarnessConsoleBindings,
    window.HarnessConsoleStorageSettings,
    window.HarnessConsoleModelSettings,
    window.HarnessConsoleNotificationTargets,
    window.HarnessConsoleSandbox,
  ]

  const appMethods = {
    boot() {
      if (!this.token || this.mustChangePassword) return
      if (!this._consoleHashBound) {
        window.addEventListener('hashchange', () => this.go(location.hash.slice(1) || 'overview'))
        this._consoleHashBound = true
      }
      this.go(location.hash.slice(1) || 'overview')
    },
    go(id) {
      if (!this.visibleNav.find(n => n.id === id)) id = 'overview'
      this.view = id
      if (location.hash.slice(1) !== id) location.hash = id
      this.load(id)
    },
  }

  window.HarnessConsoleApp = { methods: appMethods }

  const compose = () => {
    const composed = {}
    const keys = new Set()
    const add = (source, label) => {
      if (!source) return
      for (const key of Object.keys(source)) {
        if (keys.has(key)) throw new Error('Console domain key collision: ' + label + '.' + key)
        keys.add(key)
        composed[key] = source[key]
      }
    }

    for (const domain of domains) add(typeof domain?.state === 'function' ? domain.state() : domain?.state, 'state')
    add(window.HarnessConsoleAPI, 'api')
    add(window.HarnessConsoleLib, 'lib')
    for (const domain of domains) {
      const methods = typeof domain?.methods === 'function' ? domain.methods() : domain?.methods
      add(methods, 'methods')
    }
    add(appMethods, 'app')
    if (keys.has('visibleNav')) throw new Error('Console domain key collision: visibleNav')
    keys.add('visibleNav')
    Object.defineProperty(composed, 'visibleNav', {
      enumerable: true,
      configurable: true,
      get() {
        return this.nav.filter(item =>
          (!item.adminOnly || this.identity.role === 'admin') &&
          (!item.operatorOnly || this.identity.role === 'admin' || this.identity.role === 'tenant_admin'))
      },
    })
    return composed
  }

  window.HarnessConsoleApp.compose = compose

  const register = () => {
    if (!window.Alpine) return
    Alpine.data('console', compose)
  }

  if (window.Alpine) register()
  else document.addEventListener('alpine:init', register, { once: true })
})()
