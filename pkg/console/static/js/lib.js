// Small shared Console helpers. Keep transport and domain behavior out of
// this file so the helpers can be reused by independently loaded domains.
(function () {
  'use strict'

  window.HarnessConsoleLib = {
    parseScope(text) {
      return String(text || '').split('/').filter(Boolean).map(part => {
        const i = part.indexOf(':')
        return i < 0 ? { kind: part, id: part } : { kind: part.slice(0, i), id: part.slice(i + 1) }
      })
    },
    scopeText(scope) { return (scope || []).map(s => s.kind + ':' + s.id).join('/') },
    csv(text) { return String(text || '').split(',').map(s => s.trim()).filter(Boolean) },
  }
})()
