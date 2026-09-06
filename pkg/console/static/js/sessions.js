// Sessions domain for the embedded Console: list and event inspection.
(function () {
  'use strict'

  window.HarnessConsoleSessions = {
    state() {
      return {
        sessions: [],
        sessionEvents: [],
        sess: { profile: '', prefix: '', sort: 'desc', limit: 20, offset: 0, total: 0 },
      }
    },
    methods: {
      async loadSessions() {
        const params = new URLSearchParams({
          profile: this.sess.profile, prefix: this.sess.prefix,
          sort: this.sess.sort, limit: String(this.sess.limit), offset: String(this.sess.offset),
        })
        const data = await this.api('GET', '/v1/sessions?' + params)
        this.sessions = data.sessions || []
        this.sess.total = data.total ?? this.sessions.length
      },
      async viewSession(id) {
        this.sessionEvents = (await this.api('GET', '/v1/sessions/' + id + '/events')).events || []
      },
    },
  }
})()
