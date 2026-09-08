// Display catalog only: never installs, registers, or authorizes a sender.
// Source: nikoksr/notify v1.6.0 README and service/ tree, checked 2026-09-08.
// https://github.com/nikoksr/notify/tree/v1.6.0/service
// https://github.com/nikoksr/notify/blob/v1.6.0/README.md#supported-services
// LINE Notify shutdown: https://notify-bot.line.me/ (2025-03-31).
// version is a suggested auto-agent channel version, not an upstream SDK version.
(function () {
  'use strict'

  const entries = [
    ['amazonses', 'Amazon SES'],
    ['amazonsns', 'Amazon SNS'],
    ['bark', 'Bark'],
    ['dingding', 'DingTalk / 钉钉'],
    ['discord', 'Discord'],
    ['mail', 'Email / 邮件'],
    ['fcm', 'Firebase Cloud Messaging'],
    ['googlechat', 'Google Chat'],
    ['http', 'HTTP'],
    ['lark', 'Lark / 飞书'],
    ['line', 'LINE'],
    ['linenotify', 'LINE Notify', 'discontinued', '官方已于 2025-03-31 停止服务'],
    ['mailgun', 'Mailgun'],
    ['mailtrap', 'Mailtrap'],
    ['matrix', 'Matrix'],
    ['mattermost', 'Mattermost'],
    ['msteams', 'Microsoft Teams'],
    ['pagerduty', 'PagerDuty'],
    ['plivo', 'Plivo'],
    ['pushover', 'Pushover'],
    ['pushbullet', 'Pushbullet'],
    ['reddit', 'Reddit'],
    ['rocketchat', 'Rocket.Chat'],
    ['sendgrid', 'SendGrid'],
    ['slack', 'Slack'],
    ['syslog', 'Syslog'],
    ['telegram', 'Telegram'],
    ['textmagic', 'TextMagic'],
    ['twilio', 'Twilio'],
    ['twitter', 'Twitter / X'],
    ['viber', 'Viber'],
    ['wechat', 'WeChat / 微信'],
    ['webpush', 'Web Push'],
    ['whatsapp', 'WhatsApp', 'unsupported', 'notify v1.6.0 将此服务标为不支持'],
  ]

  window.HarnessConsoleNotificationPlatforms = {
    sourceVersion: 'v1.6.0',
    platforms: entries.map(([id, name, status = 'available', description = '']) => ({
      id, version: '1', name, status, description,
    })),
  }
})()
