{
  local thanos = self,
  rule+:: {
    jobPrefix: error 'must provide job prefix for Thanos Rule alerts',
    selector: error 'must provide selector for Thanos Rule alerts',
  },
  prometheusAlerts+:: {
    groups+: [
      {
        name: 'thanos-rule.rules',
        rules: [
          {
            alert: 'ThanosRuleQueueIsDroppingAlerts',
            annotations: {
              message: 'Thanos Rule {{$labels.job}} {{$labels.pod}} is failing to queue alerts.',
            },
            expr: |||
              sum by (job) (rate(thanos_alert_queue_alerts_dropped_total{%(selector)s}[5m])) > 0
            ||| % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosRuleSenderIsFailingAlerts',
            annotations: {
              message: 'Thanos Rule {{$labels.job}} {{$labels.pod}} is failing to send alerts to alertmanager.',
            },
            expr: |||
              sum by (job) (rate(thanos_alert_sender_alerts_dropped_total{%(selector)s}[5m])) > 0
            ||| % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosRuleHighRuleEvaluationFailures',
            annotations: {
              message: 'Thanos Rule {{$labels.job}} {{$labels.pod}} is failing to evaluate rules.',
            },
            expr: |||
              (
                sum by (job) (rate(prometheus_rule_evaluation_failures_total{%(selector)s}[5m]))
              /
                sum by (job) (rate(prometheus_rule_evaluations_total{%(selector)s}[5m]))
              * 100 > 5
              )
            ||| % thanos.rule,

            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosRuleHighRuleEvaluationWarnings',
            annotations: {
              message: 'Thanos Rule {{$labels.job}} {{$labels.pod}} has high number of evaluation warnings.',
            },
            expr: |||
              sum by (job) (rate(thanos_rule_evaluation_with_warnings_total{%(selector)s}[5m])) > 0
            ||| % thanos.rule,

            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosRuleRuleEvaluationLatencyHigh',
            annotations: {
              message: 'Thanos Rule {{$labels.job}}/{{$labels.pod}} has higher evaluation latency than interval for {{$labels.rule_group}}.',
            },
            expr: |||
              (
                sum by (job, pod, rule_group) (prometheus_rule_group_last_duration_seconds{%(selector)s})
              >
                sum by (job, pod, rule_group) (prometheus_rule_group_interval_seconds{%(selector)s})
              )
            ||| % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosRuleGrpcErrorRate',
            annotations: {
              message: 'Thanos Rule {{$labels.job}} is failing to handle {{ $value | humanize }}% of requests.',
            },
            expr: |||
              (
                sum by (job) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(selector)s}[5m]))
              /
                sum by (job) (rate(grpc_server_started_total{%(selector)s}[5m]))
              * 100 > 5
              )
            ||| % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosRuleConfigReloadFailure',
            annotations: {
              message: 'Thanos Rule {{$labels.job}} has not been able to reload its configuration.',
            },
            expr: 'avg(thanos_rule_config_last_reload_successful{%(selector)s}) by (job) != 1' % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosRuleQueryHighDNSFailures',
            annotations: {
              message: 'Thanos Rule {{$labels.job}} have {{ $value | humanize }}% of failing DNS queries for query endpoints.',
            },
            expr: |||
              (
                sum by (job) (rate(thanos_ruler_query_apis_dns_failures_total{%(selector)s}[5m]))
              /
                sum by (job) (rate(thanos_ruler_query_apis_dns_lookups_total{%(selector)s}[5m]))
              * 100 > 1
              )
            ||| % thanos.rule,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosRuleAlertmanagerHighDNSFailures',
            annotations: {
              message: 'Thanos Rule {{$labels.job}} have {{ $value | humanize }}% of failing DNS queries for Alertmanager endpoints.',
            },
            expr: |||
              (
                sum by (job) (rate(thanos_ruler_alertmanagers_dns_failures_total{%(selector)s}[5m]))
              /
                sum by (job) (rate(thanos_ruler_alertmanagers_dns_lookups_total{%(selector)s}[5m]))
              * 100 > 1
              )
            ||| % thanos.rule,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
        ],
      },
    ],
  },
}
