alter table login_event
  add column if not exists device_changed boolean not null default false;

alter table risk_alert
  add column if not exists reason text not null default '';

insert into risk_rule (id, name, condition_json, severity, enabled)
values
  ('6ad2c9a9-61f2-45da-a2c2-4ec2a0f4f5f1', 'new-device-repeat-failure', '{"result":"FAILURE","deviceChanged":true}'::jsonb, 'HIGH', true),
  ('d2e4c2bc-0104-4a89-bb86-e02a892cc7f6', 'region-change-after-reset', '{"result":"RESET","regionNot":"KR"}'::jsonb, 'MEDIUM', true)
on conflict (id) do update
set condition_json = excluded.condition_json,
    severity = excluded.severity,
    enabled = excluded.enabled;

create index if not exists idx_login_event_created_at on login_event (created_at desc, id desc);
create index if not exists idx_risk_alert_created_at on risk_alert (created_at desc, id desc);
create index if not exists idx_risk_alert_status on risk_alert (status);
