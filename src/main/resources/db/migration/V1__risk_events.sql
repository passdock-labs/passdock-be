create table login_event (
  id uuid primary key,
  user_hash varchar(128) not null,
  device_id varchar(128) not null,
  region varchar(40) not null,
  result varchar(24) not null,
  reason varchar(120),
  created_at timestamptz not null default now()
);

create table risk_rule (
  id uuid primary key,
  name varchar(120) not null,
  condition_json jsonb not null,
  severity varchar(24) not null,
  enabled boolean not null default true
);

create table risk_alert (
  id uuid primary key,
  login_event_id uuid not null references login_event(id),
  rule_id uuid not null references risk_rule(id),
  severity varchar(24) not null,
  status varchar(24) not null,
  created_at timestamptz not null default now()
);

create table incident_note (
  id uuid primary key,
  alert_id uuid not null references risk_alert(id),
  note text not null,
  author varchar(80) not null,
  created_at timestamptz not null default now()
);
