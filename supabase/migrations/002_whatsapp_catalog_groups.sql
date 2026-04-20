-- Catálogo de grupos WhatsApp (webhook Evolution + aba Pulseboard).
-- Executar no SQL Editor do Supabase após 001_initial_pulseboard.sql.

CREATE TABLE IF NOT EXISTS whatsapp_catalog_groups (
  group_jid TEXT PRIMARY KEY,
  subject TEXT NOT NULL DEFAULT '',
  monitoring_enabled BOOLEAN NOT NULL DEFAULT true,
  last_activity_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_event_type TEXT NOT NULL DEFAULT '',
  last_push_name TEXT NOT NULL DEFAULT '',
  last_preview TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_whatsapp_catalog_groups_activity
  ON whatsapp_catalog_groups (last_activity_at DESC);

COMMENT ON TABLE whatsapp_catalog_groups IS 'P12: grupos vistos via webhook Evolution (JID + subject)';
