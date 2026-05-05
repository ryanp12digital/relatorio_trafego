-- Origens CORS por cadastro Leads Site (merge com META_LEAD_WEBHOOK_CORS_ORIGINS no worker).
ALTER TABLE site_lead_routes
  ADD COLUMN IF NOT EXISTS cors_allowed_origins jsonb NOT NULL DEFAULT '[]'::jsonb;
