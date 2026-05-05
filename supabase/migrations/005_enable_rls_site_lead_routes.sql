-- Supabase linter: tabela em public sem RLS (PostgREST). Sem políticas = sem acesso via API anon até definires políticas.
ALTER TABLE site_lead_routes ENABLE ROW LEVEL SECURITY;
