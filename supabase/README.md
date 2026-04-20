# Supabase / Postgres (Pulseboard)

## 1. Tabelas (obrigatório antes de `DATABASE_URL` em produção)

No painel Supabase: **SQL Editor** → cole o conteúdo de `migrations/001_initial_pulseboard.sql` → **Run**.

> O MCP do Cursor com `read_only=true` **não** executa `CREATE TABLE`. Ou usas o SQL Editor, ou alteras a URL do MCP no Cursor para `read_only=false` (se a tua organização permitir).

## 2. Connection string (Easypanel)

**Project Settings → Database** → copiar a URI do **Transaction pooler** (modo **Transaction**, porta típica **6543**).

No Easypanel, variável de ambiente:

`DATABASE_URL=postgresql://postgres.[PROJECT-REF]:[SENHA]@aws-0-[REGION].pooler.supabase.com:6543/postgres`

(Substitui `[PROJECT-REF]`, `[SENHA]`, `[REGION]` pelos valores do painel.)

## 3. Primeiro deploy

Com tabelas vazias, no primeiro arranque o contentor **importa** de `data/`: `clients.json`, `google_clients.json`, `message_templates.json` (se existirem).

## 4. Auth (e-mail só interno, só login)

No Supabase (**Authentication**):

- **Providers → Email**: desliga **Confirm email** (confirmação), se crias utilizadores à mão ou por API.
- Para **não** permitir registo público: nas definições de Auth (nome varia com a versão do painel), desativa **sign ups** públicos ou restringe a convites; utilizadores novos passam a ser criados só no Dashboard Supabase ou via API com `service_role` no servidor (nunca no browser).

Variáveis opcionais para integração futura no código (não obrigatórias para o fluxo actual com `DASHBOARD_AUTH_PASSWORD`):

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY` (só front com RLS)
- `SUPABASE_SERVICE_ROLE_KEY` (só servidor; nunca no front)
