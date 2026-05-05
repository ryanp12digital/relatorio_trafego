# Leads Site via n8n: contrato de payload

Use **sempre** `POST /site-new-lead` para leads de formulário por `codi_id`. O endpoint `/meta-new-lead` aceita **apenas** roteamento por `page_id` (Meta).

## Endpoint de produção

- URL do webhook de **site** (substitui pelo teu domínio):
  `https://<domínio-do-app>/site-new-lead`

## Campos mínimos

- `codi_id`: identificador único do formulário no site com **28 a 36 dígitos numéricos** (obrigatório para rota de site; na prática costuma ter 30–32)
- `nome`
- `telefone`
- `origem`
- `pagina`
- `data`

## Exemplo recomendado

```json
{
  "codi_id": "12345678901234567890123456789012",
  "nome": "Teste",
  "telefone": "(11)99999-9999",
  "origem": "google",
  "pagina": "https://dominio.com/landing-x",
  "data": "2026-04-24T20:58:58.430Z"
}
```

## Regras no `/site-new-lead`

1. O endpoint **só** usa `codi_id` (28–36 dígitos): procura em `site_lead_routes` e monta o envio. Um `page_id` no mesmo JSON **não** é usado para rota (evita confusão com payloads n8n/Make).
2. `codi_id` inválido ou sem rota cadastrada → lead ignorado (resposta HTTP com lead em `skipped` / logs `CODI_ID_*`).
3. Leads **Meta** (`page_id`) devem ir para `POST /meta-new-lead`. Leads **Google Ads** (`google_customer_id`) para `POST /google-new-lead`.

## Origem de tráfego (templates)

- Variáveis: `{{traffic_source}}` (valores: `meta`, `google` ou `unknown`) e `{{traffic_origin_url}}`.
- Inferência: `traffic_source` explícito no payload, depois UTMs, depois URL (ex. `gclid` / domínio Google = `google`), tokens de Meta (`fb`, `ig`, `facebook`… = `meta`). Se não houver sinal confiável, fica `unknown` (não adivinha Meta “por exclusão”).

## Organização de contexto (roteamento)

- **Lead site**: `codi_id` em `POST /site-new-lead`.
- **Meta / Google**: endpoints dedicados (`/meta-new-lead`, `/google-new-lead`).
- `form_id` no JSON do site pode existir por compatibilidade com integrações; não substitui `codi_id` para rota neste endpoint.

## Observações práticas (n8n)

- Pode enviar payload plano (campos no topo do JSON) ou com `data`.
- O `codi_id` precisa existir na aba **Leads Site** do dashboard e o cadastro deve estar ativo, com `group_id`, `lead_phone_number` e `internal_notify_group_id` preenchidos.
- Se o `codi_id` não tiver entre 28 e 36 dígitos numéricos, o webhook bloqueia o lead (`CODI_ID_INVALID_FORMAT`).
- Opcional: `traffic_source` / `fonte` no JSON para forçar a origem exibida na mensagem.
