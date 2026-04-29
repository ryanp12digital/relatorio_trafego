# Leads Site via n8n: contrato de payload

Este webhook aceita leads de site no mesmo endpoint de lead (`/meta-new-lead`), com roteamento por `codi_id`.

## Endpoint de produção

- URL completa do webhook para os formulários:
  `https://python-auto-relatorio-trafego.axmxa0.easypanel.host/meta-new-lead`

## Campos mínimos

- `codi_id`: identificador único do formulário no site com **exatamente 32 dígitos numéricos** (obrigatório para rota de site)
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

## Regras de decisão no webhook

1. Se chegar `page_id` válido, segue o roteamento Meta atual.
2. Se não houver `page_id` e existir `codi_id`, busca em `site_lead_routes` e monta o envio **a partir do cadastro da rota** (não depende de cliente Meta/Google por nome).
3. Sem `page_id` e sem `codi_id`, ou `codi_id` sem rota, o lead é bloqueado (sem fallback para cliente errado).

## Origem de tráfego (templates)

- Variáveis: `{{traffic_source}}` (valores: `meta`, `google` ou `unknown`) e `{{traffic_origin_url}}`.
- Inferência: `traffic_source` explícito no payload, depois UTMs, depois URL (ex. `gclid` / domínio Google = `google`), tokens de Meta (`fb`, `ig`, `facebook`… = `meta`). Se não houver sinal confiável, fica `unknown` (não adivinha Meta “por exclusão”).

## Organização de contexto (roteamento)

- **Contexto native_ads**: usa `page_id` como chave principal de roteamento (fluxo nativo Meta/Ads).
- **Contexto site**: usa `codi_id` como chave principal para leads vindos de formulário de site.
- `form_id` permanece disponível como chave nativa para futuras implementações, sem conflitar com `codi_id`.

## Observações práticas (n8n)

- Pode enviar payload plano (campos no topo do JSON) ou com `data`.
- O `codi_id` precisa existir na aba **Leads Site** do dashboard e o cadastro deve estar ativo, com `group_id`, `lead_phone_number` e `internal_notify_group_id` preenchidos.
- Se o `codi_id` não tiver 32 dígitos numéricos, o webhook bloqueia o lead (`CODI_ID_INVALID_FORMAT`).
- Opcional: `traffic_source` / `fonte` no JSON para forçar a origem exibida na mensagem.
