# HTTP Request n8n → `POST /site-new-lead`

Corpo JSON para configurar o node **HTTP Request** no n8n após o webhook do site. O site envia o lead ao n8n; o n8n acrescenta `codi_id` (28–36 dígitos cadastrados na aba **Leads Site**) e repassa ao projeto.

## Requisição

| Campo | Valor |
|-------|--------|
| Método | `POST` |
| URL | `https://<domínio-do-app>/site-new-lead` |
| `Content-Type` | `application/json` |
| Autenticação (opcional) | `Authorization: Bearer <SITE_LEAD_WEBHOOK_SECRET>` |

## Exemplo mínimo (roteamento)

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

## Exemplo quiz.persianas (`_flat` + `codi_id`)

O quiz envia ao n8n o objeto completo (`metadata`, `contact`, `produto`, `_flat`, …). No HTTP para `/site-new-lead`, use body plano com `codi_id` e os campos de `_flat`:

```json
{
  "codi_id": "32321675219277591366962199773271",
  "nome": "Maria Silva",
  "whatsapp": "5511999999999",
  "email": "maria@exemplo.com",
  "cidade": "São Paulo",
  "bairro": "Centro",
  "ambientes": "Sala, Quarto",
  "passo_1_intencao": "orcamento",
  "tipo": "Persiana",
  "modelo": "Rolô",
  "tecido": "Blackout",
  "acionamento": "Motorizada",
  "largura": "1,20",
  "altura": "1,50",
  "itens_adicionais": ""
}
```

## Exemplo taina-aci / rx-digital-lp / vita-audio

Payload típico recebido do site (o n8n pode repassar e garantir `codi_id` no topo):

```json
{
  "codi_id": "73058194261490732816540927385016",
  "form_id": "taina_vila_mariana_sp",
  "nome": "João",
  "telefone": "+5511999999999",
  "origem": "formulario-modal",
  "pagina": "https://endocrinologista.tainaaci.com.br/vila-mariana-sp",
  "data": "2026-05-08T00:00:00.000Z",
  "utm_source": "",
  "utm_medium": "",
  "utm_campaign": ""
}
```

```json
{
  "codi_id": "23215758164244868178558826641466",
  "form_id": "Rx Digital",
  "name": "João",
  "phone": "5511999999999",
  "utm_source": "google",
  "utm_medium": "cpc",
  "utm_campaign": "rx-lp",
  "createdAt": "2026-05-08T12:00:00.000Z"
}
```

```json
{
  "codi_id": "<codi_id_vita>",
  "form_id": "vita-audio-whatsapp",
  "nome": "Ana",
  "telefone": "5519999999999",
  "consentimento": true,
  "origem": "whatsapp-float",
  "pagina": "https://vitaaudio.com.br/",
  "enviadoEm": "2026-05-08T12:00:00.000Z",
  "utm_source": "",
  "utm_medium": ""
}
```

## Campos opcionais

- `traffic_source` ou `fonte`: força origem na mensagem (`meta`, `google`, `unknown`)
- UTMs: `utm_source`, `utm_medium`, `utm_campaign`, …
- `form_id` no JSON não substitui `codi_id` para roteamento neste endpoint

## Regras do servidor

- Roteamento **somente** por `codi_id` (28–36 dígitos numéricos); `page_id` neste endpoint é ignorado para rota
- `codi_id` deve existir em **Leads Site** (ativo, com `group_id`, `lead_phone_number`, `internal_notify_group_id`)
- Payload plano ou com objeto `data` aninhado (o servidor normaliza chaves comuns)
