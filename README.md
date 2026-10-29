# Parallelization: agente de planejamento de viagens

Demonstração visual do padrão **Parallelization** do livro *Agentic Design Patterns*, feita com [Agno](https://docs.agno.com).

> **Use Case:** A travel planning agent.
> – Parallel Tasks: Check flight prices, search for hotel availability, look up local events, and find restaurant recommendations concurrently.
> – Benefit: Presents a complete travel plan faster.

A tela principal roda as mesmas 4 tarefas lado a lado:

| Agente único | Parallelization |
|---|---|
| 1 agente generalista com as 4 ferramentas | 4 subagentes especialistas, cada um com 1 ferramenta |
| `Workflow` com steps **em sequência** | `Workflow` com `Parallel(...)` |
| voos → hotéis → eventos → restaurantes → síntese | [voos ‖ hotéis ‖ eventos ‖ restaurantes] → síntese |

Nos dois casos um **Agente Planejador** junta as pesquisas no plano final. A linha do tempo usa a mesma escala nos dois lados, então dá para ver a diferença de tempo.

## Fontes de dados (APIs de terceiros)

Cada ferramenta consulta uma API real quando a chave dela está configurada no servidor:

| Tarefa | API | Variável de ambiente | Observações |
|---|---|---|---|
| Voos | [SerpApi · Google Flights](https://serpapi.com/google-flights-api) | `SERPAPI_API_KEY` | O agente converte as cidades em códigos IATA. Mostra os horários do voo de ida e o preço total ida e volta (1 adulto). |
| Hotéis | [Booking.com via RapidAPI (DataCrawler)](https://rapidapi.com/DataCrawler/api/booking-com15) | `RAPIDAPI_KEY` | API não oficial. Faz 2 chamadas: `searchDestination` e depois `searchHotels`. |
| Eventos | [Ticketmaster Discovery](https://developer.ticketmaster.com/products-and-docs/apis/discovery-api/v2/) | `TICKETMASTER_API_KEY` | Busca num raio de 30 km das coordenadas da cidade. A cobertura varia muito por país. |
| Restaurantes | [Yelp Fusion](https://docs.developer.yelp.com/reference/v3_business_search) | `YELP_API_KEY` | Paga, com teste de 30 dias. A cobertura fora dos EUA/Europa é limitada. |

A Amadeus Self-Service (sugestão original para voos) foi desativada em julho de 2026, por isso os voos usam a SerpApi.

**Fallback:** sem a chave, ou se a API falhar ou devolver um formato inesperado, a ferramenta usa dados simulados (com 2 a 3,5 s de espera) e inclui um `aviso`. O agente informa que os dados são simulados, e a tela mostra a fonte de cada tarefa (`GET /api/sources`). As chaves nunca são devolvidas ao navegador nem aparecem nas mensagens de erro.

## Código principal (`api/index.py`)

```python
# Agente único: um step depois do outro
Workflow(steps=[voos, hoteis, eventos, restaurantes, sintese])

# Parallelization: os 4 subagentes ao mesmo tempo, depois a agregação
Workflow(steps=[Parallel(voos, hoteis, eventos, restaurantes, name="pesquisas"), sintese])
```

O backend transmite os eventos do Agno (`StepStarted` / `StepCompleted`) por SSE, e o front-end anima o diagrama e a linha do tempo em tempo real.

## Provedores e chave de API

A interface só é liberada depois que a pessoa escolhe o provedor e informa uma chave, que é validada em `/api/validate`:

| Provedor | Chave | Modelos oferecidos |
|---|---|---|
| OpenAI | `sk-...` | gpt-4.1-mini, gpt-4.1-nano, gpt-4o-mini, gpt-4.1, gpt-4o |
| [Groq](https://console.groq.com/keys) | `gsk_...` | openai/gpt-oss-120b, openai/gpt-oss-20b, llama-3.3-70b-versatile, llama-3.1-8b-instant |

Os modelos ficam em `PROVIDERS` no `api/index.py`; a tela mostra só os que a chave tem acesso. No Agno, a troca de provedor é só a classe do modelo: `OpenAIChat` ou `Groq`.

A chave:

- fica apenas no `sessionStorage` da aba;
- vai em cada requisição pelos headers `X-Provider` e `X-Api-Key`;
- **nunca** é gravada no servidor (cada execução cria os agentes com a chave recebida).

## Rodar localmente

```bash
uv venv && uv pip install -r requirements.txt uvicorn
cp .env.example .env                    # preencha as chaves que tiver
set -a && source .env && set +a
.venv/bin/python api/index.py          # http://localhost:8000  (PORT=8787 para outra porta)
```

## Deploy na Vercel

```bash
vercel        # ou importe o repositório no painel da Vercel
```

- `public/index.html` é servido como estático.
- `api/index.py` (FastAPI) roda como função Python. A Vercel detecta o FastAPI e encaminha para ele as rotas que não são estáticas, com o caminho original (não use `rewrites` para `/api/index`: o app passaria a receber esse caminho e responderia 404). O `vercel.json` só define `maxDuration: 120`.
- As chaves das APIs de terceiros ficam em **Settings → Environment Variables** do projeto na Vercel (as mesmas do `.env.example`). Depois de adicionar, faça um novo deploy. As que faltarem usam dados simulados.
