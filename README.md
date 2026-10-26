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

As ferramentas simulam APIs externas (voos, hotéis, eventos, restaurantes): elas esperam de 2 a 3,5 s e devolvem dados fictícios. É essa espera que a paralelização elimina. Para usar APIs reais, troque as funções `search_*` em `api/index.py`.

## Código principal (`api/index.py`)

```python
# Agente único: um step depois do outro
Workflow(steps=[voos, hoteis, eventos, restaurantes, sintese])

# Parallelization: os 4 subagentes ao mesmo tempo, depois a agregação
Workflow(steps=[Parallel(voos, hoteis, eventos, restaurantes, name="pesquisas"), sintese])
```

O backend transmite os eventos do Agno (`StepStarted` / `StepCompleted`) por SSE, e o front-end anima o diagrama e a linha do tempo em tempo real.

## Chave da OpenAI

A interface só é liberada depois que a pessoa informa uma chave da OpenAI, que é validada em `/api/validate`. A chave:

- fica apenas no `sessionStorage` da aba;
- vai em cada requisição pelo header `X-OpenAI-Key`;
- **nunca** é gravada no servidor (cada execução cria os agentes com a chave recebida).

## Rodar localmente

```bash
uv venv && uv pip install -r requirements.txt uvicorn
.venv/bin/python api/index.py          # http://localhost:8000  (PORT=8787 para outra porta)
```

## Deploy na Vercel

```bash
vercel        # ou importe o repositório no painel da Vercel
```

- `public/index.html` é servido como estático.
- `api/index.py` (FastAPI) roda como função Python; o `vercel.json` redireciona `/api/*` para ela e define `maxDuration: 120`.
- Não é preciso configurar nenhuma variável de ambiente.
