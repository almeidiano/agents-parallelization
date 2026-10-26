"""
Demonstração do padrão agêntico de Parallelization (livro "Agentic Design Patterns").

Caso de uso: um agente que planeja viagens.
  - Agente único: um agente generalista executa as 4 tarefas UMA APÓS A OUTRA.
  - Parallelization: 4 subagentes especialistas executam as tarefas AO MESMO TEMPO
    (agno.workflow.Parallel) e um agente planejador junta tudo no final.

A chave da OpenAI vem do navegador em cada requisição (header X-OpenAI-Key) e
nunca é armazenada no servidor.
"""

import asyncio
import hashlib
import json
import random
import time
from datetime import date, timedelta
from typing import AsyncIterator, Literal

from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.run.base import RunStatus
from agno.workflow import Parallel, Step, Workflow
from agno.workflow.types import StepInput, StepOutput
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse
from openai import AsyncOpenAI, AuthenticationError
from pydantic import BaseModel, Field

app = FastAPI(title="Parallelization demo")

ALLOWED_MODELS = {"gpt-4.1-mini", "gpt-4.1-nano", "gpt-4o-mini", "gpt-4.1", "gpt-4o"}

# (chave do step, rótulo) — a ordem é a mesma nos dois modos
TASKS = [
    ("voos", "Preços de voos"),
    ("hoteis", "Disponibilidade de hotéis"),
    ("eventos", "Eventos locais"),
    ("restaurantes", "Restaurantes recomendados"),
]


# ---------------------------------------------------------------------------
# Ferramentas (APIs externas simuladas)
#
# Cada ferramenta espera alguns segundos para imitar a latência de uma API real
# (companhias aéreas, booking, agenda cultural...). É justamente essa espera
# que a paralelização "esconde".
# ---------------------------------------------------------------------------


def _rng(*parts: str) -> random.Random:
    seed = hashlib.sha256("|".join(parts).lower().encode()).hexdigest()
    return random.Random(int(seed[:16], 16))


async def _latency(rng: random.Random) -> None:
    await asyncio.sleep(rng.uniform(2.0, 3.5))


async def search_flights(origin: str, destination: str, depart_date: str, return_date: str) -> str:
    """Busca preços de voos de ida e volta entre duas cidades.

    Args:
        origin: Cidade de origem.
        destination: Cidade de destino.
        depart_date: Data de ida (AAAA-MM-DD).
        return_date: Data de volta (AAAA-MM-DD).
    """
    rng = _rng("voos", origin, destination, depart_date)
    await _latency(rng)
    airlines = ["LATAM", "GOL", "Azul", "TAP", "Air France", "Iberia", "American Airlines", "KLM"]
    flights = []
    for airline in rng.sample(airlines, 4):
        stops = rng.choice([0, 0, 1, 1, 2])
        flights.append(
            {
                "companhia": airline,
                "ida": f"{depart_date} {rng.randint(5, 22):02d}:{rng.choice(['00', '15', '30', '45'])}",
                "volta": f"{return_date} {rng.randint(5, 22):02d}:{rng.choice(['00', '15', '30', '45'])}",
                "escalas": stops,
                "duracao_horas": round(rng.uniform(1.5, 14) + stops * 2.5, 1),
                "preco_brl": rng.randrange(900, 7500, 10) - stops * 300,
            }
        )
    return json.dumps({"origem": origin, "destino": destination, "voos": flights}, ensure_ascii=False)


async def search_hotels(destination: str, checkin: str, checkout: str) -> str:
    """Busca hotéis com disponibilidade no destino para o período.

    Args:
        destination: Cidade de destino.
        checkin: Data de check-in (AAAA-MM-DD).
        checkout: Data de check-out (AAAA-MM-DD).
    """
    rng = _rng("hoteis", destination, checkin)
    await _latency(rng)
    prefixes = ["Grand", "Boutique", "Hostel", "Residence", "Palace", "Urban", "Garden", "Central"]
    areas = ["Centro histórico", "Beira-mar", "Bairro boêmio", "Distrito financeiro", "Perto do aeroporto"]
    hotels = []
    for prefix in rng.sample(prefixes, 5):
        hotels.append(
            {
                "nome": f"{prefix} {destination}",
                "regiao": rng.choice(areas),
                "estrelas": rng.randint(2, 5),
                "nota_hospedes": round(rng.uniform(7.0, 9.8), 1),
                "diaria_brl": rng.randrange(180, 1800, 10),
                "quartos_disponiveis": rng.randint(0, 12),
            }
        )
    return json.dumps({"destino": destination, "hoteis": hotels}, ensure_ascii=False)


async def search_events(destination: str, start_date: str, end_date: str) -> str:
    """Procura eventos locais (shows, exposições, festivais) no período da viagem.

    Args:
        destination: Cidade de destino.
        start_date: Primeiro dia da viagem (AAAA-MM-DD).
        end_date: Último dia da viagem (AAAA-MM-DD).
    """
    rng = _rng("eventos", destination, start_date)
    await _latency(rng)
    try:
        start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    except ValueError:
        start, end = date.today(), date.today() + timedelta(days=5)
    span = max((end - start).days, 0)
    kinds = [
        ("Festival gastronômico de rua", "gastronomia"),
        ("Show de jazz ao ar livre", "música"),
        ("Exposição de arte contemporânea", "arte"),
        ("Feira de artesanato local", "cultura"),
        ("Tour noturno guiado pelo centro", "passeio"),
        ("Partida de futebol no estádio local", "esporte"),
        ("Peça de teatro em cartaz", "teatro"),
    ]
    events = []
    for name, category in rng.sample(kinds, 4):
        events.append(
            {
                "evento": name,
                "categoria": category,
                "data": (start + timedelta(days=rng.randint(0, span))).isoformat(),
                "preco_brl": rng.choice([0, 0, 40, 80, 150, 250]),
            }
        )
    return json.dumps({"destino": destination, "eventos": events}, ensure_ascii=False)


async def search_restaurants(destination: str, interests: str) -> str:
    """Busca restaurantes bem avaliados no destino.

    Args:
        destination: Cidade de destino.
        interests: Preferências do viajante (ex.: comida local, vegetariano).
    """
    rng = _rng("restaurantes", destination, interests)
    await _latency(rng)
    styles = [
        ("Cozinha típica regional", "$$"),
        ("Frutos do mar", "$$$"),
        ("Bistrô contemporâneo", "$$$"),
        ("Vegetariano e vegano", "$$"),
        ("Mercado de comida de rua", "$"),
        ("Alta gastronomia (menu degustação)", "$$$$"),
        ("Café e padaria artesanal", "$"),
    ]
    restaurants = []
    for i, (style, price) in enumerate(rng.sample(styles, 5)):
        restaurants.append(
            {
                "nome": f"Restaurante {destination} #{i + 1}",
                "estilo": style,
                "faixa_preco": price,
                "nota": round(rng.uniform(4.0, 4.9), 1),
            }
        )
    return json.dumps({"destino": destination, "restaurantes": restaurants}, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Agentes
# ---------------------------------------------------------------------------

COMMON = [
    "Responda em português do Brasil, em markdown, de forma objetiva (no máximo ~120 palavras).",
    "Use SEMPRE a ferramenta disponível antes de responder; não invente preços.",
]


class TripRequest(BaseModel):
    origin: str = Field(min_length=2, max_length=60)
    destination: str = Field(min_length=2, max_length=60)
    depart_date: str = Field(max_length=10)
    return_date: str = Field(max_length=10)
    interests: str = Field(default="", max_length=200)
    model: str = "gpt-4.1-mini"


def task_prompts(trip: TripRequest) -> dict[str, str]:
    t = f"de {trip.origin} para {trip.destination}, de {trip.depart_date} a {trip.return_date}"
    return {
        "voos": f"Pesquise os voos de ida e volta {t}. Destaque a opção mais barata e a mais rápida.",
        "hoteis": f"Pesquise hotéis disponíveis em {trip.destination} de {trip.depart_date} a {trip.return_date}. "
        "Recomende 3 opções com bom custo-benefício (ignore os sem quartos).",
        "eventos": f"Encontre eventos em {trip.destination} entre {trip.depart_date} e {trip.return_date}. "
        f"Interesses do viajante: {trip.interests or 'variados'}.",
        "restaurantes": f"Recomende restaurantes em {trip.destination}. "
        f"Interesses do viajante: {trip.interests or 'variados'}.",
    }


def model_for(trip: TripRequest, api_key: str) -> OpenAIChat:
    return OpenAIChat(id=trip.model, api_key=api_key)


def build_specialists(trip: TripRequest, api_key: str) -> dict[str, Agent]:
    """Um subagente especialista por tarefa, cada um com só a sua ferramenta."""
    specs = {
        "voos": ("Agente de Voos", search_flights, "Você é especialista em passagens aéreas."),
        "hoteis": ("Agente de Hotéis", search_hotels, "Você é especialista em hospedagem."),
        "eventos": ("Agente de Eventos", search_events, "Você é especialista em agenda cultural."),
        "restaurantes": ("Agente de Restaurantes", search_restaurants, "Você é um crítico gastronômico."),
    }
    return {
        key: Agent(
            name=name,
            model=model_for(trip, api_key),
            tools=[tool],
            instructions=[role, *COMMON],
            markdown=True,
            telemetry=False,
        )
        for key, (name, tool, role) in specs.items()
    }


def build_generalist(trip: TripRequest, api_key: str) -> Agent:
    """Um único agente que sabe fazer tudo — mas só consegue fazer uma coisa por vez."""
    return Agent(
        name="Agente de Viagens",
        model=model_for(trip, api_key),
        tools=[search_flights, search_hotels, search_events, search_restaurants],
        instructions=["Você é um agente de viagens que pesquisa voos, hotéis, eventos e restaurantes.", *COMMON],
        markdown=True,
        telemetry=False,
    )


def build_planner(trip: TripRequest, api_key: str) -> Agent:
    return Agent(
        name="Agente Planejador",
        model=model_for(trip, api_key),
        instructions=[
            "Você monta o plano de viagem final a partir das pesquisas recebidas.",
            "Responda em português do Brasil, em markdown, com: resumo (voo + hotel escolhidos e custo estimado), "
            "roteiro curto dia a dia e dicas. Máximo ~250 palavras.",
        ],
        markdown=True,
        telemetry=False,
    )


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------


async def ask(agent: Agent, prompt: str) -> StepOutput:
    """Roda o agente e devolve o resultado como StepOutput (falhas viram success=False)."""
    try:
        response = await agent.arun(prompt)
    except Exception as exc:  # noqa: BLE001
        return StepOutput(success=False, error=f"{agent.name}: {exc}", stop=True)
    if response.status == RunStatus.error:
        return StepOutput(success=False, error=f"{agent.name}: {response.content}", stop=True)
    return StepOutput(content=str(response.content))


def agent_step(key: str, agent: Agent, prompt: str) -> Step:
    async def run(step_input: StepInput) -> StepOutput:
        return await ask(agent, prompt)

    # max_retries=0: sem novas tentativas escondidas, para os tempos serem comparáveis
    return Step(name=key, executor=run, max_retries=0)


def synthesis_step(planner: Agent, trip: TripRequest, parallel_name: str | None = None) -> Step:
    async def run(step_input: StepInput) -> StepOutput:
        if parallel_name:
            results = step_input.get_step_content(parallel_name) or {}
        else:
            results = {key: step_input.get_step_content(key) for key, _ in TASKS}
        research = "\n\n".join(f"### {label}\n{results.get(key, '(sem resultado)')}" for key, label in TASKS)
        prompt = (
            f"Monte o plano de viagem de {trip.origin} para {trip.destination} "
            f"({trip.depart_date} a {trip.return_date}).\n\nPesquisas:\n\n{research}"
        )
        return await ask(planner, prompt)

    return Step(name="sintese", executor=run, max_retries=0)


def build_single_workflow(trip: TripRequest, api_key: str) -> Workflow:
    """Agente isolado: as tarefas rodam em sequência, uma esperando a outra."""
    agent = build_generalist(trip, api_key)
    prompts = task_prompts(trip)
    return Workflow(
        name="Agente único",
        steps=[
            *[agent_step(key, agent, prompts[key]) for key, _ in TASKS],
            synthesis_step(build_planner(trip, api_key), trip),
        ],
        telemetry=False,
    )


def build_parallel_workflow(trip: TripRequest, api_key: str) -> Workflow:
    """Parallelization: os subagentes rodam ao mesmo tempo e o planejador agrega."""
    specialists = build_specialists(trip, api_key)
    prompts = task_prompts(trip)
    return Workflow(
        name="Parallelization",
        steps=[
            Parallel(
                *[agent_step(key, specialists[key], prompts[key]) for key, _ in TASKS],
                name="pesquisas",
            ),
            synthesis_step(build_planner(trip, api_key), trip, parallel_name="pesquisas"),
        ],
        telemetry=False,
    )


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------


def require_key(key: str | None) -> str:
    if not key or not key.startswith("sk-"):
        raise HTTPException(status_code=401, detail="Chave da OpenAI ausente ou inválida.")
    return key


def sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


async def stream_workflow(workflow: Workflow, trip: TripRequest) -> AsyncIterator[str]:
    t0 = time.perf_counter()

    def now() -> float:
        return round(time.perf_counter() - t0, 3)

    yield sse({"type": "start", "t": 0})
    try:
        async for event in workflow.arun(
            input=f"Planejar viagem para {trip.destination}", stream=True, stream_events=True
        ):
            name = getattr(event, "step_name", None)
            if event.event == "StepStarted" and name:
                yield sse({"type": "step_started", "step": name, "t": now()})
            elif event.event in ("StepCompleted", "StepOutput") and name:
                # Falhas chegam como StepOutput(success=False), em StepCompleted ou StepOutput
                output = getattr(event, "step_response", None) or getattr(event, "step_output", None)
                if output is not None and not output.success:
                    yield sse({"type": "error", "message": output.error or "Falha no agente", "t": now()})
                    return
                if event.event == "StepOutput":
                    continue
                yield sse({"type": "step_completed", "step": name, "t": now(), "content": str(event.content or "")})
            elif event.event in ("WorkflowError", "StepError"):
                yield sse({"type": "error", "message": str(getattr(event, "error", "Erro no workflow")), "t": now()})
                return
    except Exception as exc:  # noqa: BLE001 — o erro vai para a interface
        yield sse({"type": "error", "message": f"{type(exc).__name__}: {exc}", "t": now()})
        return
    yield sse({"type": "done", "t": now()})


@app.post("/api/validate")
async def validate(x_openai_key: str | None = Header(default=None)):
    client = AsyncOpenAI(api_key=require_key(x_openai_key))
    try:
        await client.models.list()
    except AuthenticationError:
        raise HTTPException(status_code=401, detail="A OpenAI recusou essa chave.")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Não foi possível validar a chave: {exc}")
    return {"ok": True}


@app.post("/api/run/{mode}")
async def run(mode: Literal["single", "parallel"], trip: TripRequest, x_openai_key: str | None = Header(default=None)):
    api_key = require_key(x_openai_key)
    if trip.model not in ALLOWED_MODELS:
        raise HTTPException(status_code=400, detail="Modelo não permitido.")
    builder = build_single_workflow if mode == "single" else build_parallel_workflow
    return StreamingResponse(
        stream_workflow(builder(trip, api_key), trip),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


if __name__ == "__main__":
    # Desenvolvimento local: serve a API e o front-end juntos em http://localhost:8000
    import os
    from pathlib import Path

    import uvicorn
    from fastapi.staticfiles import StaticFiles

    app.mount("/", StaticFiles(directory=Path(__file__).parent.parent / "public", html=True), name="public")
    uvicorn.run(app, host="127.0.0.1", port=int(os.getenv("PORT", "8000")))
