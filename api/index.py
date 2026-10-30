"""
Demonstração do padrão agêntico de Parallelization (livro "Agentic Design Patterns").

Caso de uso: um agente que planeja viagens.
  - Agente único: um agente generalista executa as 4 tarefas UMA APÓS A OUTRA.
  - Parallelization: 4 subagentes especialistas executam as tarefas AO MESMO TEMPO
    (agno.workflow.Parallel) e um agente planejador junta tudo no final.

Provedores suportados: OpenAI e Groq. A chave vem do navegador em cada requisição
(headers X-Provider e X-Api-Key) e nunca é armazenada no servidor.
"""

import asyncio
import hashlib
import json
import os
import random
import time
from datetime import date, timedelta
from typing import Any, AsyncIterator, Awaitable, Callable, Literal

from agno.agent import Agent
from agno.models.base import Model
from agno.models.groq import Groq
from agno.models.openai import OpenAIChat
from agno.run.base import RunStatus
from agno.workflow import Parallel, Step, Workflow
from agno.workflow.types import StepInput, StepOutput
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse
import groq
import httpx
import openai
from pydantic import BaseModel, Field

app = FastAPI(title="Parallelization demo")

# Modelos oferecidos por provedor (o primeiro é o padrão). Todos suportam tool calling.
PROVIDERS = {
    "openai": {
        "label": "OpenAI",
        "key_prefix": "sk-",
        "models": ["gpt-4.1-mini", "gpt-4.1-nano", "gpt-4o-mini", "gpt-4.1", "gpt-4o"],
    },
    "groq": {
        "label": "Groq",
        "key_prefix": "gsk_",
        "models": ["openai/gpt-oss-120b", "openai/gpt-oss-20b", "llama-3.3-70b-versatile", "llama-3.1-8b-instant"],
    },
}

# (chave do step, rótulo) — a ordem é a mesma nos dois modos
TASKS = [
    ("voos", "Preços de voos"),
    ("hoteis", "Disponibilidade de hotéis"),
    ("eventos", "Eventos locais"),
    ("restaurantes", "Restaurantes recomendados"),
]


# ---------------------------------------------------------------------------
# Ferramentas: APIs de terceiros
#
# Cada tarefa usa uma API real quando a chave dela está configurada no servidor
# (variáveis de ambiente). Sem chave, ou se a API falhar, a ferramenta devolve
# dados simulados com um "aviso", para a demonstração nunca travar.
# ---------------------------------------------------------------------------

# tarefa -> (nome da fonte, variável de ambiente com a chave)
SOURCES = {
    "voos": ("SerpApi · Google Flights", "SERPAPI_API_KEY"),
    "hoteis": ("SerpApi · Google Hotels", "SERPAPI_API_KEY"),
    "eventos": ("Ticketmaster Discovery", "TICKETMASTER_API_KEY"),
    "restaurantes": ("Yelp Fusion", "YELP_API_KEY"),
}

HTTP_TIMEOUT = 20


class ExternalAPIError(Exception):
    pass


async def _get_json(url: str, *, params: dict | None = None, headers: dict | None = None) -> Any:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        try:
            response = await client.get(url, params=params, headers=headers)
        except httpx.HTTPError as exc:
            # Sem a URL na mensagem: ela pode conter a chave da API (query string)
            raise ExternalAPIError(type(exc).__name__) from None
    if response.status_code >= 400:
        raise ExternalAPIError(f"HTTP {response.status_code}")
    return response.json()


async def _with_fallback(
    task: str, real: Callable[[str], Awaitable[dict]], mock: Callable[[], Awaitable[dict]]
) -> str:
    source, env_var = SOURCES[task]
    api_key = os.getenv(env_var)
    if api_key:
        try:
            return json.dumps({"fonte": source, **await real(api_key)}, ensure_ascii=False)
        except ExternalAPIError as exc:
            warning = f"{source} falhou ({exc}); dados simulados."
        except (AttributeError, KeyError, TypeError, ValueError, IndexError):
            warning = f"{source} devolveu uma resposta em formato inesperado; dados simulados."
    else:
        warning = f"{env_var} não configurada; dados simulados."
    return json.dumps({"fonte": "simulada", "aviso": warning, **await mock()}, ensure_ascii=False)


def _geohash(lat: float, lon: float, precision: int = 7) -> str:
    """Codifica coordenadas em geohash (formato do parâmetro geoPoint da Ticketmaster)."""
    alphabet = "0123456789bcdefghjkmnpqrstuvwxyz"
    lat_range, lon_range = [-90.0, 90.0], [-180.0, 180.0]
    out, bits, n_bits, use_lon = "", 0, 0, True
    while len(out) < precision:
        rng, value = (lon_range, lon) if use_lon else (lat_range, lat)
        mid = (rng[0] + rng[1]) / 2
        bits = bits * 2 + (value > mid)
        rng[0 if value > mid else 1] = mid
        use_lon, n_bits = not use_lon, n_bits + 1
        if n_bits == 5:
            out, bits, n_bits = out + alphabet[bits], 0, 0
    return out


async def search_flights(origin_airport: str, destination_airport: str, depart_date: str, return_date: str) -> str:
    """Busca preços de voos de ida e volta entre dois aeroportos.

    Args:
        origin_airport: Código IATA do aeroporto de origem (ex.: GRU).
        destination_airport: Código IATA do aeroporto de destino (ex.: LIS).
        depart_date: Data de ida (AAAA-MM-DD).
        return_date: Data de volta (AAAA-MM-DD).
    """

    async def real(api_key: str) -> dict:
        data = await _get_json(
            "https://serpapi.com/search.json",
            params={
                "engine": "google_flights",
                "departure_id": origin_airport.upper(),
                "arrival_id": destination_airport.upper(),
                "outbound_date": depart_date,
                "return_date": return_date,
                "type": 1,
                "currency": "BRL",
                "hl": "pt",
                "gl": "br",
                "api_key": api_key,
            },
        )
        if data.get("error"):
            raise ExternalAPIError(data["error"])
        options = (data.get("best_flights") or []) + (data.get("other_flights") or [])
        flights = []
        for option in options[:6]:
            legs = option.get("flights") or [{}]
            flights.append(
                {
                    "companhias": sorted({leg.get("airline", "?") for leg in legs}),
                    "saida": legs[0].get("departure_airport", {}).get("time"),
                    "chegada": legs[-1].get("arrival_airport", {}).get("time"),
                    "escalas": len(option.get("layovers") or []),
                    "duracao_horas": round((option.get("total_duration") or 0) / 60, 1),
                    "preco_ida_e_volta_brl": option.get("price"),
                }
            )
        return {
            "observacao": "Horários do voo de ida; o preço é da viagem de ida e volta, 1 adulto.",
            "voos": flights,
        }

    async def mock() -> dict:
        rng = _rng("voos", origin_airport, destination_airport, depart_date)
        await _latency(rng)
        airlines = ["LATAM", "GOL", "Azul", "TAP", "Air France", "Iberia", "American Airlines", "KLM"]
        flights = []
        for airline in rng.sample(airlines, 4):
            stops = rng.choice([0, 0, 1, 1, 2])
            flights.append(
                {
                    "companhias": [airline],
                    "saida": f"{depart_date} {rng.randint(5, 22):02d}:{rng.choice(['00', '15', '30', '45'])}",
                    "escalas": stops,
                    "duracao_horas": round(rng.uniform(1.5, 14) + stops * 2.5, 1),
                    "preco_ida_e_volta_brl": rng.randrange(900, 7500, 10) - stops * 300,
                }
            )
        return {"voos": flights}

    return await _with_fallback("voos", real, mock)


async def search_hotels(destination: str, checkin: str, checkout: str) -> str:
    """Busca hotéis com disponibilidade no destino para o período.

    Args:
        destination: Cidade de destino.
        checkin: Data de check-in (AAAA-MM-DD).
        checkout: Data de check-out (AAAA-MM-DD).
    """

    async def real(api_key: str) -> dict:
        data = await _get_json(
            "https://serpapi.com/search.json",
            params={
                "engine": "google_hotels",
                "q": f"hotéis em {destination}",
                "check_in_date": checkin,
                "check_out_date": checkout,
                "adults": 1,
                "currency": "BRL",
                "hl": "pt",
                "gl": "br",
                "api_key": api_key,
            },
        )
        if data.get("error"):
            raise ExternalAPIError(data["error"])
        hotels = []
        for prop in (data.get("properties") or [])[:8]:
            total = prop.get("total_rate") or {}
            nightly = prop.get("rate_per_night") or {}
            hotels.append(
                {
                    "nome": prop.get("name"),
                    "tipo": prop.get("type"),
                    "estrelas": prop.get("extracted_hotel_class") or prop.get("hotel_class"),
                    "nota_hospedes_0a5": prop.get("overall_rating"),
                    "num_avaliacoes": prop.get("reviews"),
                    "diaria_brl": nightly.get("extracted_lowest"),
                    "preco_total_estadia_brl": total.get("extracted_lowest"),
                    "comodidades": (prop.get("amenities") or [])[:5],
                }
            )
        return {
            "destino": destination,
            "observacao": "Preços para 1 adulto, com disponibilidade nas datas informadas.",
            "hoteis": hotels,
        }

    async def mock() -> dict:
        rng = _rng("hoteis", destination, checkin)
        await _latency(rng)
        prefixes = ["Grand", "Boutique", "Hostel", "Residence", "Palace", "Urban", "Garden", "Central"]
        nights = _nights(checkin, checkout)
        hotels = []
        for prefix in rng.sample(prefixes, 5):
            hotels.append(
                {
                    "nome": f"{prefix} {destination}",
                    "estrelas": rng.randint(2, 5),
                    "nota_hospedes": round(rng.uniform(7.0, 9.8), 1),
                    "preco_total_estadia": rng.randrange(180, 1800, 10) * nights,
                    "moeda": "BRL",
                }
            )
        return {"destino": destination, "hoteis": hotels}

    return await _with_fallback("hoteis", real, mock)


async def search_events(city: str, latitude: float, longitude: float, start_date: str, end_date: str) -> str:
    """Procura eventos locais (shows, esportes, teatro, festivais) num raio de 30 km da cidade.

    Args:
        city: Nome da cidade.
        latitude: Latitude do centro da cidade.
        longitude: Longitude do centro da cidade.
        start_date: Primeiro dia da viagem (AAAA-MM-DD).
        end_date: Último dia da viagem (AAAA-MM-DD).
    """

    async def real(api_key: str) -> dict:
        data = await _get_json(
            "https://app.ticketmaster.com/discovery/v2/events.json",
            params={
                "apikey": api_key,
                "geoPoint": _geohash(latitude, longitude),
                "radius": 30,
                "unit": "km",
                "startDateTime": f"{start_date}T00:00:00Z",
                "endDateTime": f"{end_date}T23:59:59Z",
                "sort": "date,asc",
                "size": 10,
            },
        )
        events = []
        for event in (data.get("_embedded") or {}).get("events") or []:
            start = (event.get("dates") or {}).get("start") or {}
            category = (event.get("classifications") or [{}])[0]
            venue = ((event.get("_embedded") or {}).get("venues") or [{}])[0]
            price = (event.get("priceRanges") or [{}])[0]
            events.append(
                {
                    "evento": event.get("name"),
                    "data": start.get("localDate"),
                    "hora": start.get("localTime"),
                    "categoria": (category.get("segment") or {}).get("name"),
                    "genero": (category.get("genre") or {}).get("name"),
                    "local": venue.get("name"),
                    "preco": f"{price['min']}–{price.get('max', price['min'])} {price.get('currency', '')}".strip()
                    if price.get("min") is not None
                    else None,
                }
            )
        result: dict = {"cidade": city, "eventos": events}
        if not events:
            result["observacao"] = "Nenhum evento no Ticketmaster para o período (a cobertura varia por país)."
        return result

    async def mock() -> dict:
        rng = _rng("eventos", city, start_date)
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
                    "preco": f"{rng.choice([0, 0, 40, 80, 150, 250])} BRL",
                }
            )
        return {"cidade": city, "eventos": events}

    return await _with_fallback("eventos", real, mock)


async def search_restaurants(location: str, term: str = "restaurants") -> str:
    """Busca restaurantes bem avaliados no destino.

    Args:
        location: Cidade e país (ex.: "Lisboa, Portugal").
        term: Palavras-chave em inglês sobre o tipo de comida (ex.: "seafood", "vegetarian", "local food").
    """

    async def real(api_key: str) -> dict:
        data = await _get_json(
            "https://api.yelp.com/v3/businesses/search",
            params={"location": location, "term": term or "restaurants", "categories": "restaurants", "limit": 8},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        restaurants = []
        for business in data.get("businesses") or []:
            address = (business.get("location") or {}).get("display_address") or []
            restaurants.append(
                {
                    "nome": business.get("name"),
                    "estilo": ", ".join(c.get("title", "") for c in business.get("categories") or []),
                    "faixa_preco": business.get("price"),
                    "nota": business.get("rating"),
                    "num_avaliacoes": business.get("review_count"),
                    "endereco": ", ".join(address) or business.get("formatted_address"),
                }
            )
        return {"local": location, "restaurantes": restaurants}

    async def mock() -> dict:
        rng = _rng("restaurantes", location, term)
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
        city = location.split(",")[0].strip()
        restaurants = []
        for i, (style, price) in enumerate(rng.sample(styles, 5)):
            restaurants.append(
                {
                    "nome": f"Restaurante {city} #{i + 1}",
                    "estilo": style,
                    "faixa_preco": price,
                    "nota": round(rng.uniform(4.0, 4.9), 1),
                }
            )
        return {"local": location, "restaurantes": restaurants}

    return await _with_fallback("restaurantes", real, mock)


# Dados simulados (fallback): esperam alguns segundos para imitar a latência de
# uma API real, e são sempre os mesmos para a mesma entrada.


def _rng(*parts: str) -> random.Random:
    seed = hashlib.sha256("|".join(parts).lower().encode()).hexdigest()
    return random.Random(int(seed[:16], 16))


async def _latency(rng: random.Random) -> None:
    await asyncio.sleep(rng.uniform(2.0, 3.5))


def _nights(checkin: str, checkout: str) -> int:
    try:
        return max((date.fromisoformat(checkout) - date.fromisoformat(checkin)).days, 1)
    except ValueError:
        return 1


# ---------------------------------------------------------------------------
# Agentes
# ---------------------------------------------------------------------------

COMMON = [
    "Responda em português do Brasil, em markdown, de forma objetiva (no máximo ~120 palavras).",
    "Use SEMPRE a ferramenta disponível antes de responder; não invente preços, nomes ou datas.",
    "Termine com uma linha 'Fonte: <campo fonte>'. Se a ferramenta devolver 'aviso', diga que os dados são simulados.",
]


class TripRequest(BaseModel):
    origin: str = Field(min_length=2, max_length=60)
    destination: str = Field(min_length=2, max_length=60)
    depart_date: str = Field(max_length=10)
    return_date: str = Field(max_length=10)
    interests: str = Field(default="", max_length=200)
    model: str


def task_prompts(trip: TripRequest) -> dict[str, str]:
    t = f"de {trip.origin} para {trip.destination}, de {trip.depart_date} a {trip.return_date}"
    return {
        "voos": f"Pesquise os voos de ida e volta {t} (use os códigos IATA dos aeroportos principais). "
        "Destaque a opção mais barata e a mais rápida.",
        "hoteis": f"Pesquise hotéis disponíveis em {trip.destination} de {trip.depart_date} a {trip.return_date}. "
        "Recomende 3 opções com bom custo-benefício.",
        "eventos": f"Encontre eventos em {trip.destination} entre {trip.depart_date} e {trip.return_date} "
        "(informe as coordenadas do centro da cidade). "
        f"Interesses do viajante: {trip.interests or 'variados'}.",
        "restaurantes": f"Recomende restaurantes em {trip.destination} (informe cidade e país). "
        f"Interesses do viajante: {trip.interests or 'variados'}.",
    }


class Credentials(BaseModel):
    provider: Literal["openai", "groq"]
    api_key: str


def model_for(trip: TripRequest, creds: Credentials) -> Model:
    if creds.provider == "groq":
        return Groq(id=trip.model, api_key=creds.api_key)
    return OpenAIChat(id=trip.model, api_key=creds.api_key)


def build_specialists(trip: TripRequest, creds: Credentials) -> dict[str, Agent]:
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
            model=model_for(trip, creds),
            tools=[tool],
            instructions=[role, *COMMON],
            markdown=True,
            telemetry=False,
        )
        for key, (name, tool, role) in specs.items()
    }


def build_generalist(trip: TripRequest, creds: Credentials) -> Agent:
    """Um único agente que sabe fazer tudo — mas só consegue fazer uma coisa por vez."""
    return Agent(
        name="Agente de Viagens",
        model=model_for(trip, creds),
        tools=[search_flights, search_hotels, search_events, search_restaurants],
        instructions=["Você é um agente de viagens que pesquisa voos, hotéis, eventos e restaurantes.", *COMMON],
        markdown=True,
        telemetry=False,
    )


def build_planner(trip: TripRequest, creds: Credentials) -> Agent:
    return Agent(
        name="Agente Planejador",
        model=model_for(trip, creds),
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


def build_single_workflow(trip: TripRequest, creds: Credentials) -> Workflow:
    """Agente isolado: as tarefas rodam em sequência, uma esperando a outra."""
    agent = build_generalist(trip, creds)
    prompts = task_prompts(trip)
    return Workflow(
        name="Agente único",
        steps=[
            *[agent_step(key, agent, prompts[key]) for key, _ in TASKS],
            synthesis_step(build_planner(trip, creds), trip),
        ],
        telemetry=False,
    )


def build_parallel_workflow(trip: TripRequest, creds: Credentials) -> Workflow:
    """Parallelization: os subagentes rodam ao mesmo tempo e o planejador agrega."""
    specialists = build_specialists(trip, creds)
    prompts = task_prompts(trip)
    return Workflow(
        name="Parallelization",
        steps=[
            Parallel(
                *[agent_step(key, specialists[key], prompts[key]) for key, _ in TASKS],
                name="pesquisas",
            ),
            synthesis_step(build_planner(trip, creds), trip, parallel_name="pesquisas"),
        ],
        telemetry=False,
    )


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------


def require_creds(provider: str | None, key: str | None) -> Credentials:
    info = PROVIDERS.get(provider or "")
    if not info:
        raise HTTPException(status_code=400, detail="Provedor inválido.")
    if not key or not key.startswith(info["key_prefix"]):
        raise HTTPException(
            status_code=401, detail=f"Chave da {info['label']} ausente ou inválida (deve começar com {info['key_prefix']})."
        )
    return Credentials(provider=provider, api_key=key)


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
async def validate(x_provider: str | None = Header(default=None), x_api_key: str | None = Header(default=None)):
    """Confere a chave listando os modelos da conta; devolve os modelos da demo que ela pode usar."""
    creds = require_creds(x_provider, x_api_key)
    info = PROVIDERS[creds.provider]
    client = groq.AsyncGroq if creds.provider == "groq" else openai.AsyncOpenAI
    try:
        available = {m.id for m in (await client(api_key=creds.api_key).models.list()).data}
    except (openai.AuthenticationError, groq.AuthenticationError):
        raise HTTPException(status_code=401, detail=f"A {info['label']} recusou essa chave.")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Não foi possível validar a chave: {exc}")
    models = [m for m in info["models"] if m in available] or info["models"]
    return {"ok": True, "provider": creds.provider, "label": info["label"], "models": models}


@app.get("/api/sources")
async def sources():
    """Quais APIs de terceiros estão configuradas (nunca devolve as chaves)."""
    return {task: {"fonte": name, "real": bool(os.getenv(env_var))} for task, (name, env_var) in SOURCES.items()}


@app.post("/api/run/{mode}")
async def run(
    mode: Literal["single", "parallel"],
    trip: TripRequest,
    x_provider: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None),
):
    creds = require_creds(x_provider, x_api_key)
    if trip.model not in PROVIDERS[creds.provider]["models"]:
        raise HTTPException(status_code=400, detail="Modelo não permitido.")
    builder = build_single_workflow if mode == "single" else build_parallel_workflow
    return StreamingResponse(
        stream_workflow(builder(trip, creds), trip),
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
