"""
basket.py — Backtesting EN VIVO de Divergencia Armónica ETH/SOL/BTC
Lee precios REALES del order book de Polymarket — SIN ejecutar órdenes reales.

LOGICA BINARIA CORRECTA:
  Los tokens de Polymarket resuelven en 0 o 1 (todo o nada).
  Cada entrada cuesta exactamente $1.00 (el 1% del capital de $100).
  Shares comprados = $1.00 / precio_ask

v6: Entrada en los 3 activos simultáneamente cuando se detecta el armónico.
  - bt["position"] → bt["positions"] (lista de hasta 3 posiciones)
  - Capital descontado = ENTRY_USD * 3 por ciclo
  - Side de cada activo: el que tenga mayor divergencia (señal global) aplica
    a todos. Cada activo entra por el side del armónico detectado.
"""

import asyncio
import os
import sys
import time
import json
import csv
import logging
import threading
from collections import deque
from datetime import datetime

from strategy_core import (
    find_active_market,
    get_order_book_metrics,
    seconds_remaining,
    fetch_market_resolution,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("basket")

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ═══════════════════════════════════════════════════════
#  PARÁMETROS
# ═══════════════════════════════════════════════════════
POLL_INTERVAL        = 0.5
DIVERGENCE_THRESHOLD = 0.05
DIVERGENCE_MAX       = 0.14
WAKE_UP_SECS         = 90
ENTRY_WINDOW_SECS    = 85
ENTRY_OPEN_SECS      = 60
ENTRY_CLOSE_SECS     = 30
RESOLUTION_MAX_SECS  = 7   # solo chequear resolucion cuando queden <= 7s

CAPITAL_TOTAL        = 100.0
ENTRY_PCT            = 0.01
ENTRY_USD            = CAPITAL_TOTAL * ENTRY_PCT   # $1 por activo

RESOLVED_UP_THRESH   = 0.98
RESOLVED_DN_THRESH   = 0.02

CONSENSUS_FULL       = 0.80
CONSENSUS_SOFT       = 0.80

ENTRY_MIN_PRICE      = 0.65
MID_HISTORY_SIZE     = 10   # 5 segundos a 0.5s de poll → resolución confirmada

LOG_FILE   = os.environ.get("LOG_FILE",   "/data/basket_log.json")
CSV_FILE   = os.environ.get("CSV_FILE",   "/data/basket_trades.csv")
STATE_FILE = os.environ.get("STATE_FILE", "/data/state.json")

# ═══════════════════════════════════════════════════════
#  ESTADO
# ═══════════════════════════════════════════════════════
SYMBOLS = ["ETH", "SOL", "BTC"]

markets = {
    s: {
        "info":      None,
        "up_bid":    0.0, "up_ask": 0.0, "up_mid": 0.0,
        "dn_bid":    0.0, "dn_ask": 0.0, "dn_mid": 0.0,
        "time_left": "N/A",
        "error":     None,
    }
    for s in SYMBOLS
}

mid_history: dict[str, deque] = {
    s: deque(maxlen=MID_HISTORY_SIZE) for s in SYMBOLS
}

bt = {
    "harm_up":      0.0,
    "harm_dn":      0.0,
    "signal_asset": None,
    "signal_side":  None,
    "signal_div":   0.0,
    "entry_window": False,
    "positions":    [],          # ← LISTA de posiciones (máx 3)
    "pending_positions": [],     # ← posiciones esperando resolución de Gamma
    "traded_this_cycle": False,
    "capital":      CAPITAL_TOTAL,
    "total_pnl":    0.0,
    "peak_capital": CAPITAL_TOTAL,
    "max_drawdown": 0.0,
    "wins":         0,
    "losses":       0,
    "consensus":    "NONE",
    "skipped":      0,
    "trades":       [],
    "cycle":        0,
    "phase":        "DURMIENDO",
    "next_wake":    "N/A",
}

recent_events = deque(maxlen=50)

CSV_COLUMNS = [
    "trade_id", "entry_ts", "exit_ts", "duration_s",
    "asset", "side", "consensus",
    "entry_ask", "entry_bid", "entry_mid", "entry_usd", "shares",
    "secs_left_entry", "harm_entry", "gap_pts",
    "peer1_sym", "peer1_side_mid", "peer1_opp_mid",
    "peer2_sym", "peer2_side_mid", "peer2_opp_mid",
    "sl_price", "exit_type", "exit_price", "resolved", "binary_win",
    "pnl_usd", "pnl_pct_entry", "max_possible_win", "outcome",
    "capital_before", "capital_after", "cumulative_pnl", "trade_number",
]


# ═══════════════════════════════════════════════════════
#  UTILIDADES
# ═══════════════════════════════════════════════════════

def log_event(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    recent_events.append(entry)
    log.info(msg)


def harmonic_mean(values: list) -> float:
    if not values or any(v <= 0 for v in values):
        return 0.0
    return len(values) / sum(1.0 / v for v in values)


def find_cheapest(mids: dict, h_avg: float):
    if h_avg == 0:
        return None, 0.0
    cheapest_name, cheapest_diff = None, 0.0
    for name, mid in mids.items():
        diff = mid - h_avg
        if diff < cheapest_diff:
            cheapest_diff, cheapest_name = diff, name
    return cheapest_name, cheapest_diff


def min_secs_remaining() -> float | None:
    result = None
    for sym in SYMBOLS:
        info = markets[sym]["info"]
        if info:
            secs = seconds_remaining(info)
            if secs is not None:
                result = secs if result is None else min(result, secs)
    return result


def update_drawdown():
    cap = bt["capital"]
    if cap > bt["peak_capital"]:
        bt["peak_capital"] = cap
    dd = bt["peak_capital"] - cap
    if dd > bt["max_drawdown"]:
        bt["max_drawdown"] = dd


# ═══════════════════════════════════════════════════════
#  RESOLUCIÓN VÍA GAMMA (resultado real)
# ═══════════════════════════════════════════════════════

GAMMA_POLL_INTERVAL  = 3.0   # segundos entre polls (CLOB es la fuente primaria)
GAMMA_TIMEOUT_SECS   = 600   # 10 min máximo antes de abandonar con LOSS

def _move_to_pending(pos: dict):
    """Mueve una posición a pending — el resultado se sabrá cuando Gamma resuelva."""
    sym      = pos["asset"]
    snapshot = pos.get("market_info_snapshot") or {}

    # Usar gamma_condition_id si está disponible, sino el condition_id del CLOB
    condition_id = snapshot.get("gamma_condition_id") or snapshot.get("condition_id")
    market_slug  = snapshot.get("market_slug", "")

    if not condition_id and not market_slug:
        log_event(f"PENDING {sym}: sin condition_id ni slug — no se puede consultar Gamma. LOSS conservador.")
        pnl = -ENTRY_USD
        bt["capital"]   += ENTRY_USD + pnl
        bt["total_pnl"] += pnl
        bt["losses"]    += 1
        update_drawdown()
        _record_trade(pos, "UNKNOWN", "LOSS", pnl, source="UNKNOWN")
        return

    pos["pending_since"]  = time.time()
    pos["condition_id"]   = condition_id
    pos["market_slug"]    = market_slug
    pos["gamma_polls"]    = 0
    bt["pending_positions"].append(pos)
    log_event(
        f"PENDING {sym} ({pos['side']}) — esperando Gamma "
        f"slug={market_slug or 'N/A'} cid={str(condition_id or '')[:8]}..."
    )


def _get_last_trade_price(token_id: str) -> float | None:
    """
    Consulta el último precio de trade del CLOB para un token.
    Cuando un mercado resuelve, el último trade del token ganador es 1.0 (o muy cercano).
    Endpoint: GET /last-trade-price?token_id=...
    """
    try:
        r = requests.get(
            f"{CLOB_HOST}/last-trade-price",
            params={"token_id": token_id},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            price = data.get("price")
            if price is not None:
                return float(price)
    except Exception:
        pass
    return None


async def _check_clob_resolution(pos: dict) -> str | None:
    """
    Detecta resolución directamente vía CLOB sin depender de Gamma.
    Gamma tiene lag de varios minutos — el CLOB refleja el estado on-chain inmediatamente.

    Estrategia en 2 pasos:
      1. last-trade-price: cuando un token gana, su último trade es >= 0.98
      2. order book: bid >= 0.98 sin asks = token resuelto en 1.0
    """
    snap     = pos.get("market_info_snapshot") or {}
    side     = pos["side"]
    up_tid   = snap.get("up_token_id")
    down_tid = snap.get("down_token_id")

    if not up_tid or not down_tid:
        return None

    loop = asyncio.get_event_loop()

    try:
        # ── Paso 1: last-trade-price (señal más directa de resolución) ────────
        up_last, dn_last = await asyncio.gather(
            loop.run_in_executor(None, _get_last_trade_price, up_tid),
            loop.run_in_executor(None, _get_last_trade_price, down_tid),
        )

        if up_last is not None and up_last >= 0.95:
            return "UP"
        if dn_last is not None and dn_last >= 0.95:
            return "DOWN"

        # ── Paso 2: order book (fallback si last-trade no está disponible) ────
        up_metrics, _ = await loop.run_in_executor(None, get_order_book_metrics, up_tid)
        dn_metrics, _ = await loop.run_in_executor(None, get_order_book_metrics, down_tid)

        if up_metrics:
            bid = up_metrics["best_bid"]
            ask = up_metrics["best_ask"]
            n_asks = up_metrics["num_asks"]
            # Resuelto UP: bid alto sin asks, o book completamente vacío con last-trade alto
            if bid >= 0.95 and (n_asks == 0 or ask >= 0.99):
                return "UP"

        if dn_metrics:
            bid = dn_metrics["best_bid"]
            ask = dn_metrics["best_ask"]
            n_asks = dn_metrics["num_asks"]
            if bid >= 0.95 and (n_asks == 0 or ask >= 0.99):
                return "DOWN"

    except Exception:
        pass

    return None


async def pending_resolution_loop():
    """
    Loop independiente que corre en paralelo al main_loop.

    Fuente primaria: CLOB directo (last-trade-price + order book).
      → Refleja resolución on-chain inmediatamente, igual que la UI de Polymarket.
      → Gamma tiene lag de minutos y bugs conocidos con closed/outcomePrices.

    Fuente secundaria: Gamma API (solo como fallback adicional).
    """
    while True:
        await asyncio.sleep(GAMMA_POLL_INTERVAL)
        if not bt["pending_positions"]:
            continue

        resueltas = []
        for pos in list(bt["pending_positions"]):
            sym          = pos["asset"]
            condition_id = pos.get("condition_id", "")
            elapsed      = time.time() - pos["pending_since"]
            pos["gamma_polls"] += 1

            # ── Fuente 1: CLOB (last-trade-price + order book) ───────────────
            resolved = await _check_clob_resolution(pos)
            if resolved in ("UP", "DOWN"):
                log_event(f"RESOLUCION CLOB {sym}: → {resolved} ({elapsed:.0f}s)")
                _apply_resolution(pos, resolved, source="CLOB_PENDING")
                resueltas.append(pos)
                continue

            # ── Fuente 2: Gamma (fallback — lag conocido de varios minutos) ──
            resolved = fetch_market_resolution(
                condition_id,
                market_slug=pos.get("market_slug", ""),
            )
            if resolved in ("UP", "DOWN"):
                log_event(f"RESOLUCION GAMMA {sym}: → {resolved} ({elapsed:.0f}s, {pos['gamma_polls']} polls)")
                _apply_resolution(pos, resolved, source="GAMMA")
                resueltas.append(pos)

            elif elapsed > GAMMA_TIMEOUT_SECS:
                log_event(f"TIMEOUT {sym}: {GAMMA_TIMEOUT_SECS}s sin respuesta CLOB ni Gamma — LOSS conservador")
                pnl = -ENTRY_USD
                bt["capital"]   += ENTRY_USD + pnl
                bt["total_pnl"] += pnl
                bt["losses"]    += 1
                update_drawdown()
                _record_trade(pos, "TIMEOUT", "LOSS", pnl, source="TIMEOUT")
                resueltas.append(pos)

            else:
                log_event(f"PENDING {sym}: esperando CLOB/Gamma ({elapsed:.0f}s / {GAMMA_TIMEOUT_SECS}s máx)")

        for pos in resueltas:
            bt["pending_positions"].remove(pos)

        if resueltas:
            write_state()


# ═══════════════════════════════════════════════════════
#  ESCRITURA DE ESTADO PARA DASHBOARD
# ═══════════════════════════════════════════════════════

def write_state():
    total_trades = bt["wins"] + bt["losses"]
    win_rate = (bt["wins"] / total_trades * 100) if total_trades > 0 else 0.0
    roi = (bt["capital"] - CAPITAL_TOTAL) / CAPITAL_TOTAL * 100

    state = {
        "ts": datetime.now().isoformat(),
        "phase": bt["phase"],
        "cycle": bt["cycle"],
        "capital": round(bt["capital"], 4),
        "total_pnl": round(bt["total_pnl"], 4),
        "roi": round(roi, 2),
        "peak_capital": round(bt["peak_capital"], 4),
        "max_drawdown": round(bt["max_drawdown"], 4),
        "wins": bt["wins"],
        "losses": bt["losses"],
        "win_rate": round(win_rate, 1),
        "skipped": bt["skipped"],
        "consensus": bt["consensus"],
        "entry_window": bt["entry_window"],
        "next_wake": bt["next_wake"],
        "harm_up": round(bt["harm_up"], 4),
        "harm_dn": round(bt["harm_dn"], 4),
        "signal_asset": bt["signal_asset"],
        "signal_side": bt["signal_side"],
        "signal_div": round(bt["signal_div"], 4),
        "positions": bt["positions"],          # ← lista completa
        "pending_resolution": None,
        "markets": {
            sym: {
                "up_mid": round(markets[sym]["up_mid"], 4),
                "dn_mid": round(markets[sym]["dn_mid"], 4),
                "up_ask": round(markets[sym]["up_ask"], 4),
                "dn_ask": round(markets[sym]["dn_ask"], 4),
                "time_left": markets[sym]["time_left"],
                "error": markets[sym]["error"],
            }
            for sym in SYMBOLS
        },
        "events": list(recent_events)[-30:],
        "recent_trades": bt["trades"][-10:],
    }
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"write_state error: {e}")


# ═══════════════════════════════════════════════════════
#  RESTAURAR ESTADO DESDE CSV
# ═══════════════════════════════════════════════════════

def restore_state_from_csv():
    if not os.path.isfile(CSV_FILE):
        log.info("No hay CSV previo — iniciando desde cero.")
        return
    try:
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            return
        last = rows[-1]
        bt["capital"]      = float(last["capital_after"])
        bt["total_pnl"]    = float(last["cumulative_pnl"])
        bt["wins"]         = sum(1 for r in rows if r["outcome"] == "WIN")
        bt["losses"]       = sum(1 for r in rows if r["outcome"] == "LOSS")
        bt["trades"]       = [dict(r) for r in rows]
        peak = CAPITAL_TOTAL
        for r in rows:
            cap = float(r["capital_after"])
            if cap > peak:
                peak = cap
            dd = peak - cap
            if dd > bt["max_drawdown"]:
                bt["max_drawdown"] = dd
        bt["peak_capital"] = peak
        total = bt["wins"] + bt["losses"]
        log.info(f"Estado restaurado — {total} trades | Capital: ${bt['capital']:.4f} | "
                 f"PnL: ${bt['total_pnl']:+.4f} | W:{bt['wins']} L:{bt['losses']}")
    except Exception as e:
        log.warning(f"No se pudo restaurar estado desde CSV: {e}")


# ═══════════════════════════════════════════════════════
#  DISCOVERY Y FETCH
# ═══════════════════════════════════════════════════════

async def discover_all():
    loop = asyncio.get_event_loop()
    for sym in SYMBOLS:
        try:
            info = await loop.run_in_executor(None, find_active_market, sym)
            if info:
                markets[sym]["info"]  = info
                markets[sym]["error"] = None
                mid_history[sym].clear()
                log_event(f"{sym}: mercado encontrado — {info.get('question','')[:50]}")
            else:
                markets[sym]["info"]  = None
                markets[sym]["error"] = "sin mercado activo"
                log_event(f"{sym}: no se encontró mercado activo")
        except Exception as e:
            markets[sym]["info"]  = None
            markets[sym]["error"] = str(e)
            log_event(f"{sym}: error en discovery — {e}")
    bt["traded_this_cycle"] = False
    write_state()


async def fetch_one(sym: str):
    info = markets[sym]["info"]
    if not info:
        return
    loop = asyncio.get_event_loop()
    try:
        up_metrics, err_up = await loop.run_in_executor(
            None, get_order_book_metrics, info["up_token_id"]
        )
        dn_metrics, err_dn = await loop.run_in_executor(
            None, get_order_book_metrics, info["down_token_id"]
        )

        if up_metrics and dn_metrics:
            markets[sym]["up_bid"] = up_metrics["best_bid"]
            markets[sym]["up_ask"] = up_metrics["best_ask"]
            markets[sym]["dn_bid"] = dn_metrics["best_bid"]
            markets[sym]["dn_ask"] = dn_metrics["best_ask"]

            def calc_mid(bid, ask):
                if bid > 0 and ask > 0:
                    return round((bid + ask) / 2, 4)
                elif bid > 0:
                    return round(bid, 4)
                elif ask > 0:
                    return round(ask, 4)
                return 0.0

            up_mid = calc_mid(up_metrics["best_bid"], up_metrics["best_ask"])
            markets[sym]["up_mid"] = up_mid
            markets[sym]["dn_mid"] = calc_mid(dn_metrics["best_bid"], dn_metrics["best_ask"])

            if up_mid > 0:
                mid_history[sym].append(up_mid)

            secs = seconds_remaining(info)
            if secs is not None:
                markets[sym]["time_left"] = f"{int(secs)}s"
                if secs <= 0:
                    markets[sym]["info"] = None
            else:
                markets[sym]["time_left"] = "N/A"
            markets[sym]["error"] = None
        else:
            markets[sym]["error"] = err_up or err_dn or "error ob"
    except Exception as e:
        markets[sym]["error"] = str(e)


async def fetch_all():
    await asyncio.gather(*[fetch_one(sym) for sym in SYMBOLS])


# ═══════════════════════════════════════════════════════
#  SEÑALES Y LÓGICA DE TRADING
# ═══════════════════════════════════════════════════════

def compute_signals():
    def normalized_up(s):
        # Usar precio real — NO clampear 0.98 a 1.0.
        # Clampearlo aplana la harmonica y destruye la divergencia armonica
        # cuando un activo esta en 0.98/0.99 pero sin confirmar resolucion.
        mid = markets[s]["up_mid"]
        if mid <= 0:
            return 0.0
        return mid

    def normalized_dn(s):
        mid = markets[s]["dn_mid"]
        if mid <= 0:
            return 0.0
        return mid

    up_mids = {s: normalized_up(s) for s in SYMBOLS if markets[s]["up_mid"] > 0}
    dn_mids = {s: normalized_dn(s) for s in SYMBOLS if markets[s]["dn_mid"] > 0}

    if len(up_mids) < 2:
        bt["signal_asset"] = None
        return

    harm_up = harmonic_mean(list(up_mids.values()))
    harm_dn = harmonic_mean(list(dn_mids.values()))
    bt["harm_up"] = harm_up
    bt["harm_dn"] = harm_dn

    cheapest_up, div_up = find_cheapest(up_mids, harm_up)
    cheapest_dn, div_dn = find_cheapest(dn_mids, harm_dn)

    if abs(div_up) >= abs(div_dn) and cheapest_up:
        bt["signal_asset"] = cheapest_up
        bt["signal_side"]  = "UP"
        bt["signal_div"]   = div_up
    elif cheapest_dn:
        bt["signal_asset"] = cheapest_dn
        bt["signal_side"]  = "DOWN"
        bt["signal_div"]   = div_dn
    else:
        bt["signal_asset"] = None

    if bt["signal_asset"] and bt["signal_side"]:
        peers = [s for s in SYMBOLS if s != bt["signal_asset"]]
        if bt["signal_side"] == "UP":
            peer_vals = [markets[p]["up_mid"] for p in peers if markets[p]["up_mid"] > 0]
        else:
            peer_vals = [markets[p]["dn_mid"] for p in peers if markets[p]["dn_mid"] > 0]

        if len(peer_vals) == 2 and all(v > CONSENSUS_FULL for v in peer_vals):
            bt["consensus"] = "FULL"
        elif len(peer_vals) >= 1 and sum(1 for v in peer_vals if v > CONSENSUS_SOFT) >= 1:
            bt["consensus"] = "SOFT"
        else:
            bt["consensus"] = "NONE"


def _build_single_position(sym: str, side: str, secs: float,
                            harm_entry: float, gap_entry: float,
                            capital_before: float) -> dict | None:
    """
    Construye una posición individual para un símbolo.
    Retorna None si el activo no cumple los filtros de precio.
    """
    if side == "UP":
        entry_ask = markets[sym]["up_ask"]
        entry_bid = markets[sym]["up_bid"]
        entry_mid = markets[sym]["up_mid"]
    else:
        entry_ask = markets[sym]["dn_ask"]
        entry_bid = markets[sym]["dn_bid"]
        entry_mid = markets[sym]["dn_mid"]

    if entry_ask <= 0 or entry_ask >= 1:
        log_event(f"SKIP {side} {sym} — ask inválido ({entry_ask:.4f})")
        return None

    # Solo bloquear si el activo esta CONFIRMADAMENTE resuelto (5s sostenidos)
    # Un tick momentaneo en 0.98/0.02 NO descarta la posicion
    if _is_confirmed_resolved(sym) is not None:
        log_event(f"SKIP {side} {sym} — activo confirmado resuelto (5s sostenidos)")
        return None

    if entry_ask < ENTRY_MIN_PRICE:
        log_event(f"SKIP {side} {sym} — ask={entry_ask:.4f} bajo mínimo {ENTRY_MIN_PRICE}")
        return None

    shares = round(ENTRY_USD / entry_ask, 6)

    return {
        "asset":           sym,
        "side":            side,
        "entry_price":     entry_ask,
        "entry_bid":       entry_bid,
        "entry_mid":       entry_mid,
        "entry_usd":       ENTRY_USD,
        "shares":          shares,
        "secs_left_entry": secs,
        "harm_entry":      harm_entry,
        "gap_entry":       gap_entry,
        "entry_ts":        datetime.now().isoformat(),
        "consensus_entry": bt["consensus"],
        "peer_snaps":      {
            p: {"up_mid": markets[p]["up_mid"], "dn_mid": markets[p]["dn_mid"]}
            for p in SYMBOLS if p != sym
        },
        "capital_before":  capital_before,
        "market_info_snapshot": {
            "condition_id":       markets[sym]["info"].get("condition_id") if markets[sym]["info"] else None,
            "gamma_condition_id": markets[sym]["info"].get("gamma_condition_id") if markets[sym]["info"] else None,
            "market_slug":        markets[sym]["info"].get("market_slug", "") if markets[sym]["info"] else "",
            # Token IDs para verificar resolución directamente en el CLOB (más rápido que Gamma)
            "up_token_id":        markets[sym]["info"].get("up_token_id") if markets[sym]["info"] else None,
            "down_token_id":      markets[sym]["info"].get("down_token_id") if markets[sym]["info"] else None,
        },
    }


def check_entry():
    """
    Abre posición en los 3 activos simultáneamente cuando se detecta
    el armónico con consenso FULL y divergencia dentro del rango válido.
    Todos entran por el mismo side (el del armónico).
    """
    if bt["traded_this_cycle"]:
        return
    if not bt["entry_window"]:
        return
    if bt["consensus"] != "FULL":
        bt["skipped"] += 1
        return
    if not bt["signal_asset"]:
        return

    div_abs = abs(bt["signal_div"])
    if div_abs < DIVERGENCE_THRESHOLD:
        return
    if div_abs > DIVERGENCE_MAX:
        log_event(
            f"SKIP — gap={div_abs*100:.1f}pts excede máximo {DIVERGENCE_MAX*100:.0f}pts"
        )
        bt["skipped"] += 1
        return

    side        = bt["signal_side"]
    harm_entry  = bt["harm_up"] if side == "UP" else bt["harm_dn"]
    gap_entry   = bt["signal_div"]
    secs        = min_secs_remaining() or 0

    # Intentar abrir los 3
    nuevas_posiciones = []
    for sym in SYMBOLS:
        capital_before = bt["capital"]   # se va actualizando por posición
        pos = _build_single_position(sym, side, secs, harm_entry, gap_entry, capital_before)
        if pos:
            bt["capital"] -= ENTRY_USD
            nuevas_posiciones.append(pos)
            log_event(
                f"ENTRADA {side} {sym} @ ask={pos['entry_price']:.4f} | "
                f"div={gap_entry*100:+.1f}pts | arm={harm_entry:.4f} | "
                f"shares={pos['shares']:.4f} | capital=${bt['capital']:.2f}"
            )

    if not nuevas_posiciones:
        log_event("SKIP — ningún activo pasó los filtros de precio")
        bt["skipped"] += 1
        return

    bt["positions"] = nuevas_posiciones
    bt["traded_this_cycle"] = True
    log_event(f"BASKET ABIERTO — {len(nuevas_posiciones)}/3 posiciones en {side}")
    write_state()



def _apply_resolution(pos, resolved, source="CLOB"):
    sym  = pos["asset"]
    side = pos["side"]
    if resolved == side:
        pnl     = round((pos["shares"] - 1) * ENTRY_USD, 6)
        outcome = "WIN"
        bt["wins"] += 1
    else:
        pnl     = -ENTRY_USD
        outcome = "LOSS"
        bt["losses"] += 1
    bt["capital"]   += ENTRY_USD + pnl
    bt["total_pnl"] += pnl
    update_drawdown()
    log_event(
        f"RESOLUCIÓN {outcome} [{source}] {side} {sym} → {resolved} | "
        f"PnL=${pnl:+.4f} | Capital=${bt['capital']:.4f}"
    )
    _record_trade(pos, resolved, outcome, pnl, source=source)


RESOLUTION_CONFIRM_SAMPLES = 10   # 10 muestras x 0.5s = 5s sostenidos


def _is_confirmed_resolved(sym):
    """Retorna 'UP', 'DOWN' o None.
    Solo resuelve si TODAS las ultimas RESOLUTION_CONFIRM_SAMPLES muestras
    del mid_history superan los umbrales — evita falsos positivos por un
    tick momentaneo en 0.98 cuando el mercado sigue activo.
    """
    history = list(mid_history[sym])
    if len(history) < RESOLUTION_CONFIRM_SAMPLES:
        return None  # historia insuficiente — no resolver todavia
    recent = history[-RESOLUTION_CONFIRM_SAMPLES:]
    if all(v >= RESOLVED_UP_THRESH for v in recent):
        return "UP"
    if all(v <= RESOLVED_DN_THRESH for v in recent):
        return "DOWN"
    return None


def check_resolution():
    if not bt["positions"]:
        return

    secs = min_secs_remaining()
    market_expired = all(markets[s]["info"] is None for s in SYMBOLS)
    if secs is not None and secs > RESOLUTION_MAX_SECS and not market_expired:
        return

    cerradas = []
    for pos in bt["positions"]:
        sym = pos["asset"]

        # Primero intentar confirmar por CLOB (precio sostenido 0.98/0.02 por 5s)
        resolved = _is_confirmed_resolved(sym)
        if resolved:
            _apply_resolution(pos, resolved)
            cerradas.append(pos)
            continue

        # Mercado expirado sin confirmación CLOB → esperar a Gamma
        if market_expired or (secs is not None and secs <= 0):
            _move_to_pending(pos)
            cerradas.append(pos)

    for pos in cerradas:
        bt["positions"].remove(pos)

    if cerradas:
        write_state()


# ═══════════════════════════════════════════════════════
#  PERSISTENCIA
# ═══════════════════════════════════════════════════════

def _build_trade_record(pos, exit_type, exit_price, resolved, outcome, pnl):
    exit_ts    = datetime.now().isoformat()
    duration_s = round((datetime.fromisoformat(exit_ts) - datetime.fromisoformat(pos["entry_ts"])).total_seconds(), 1)
    trade_number = bt["wins"] + bt["losses"]

    peers      = [s for s in SYMBOLS if s != pos["asset"]]
    peer_snaps = pos.get("peer_snaps", {})

    def peer_mids(p):
        snap = peer_snaps.get(p, {})
        side = pos["side"]
        if side == "UP":
            return snap.get("up_mid", 0.0), snap.get("dn_mid", 0.0)
        else:
            return snap.get("dn_mid", 0.0), snap.get("up_mid", 0.0)

    p1_side_mid, p1_opp_mid = peer_mids(peers[0]) if len(peers) > 0 else (0.0, 0.0)
    p2_side_mid, p2_opp_mid = peer_mids(peers[1]) if len(peers) > 1 else (0.0, 0.0)

    sl_price = 0.0
    max_win  = round((1.0 - pos["entry_price"]) / pos["entry_price"] * pos["entry_usd"], 6)

    binary_win = 1 if outcome == "WIN" and exit_type == "RESOLUTION" else \
                 0 if outcome == "LOSS" and exit_type == "RESOLUTION" else -1

    return {
        "trade_id":         f"T{trade_number:04d}",
        "entry_ts":         pos["entry_ts"],
        "exit_ts":          exit_ts,
        "duration_s":       duration_s,
        "asset":            pos["asset"],
        "side":             pos["side"],
        "consensus":        pos["consensus_entry"],
        "entry_ask":        round(pos["entry_price"], 6),
        "entry_bid":        round(pos["entry_bid"], 6),
        "entry_mid":        round(pos["entry_mid"], 6),
        "entry_usd":        round(pos["entry_usd"], 4),
        "shares":           round(pos["shares"], 6),
        "secs_left_entry":  round(pos["secs_left_entry"], 1),
        "harm_entry":       round(pos["harm_entry"], 6),
        "gap_pts":          round(pos["gap_entry"] * 100, 2),
        "peer1_sym":        peers[0] if len(peers) > 0 else "",
        "peer1_side_mid":   round(p1_side_mid, 6),
        "peer1_opp_mid":    round(p1_opp_mid, 6),
        "peer2_sym":        peers[1] if len(peers) > 1 else "",
        "peer2_side_mid":   round(p2_side_mid, 6),
        "peer2_opp_mid":    round(p2_opp_mid, 6),
        "sl_price":         sl_price,
        "exit_type":        exit_type,
        "exit_price":       round(exit_price, 6),
        "resolved":         resolved or "",
        "binary_win":       binary_win,
        "pnl_usd":          round(pnl, 6),
        "pnl_pct_entry":    round(pnl / pos["entry_usd"] * 100, 2),
        "max_possible_win": max_win,
        "outcome":          outcome,
        "capital_before":   round(pos["capital_before"], 4),
        "capital_after":    round(bt["capital"], 4),
        "cumulative_pnl":   round(bt["total_pnl"], 6),
        "trade_number":     trade_number,
    }


def _save_csv(record: dict):
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(record)


def _record_trade(pos, resolved, outcome, pnl, source="CLOB"):
    exit_price = 1.0 if resolved == pos["side"] else 0.0
    record = _build_trade_record(pos, source, exit_price, resolved, outcome, pnl)
    bt["trades"].append(record)
    _save_csv(record)
    _save_log()




def _save_log():
    total = bt["wins"] + bt["losses"]
    with open(LOG_FILE, "w") as f:
        json.dump({
            "summary": {
                "capital_inicial": CAPITAL_TOTAL,
                "capital_actual":  round(bt["capital"], 4),
                "total_pnl_usd":   round(bt["total_pnl"], 4),
                "roi_pct":         round((bt["capital"] - CAPITAL_TOTAL) / CAPITAL_TOTAL * 100, 2),
                "max_drawdown":    round(bt["max_drawdown"], 4),
                "wins":            bt["wins"],
                "losses":          bt["losses"],
                "win_rate":        round(bt["wins"] / total * 100, 1) if total else 0,
                "skipped":         bt["skipped"],
                "entry_usd":       ENTRY_USD,
            },
            "trades": bt["trades"],
        }, f, indent=2)


# ═══════════════════════════════════════════════════════
#  LOOP PRINCIPAL
# ═══════════════════════════════════════════════════════

async def main_loop():
    log_event("basket.py iniciado — SIMULACION BINARIA v6 (basket 3 activos)")
    log_event(f"Capital: ${CAPITAL_TOTAL:.0f} | Entrada: ${ENTRY_USD:.2f}x3 = ${ENTRY_USD*3:.2f} por ciclo")
    log_event(f"div>={DIVERGENCE_THRESHOLD:.0%} ≤{DIVERGENCE_MAX:.0%} | Ventana {ENTRY_OPEN_SECS}s–{ENTRY_WINDOW_SECS}s")

    restore_state_from_csv()

    bt["phase"] = "ACTIVO"
    write_state()
    await discover_all()

    while True:
        try:
            secs = min_secs_remaining()

            if secs is not None and secs > WAKE_UP_SECS and not bt["positions"] and not bt["pending_positions"]:
                sleep_duration = secs - WAKE_UP_SECS
                wake_at = datetime.fromtimestamp(time.time() + sleep_duration).strftime("%H:%M:%S")
                bt["phase"]        = "DURMIENDO"
                bt["entry_window"] = False

                slept = 0
                while slept < sleep_duration:
                    chunk = min(5.0, sleep_duration - slept)
                    await asyncio.sleep(chunk)
                    slept += chunk
                    bt["next_wake"] = f"{wake_at} (en {int(max(0, sleep_duration - slept))}s)"
                    write_state()

                bt["phase"] = "ACTIVO"
                log_event(f"Despertando — faltan ~{WAKE_UP_SECS}s")
                await discover_all()
                continue

            bt["phase"] = "ACTIVO"
            bt["cycle"] += 1

            await fetch_all()

            secs = min_secs_remaining()
            bt["entry_window"] = (
                secs is not None and
                secs <= ENTRY_WINDOW_SECS and
                secs >= ENTRY_OPEN_SECS and
                secs > ENTRY_CLOSE_SECS
            )

            if bt["positions"]:
                check_resolution()

            if all(markets[s]["info"] is None for s in SYMBOLS):
                if bt["positions"]:
                    log_event("Mercado expirado con posiciones abiertas — moviendo a pendientes...")
                    check_resolution()
                if not bt["positions"]:
                    log_event("Ciclo expirado — buscando nuevo ciclo...")
                    await discover_all()
                continue

            if not bt["positions"]:
                compute_signals()
                check_entry()

            write_state()

        except Exception as e:
            log_event(f"Error en loop: {e}")
            write_state()

        await asyncio.sleep(POLL_INTERVAL)


# ═══════════════════════════════════════════════════════
#  DASHBOARD EN HILO SECUNDARIO
# ═══════════════════════════════════════════════════════

def run_dashboard():
    import importlib.util
    spec = importlib.util.spec_from_file_location("dashboard", "dashboard.py")
    dash = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dash)
    port = int(os.environ.get("PORT", 5000))
    log.info(f"Dashboard iniciando en puerto {port}")
    dash.app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


# ═══════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("=" * 54)
    log.info("  BASKET — DIVERGENCIA ARMONICA  [BINARIO]  v6")
    log.info(f"  Capital: ${CAPITAL_TOTAL:.0f}  |  Entrada: ${ENTRY_USD:.2f}x3 por ciclo")
    log.info(f"  Gap: {DIVERGENCE_THRESHOLD*100:.0f}pts — {DIVERGENCE_MAX*100:.0f}pts  |  Ventana: {ENTRY_OPEN_SECS}s — {ENTRY_WINDOW_SECS}s")
    log.info("  SIMULACION — SIN DINERO REAL")
    log.info("=" * 54)
    log.info(f"State -> {STATE_FILE} | Log -> {LOG_FILE}")

    t = threading.Thread(target=run_dashboard, daemon=True)
    t.start()

    try:
        async def run_all():
            await asyncio.gather(
                main_loop(),
                pending_resolution_loop(),
            )
        asyncio.run(run_all())
    except KeyboardInterrupt:
        log.info("Basket detenido.")
        total = bt["wins"] + bt["losses"]
        roi   = (bt["capital"] - CAPITAL_TOTAL) / CAPITAL_TOTAL * 100
        log.info(f"Capital final: ${bt['capital']:.4f}  (ROI: {roi:+.2f}%)")
        log.info(f"P&L total: ${bt['total_pnl']:+.4f}")
        log.info(f"Trades: {total}  (WIN: {bt['wins']}  LOSS: {bt['losses']})")
