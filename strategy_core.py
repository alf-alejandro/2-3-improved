"""
strategy_core.py — Market discovery + order book metrics + signal engine

Configurable via env vars:
  SYMBOL = SOL | BTC   (default: SOL)

v3: fetch_market_resolution corregido:
  - Parámetro Gamma correcto: condition_ids (plural, no conditionId)
  - Búsqueda primaria por slug (100% confiable, ya funciona en find_active_market)
  - Validación de que el conditionId en la respuesta coincide con el pedido
  - Fallback a condition_ids si no hay slug disponible
  - build_market_info guarda gamma_condition_id y market_slug para resolución
"""

import os
import sys
import time
import json as _json
import requests
from datetime import datetime, timezone
from collections import deque
from py_clob_client.client import ClobClient

CLOB_HOST   = "https://clob.polymarket.com"
GAMMA_API   = "https://gamma-api.polymarket.com"
SLOT_ORIGIN = 1771778100   # slot anchor compartido SOL y BTC (Feb 22 2026)
SLOT_STEP   = 300          # 5 minutos
TOP_LEVELS  = 15

SYMBOL      = os.environ.get("SYMBOL", "SOL").upper()
SLUG_PREFIX = "btc-updown-5m" if SYMBOL == "BTC" else "sol-updown-5m"
MARKET_NAME = "Bitcoin" if SYMBOL == "BTC" else "Solana"

SLUG_PREFIXES = {
    "SOL": "sol-updown-5m",
    "BTC": "btc-updown-5m",
    "ETH": "eth-updown-5m",
}


# ── Market discovery ──────────────────────────────────────────────────────────

def get_current_slot_ts():
    now     = int(time.time())
    elapsed = (now - SLOT_ORIGIN) % SLOT_STEP
    return now - elapsed


def fetch_gamma_market(slug: str):
    try:
        r = requests.get(f"{GAMMA_API}/markets", params={"slug": slug}, timeout=8)
        r.raise_for_status()
        data = r.json()
        return data[0] if isinstance(data, list) and data else None
    except Exception:
        return None


def fetch_clob_market(condition_id: str):
    try:
        r = requests.get(f"{CLOB_HOST}/markets/{condition_id}", timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def build_market_info(gamma_m, clob_m) -> dict | None:
    tokens = clob_m.get("tokens", [])
    if len(tokens) < 2:
        return None

    up_t   = next((t for t in tokens if "up"   in (t.get("outcome") or "").lower()), tokens[0])
    down_t = next((t for t in tokens if "down" in (t.get("outcome") or "").lower()), tokens[1])

    # Guardar slug y gamma_condition_id para usarlos en fetch_market_resolution
    slug = clob_m.get("market_slug") or gamma_m.get("slug", "")

    return {
        "condition_id":       clob_m.get("condition_id"),
        "gamma_condition_id": gamma_m.get("conditionId"),   # ← ID tal como lo conoce Gamma
        "market_slug":        slug,                          # ← slug para buscar resolución
        "question":           clob_m.get("question", "SOL Up/Down 5min"),
        "end_date":           gamma_m.get("endDate") or clob_m.get("end_date_iso", ""),
        "accepting_orders":   bool(clob_m.get("accepting_orders")),
        "up_token_id":        up_t["token_id"],
        "up_outcome":         up_t.get("outcome", "Up"),
        "up_price":           float(up_t.get("price") or 0.5),
        "down_token_id":      down_t["token_id"],
        "down_outcome":       down_t.get("outcome", "Down"),
        "down_price":         float(down_t.get("price") or 0.5),
    }


def _order_book_live(token_id: str) -> bool:
    try:
        r = requests.get(
            f"{CLOB_HOST}/book",
            params={"token_id": token_id},
            timeout=5,
        )
        return r.status_code == 200
    except Exception:
        return False


def find_active_market(symbol: str) -> dict | None:
    """
    Busca el mercado UP/DOWN 5m activo para el simbolo dado (SOL, BTC, ETH).
    """
    slug_prefix = SLUG_PREFIXES.get(symbol.upper())
    if not slug_prefix:
        raise ValueError(f"Simbolo no soportado: {symbol}. Usa SOL, BTC o ETH.")

    now  = int(time.time())
    base = now - (now % SLOT_STEP)

    for offset in [0, 1, -1, 2, -2, -3]:
        ts   = base + offset * SLOT_STEP
        slug = f"{slug_prefix}-{ts}"
        gm   = fetch_gamma_market(slug)
        if not gm:
            continue
        cid = gm.get("conditionId")
        if not cid:
            continue
        cm = fetch_clob_market(cid)
        if not cm:
            continue
        info = build_market_info(gm, cm)
        if not info:
            continue
        if _order_book_live(info["up_token_id"]):
            return info
    return None


def _parse_gamma_resolution(market: dict) -> str | None:
    """
    Dado un objeto market de Gamma, intenta extraer UP o DOWN.
    Retorna None si aún no hay resolución confirmada (precio < 0.95).
    """
    raw_prices   = market.get("outcomePrices")
    raw_outcomes = market.get("outcomes")

    if not raw_prices:
        return None

    try:
        prices = [float(p) for p in _json.loads(raw_prices)] \
                 if isinstance(raw_prices, str) else [float(p) for p in raw_prices]

        outcomes = _json.loads(raw_outcomes) \
                   if isinstance(raw_outcomes, str) else (raw_outcomes or [])

        if outcomes:
            for outcome, price in zip(outcomes, prices):
                if price >= 0.95:
                    label = str(outcome).strip().upper()
                    if "UP" in label:
                        return "UP"
                    elif "DOWN" in label:
                        return "DOWN"
        else:
            # Sin labels: convención índice 0=Up, 1=Down en mercados 5m de Polymarket
            if prices[0] >= 0.95:
                return "UP"
            elif len(prices) > 1 and prices[1] >= 0.95:
                return "DOWN"
    except Exception:
        pass

    return None


def fetch_market_resolution(condition_id: str, market_slug: str = "") -> str | None:
    """
    Consulta Gamma para obtener el resultado final de un mercado cerrado.
    Retorna 'UP', 'DOWN', o None si aún no está resuelto.

    Bugs corregidos vs versión original:
      1. El parámetro de Gamma es 'condition_ids' (plural), no 'conditionId'.
         Con 'conditionId' Gamma ignora el filtro y retorna mercados random.
      2. La búsqueda por slug es más confiable — es el mismo mecanismo
         que usa find_active_market y nunca falla en discovery.
      3. Se valida que el conditionId de la respuesta coincida con el pedido.
    """

    # ── MÉTODO 1: por slug (más confiable) ───────────────────────────────────
    if market_slug:
        try:
            r = requests.get(
                f"{GAMMA_API}/markets",
                params={"slug": market_slug},
                timeout=8,
            )
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list) and data:
                market = data[0]
                # Validar que el slug coincide exactamente
                if market.get("slug", "").lower() == market_slug.lower():
                    result = _parse_gamma_resolution(market)
                    if result:
                        print(
                            f"[GAMMA OK] slug={market_slug} → {result} "
                            f"prices={market.get('outcomePrices')}",
                            file=sys.stderr, flush=True,
                        )
                        return result
                    # Mercado correcto pero aún sin resolver
                    print(
                        f"[GAMMA WAIT] slug={market_slug} | "
                        f"closed={market.get('closed')} | prices={market.get('outcomePrices')}",
                        file=sys.stderr, flush=True,
                    )
                    return None
        except Exception as e:
            print(f"[GAMMA ERR] slug lookup falló: {e}", file=sys.stderr, flush=True)

    # ── MÉTODO 2: por condition_ids (parámetro plural correcto) ──────────────
    if condition_id:
        try:
            r = requests.get(
                f"{GAMMA_API}/markets",
                params={"condition_ids": condition_id},
                timeout=8,
            )
            r.raise_for_status()
            data = r.json()

            # Buscar el market que realmente coincide — NO asumir data[0]
            # Gamma puede retornar varios resultados o un mercado incorrecto
            market = None
            if isinstance(data, list):
                market = next(
                    (m for m in data
                     if (m.get("conditionId") or "").lower() == condition_id.lower()),
                    None,
                )
            elif isinstance(data, dict):
                if (data.get("conditionId") or "").lower() == condition_id.lower():
                    market = data

            if market:
                result = _parse_gamma_resolution(market)
                if result:
                    print(
                        f"[GAMMA OK] cid={condition_id[:12]}... → {result} "
                        f"prices={market.get('outcomePrices')}",
                        file=sys.stderr, flush=True,
                    )
                    return result
                print(
                    f"[GAMMA WAIT] cid={condition_id[:12]}... | "
                    f"closed={market.get('closed')} | prices={market.get('outcomePrices')}",
                    file=sys.stderr, flush=True,
                )
                return None
            else:
                print(
                    f"[GAMMA MISS] cid={condition_id[:12]}... no encontrado "
                    f"en {len(data) if isinstance(data, list) else 1} resultados. "
                    f"Verificar que el condition_id es correcto.",
                    file=sys.stderr, flush=True,
                )
        except Exception as e:
            print(f"[GAMMA ERR] condition_ids lookup falló: {e}", file=sys.stderr, flush=True)

    return None


def find_active_btc_market() -> dict | None:
    return find_active_market("BTC")


def find_active_eth_market() -> dict | None:
    return find_active_market("ETH")


def seconds_remaining(market_info: dict) -> float | None:
    end_raw = market_info.get("end_date", "")
    if not end_raw:
        return None
    try:
        end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
        diff   = (end_dt - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, diff)
    except Exception:
        return None


# ── Order book ────────────────────────────────────────────────────────────────

_clob_client = None

def get_clob_client() -> ClobClient:
    global _clob_client
    if _clob_client is None:
        _clob_client = ClobClient(CLOB_HOST)
    return _clob_client


def get_order_book_metrics(token_id: str, top_n: int = TOP_LEVELS) -> tuple[dict | None, str | None]:
    try:
        ob = get_clob_client().get_order_book(token_id)
    except Exception as e:
        return None, str(e)

    bids = sorted(ob.bids or [], key=lambda x: float(x.price), reverse=True)[:top_n]
    asks = sorted(ob.asks or [], key=lambda x: float(x.price))[:top_n]

    bid_vol = sum(float(b.size) for b in bids)
    ask_vol = sum(float(a.size) for a in asks)
    total   = bid_vol + ask_vol
    obi     = (bid_vol - ask_vol) / total if total > 0 else 0.0

    best_bid = float(bids[0].price) if bids else 0.0
    best_ask = float(asks[0].price) if asks else 0.0
    spread   = round(best_ask - best_bid, 4)

    if total > 0:
        bvwap = sum(float(b.price) * float(b.size) for b in bids) / bid_vol if bid_vol > 0 else 0
        avwap = sum(float(a.price) * float(a.size) for a in asks) / ask_vol if ask_vol > 0 else 0
        vwap_mid = (bvwap * bid_vol + avwap * ask_vol) / total
    else:
        vwap_mid = (best_bid + best_ask) / 2

    return {
        "bid_volume":   round(bid_vol, 2),
        "ask_volume":   round(ask_vol, 2),
        "total_volume": round(total, 2),
        "obi":          round(obi, 4),
        "best_bid":     round(best_bid, 4),
        "best_ask":     round(best_ask, 4),
        "spread":       spread,
        "vwap_mid":     round(vwap_mid, 4),
        "num_bids":     len(ob.bids or []),
        "num_asks":     len(ob.asks or []),
        "top_bids":     [(round(float(b.price), 4), round(float(b.size), 2)) for b in bids[:8]],
        "top_asks":     [(round(float(a.price), 4), round(float(a.size), 2)) for a in asks[:8]],
    }, None


# ── Signal engine ─────────────────────────────────────────────────────────────

def compute_signal(obi_now: float, obi_window: list[float], threshold: float) -> dict:
    avg_obi  = sum(obi_window) / len(obi_window) if obi_window else obi_now
    combined = round(0.6 * obi_now + 0.4 * avg_obi, 4)
    abs_c    = abs(combined)

    if combined > threshold:
        conf  = min(int(50 + (abs_c / 0.5) * 50), 99)
        label = "STRONG UP" if combined > threshold * 2 else "UP"
        color = "green"
    elif combined < -threshold:
        conf  = min(int(50 + (abs_c / 0.5) * 50), 99)
        label = "STRONG DOWN" if combined < -threshold * 2 else "DOWN"
        color = "red"
    else:
        label = "NEUTRAL"
        color = "yellow"
        conf  = 50

    return {
        "label":      label,
        "color":      color,
        "confidence": conf,
        "obi_now":    round(obi_now, 4),
        "obi_avg":    round(avg_obi, 4),
        "combined":   combined,
        "history":    list(obi_window)[-20:],
        "threshold":  threshold,
    }
