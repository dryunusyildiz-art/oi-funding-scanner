#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
  CRYPTO SPOT POSITIONING-PROXY SCANNER  (main.py'nin spot-uyarlanmis kardesi)
================================================================================

main.py (futures OI + funding botu), Binance futures API'sinin (fapi.binance.com)
GitHub Actions gibi bulut/CI IP'lerinden HTTP 451 ("restricted location") ile
tamamen engellendigi tespit edildikten sonra ortaya cikan bu dosya, AYNI
mimariyi (coklu TF, confluence, BTC filtresi, ATR SL/TP, backtest, cooldown,
sanal performans takibi) korurken, futures'a ozgu OI/funding kavramlarinin
YERINE spot piyasada olculebilen PROXY sinyaller kullanir:

  - OI (Open Interest) yerine -> "HACIM REJIMI": fiyat yonu + hacmin kendi
    ortalamasina gore nispi degisimi. OI "yeni pozisyon aciliyor mu" sorusuna
    cevap verirken, hacim-degisimi "gercek katilim/ilgi artiyor mu" sorusuna
    cevap verir. Ayni 4 REJIM felsefesi korunur:
      fiyat UP + hacim UP    -> ACCUMULATION_BUILDUP  (LONG_BUILDUP karsiligi)
      fiyat DOWN + hacim UP  -> DISTRIBUTION_BUILDUP  (SHORT_BUILDUP karsiligi)
      fiyat UP + hacim DOWN  -> WEAK_RALLY            (SHORT_COVERING karsiligi)
      fiyat DOWN + hacim DOWN-> WEAK_DECLINE          (LONG_UNWIND karsiligi)

  - Funding Rate yerine -> "TAKER BUY RATIO SAPMASI": Binance'in ham kline
    verisindeki 'taker buy base volume' alanindan hesaplanan, bir bar'daki
    hacmin ne kadarinin AGRESIF ALIM (piyasa emriyle yukari vuran) oldugunu
    gosteren oran. %50'den asiri sapma, "gizli guc/zayiflik" (absorption)
    isareti sayilir: fiyat/rejim YUKARI iken agresif SATIS hacmi baskinsa
    (oran dusuk) bu, satislarin sessizce yutuldugu -> LONG icin squeeze-benzeri
    yakit sayilir (negatif funding'in LONG'a bonus vermesiyle AYNI mantik).
    Simetrigi SHORT icin gecerlidir.

  UYARI: Funding, VADELI POZISYONLARIN gercek maliyetini yansitirken, taker
  buy ratio yalnizca O ANKI emir akisini yansitir -- cok daha kisa vadeli ve
  gurultulu bir proxy'dir. Esikler (TAKER_RATIO_*) ilk-tahmin degerlerdir,
  canli veriyle kalibrasyon gerekebilir (tipki orijinal OI esiklerinin de
  zamanla ayarlandigi gibi).

  Likidasyon verisi TAMAMEN KALDIRILDI (spot piyasada likidasyon kavrami yok).

  !!! Bu sistem YATIRIM TAVSIYESI URETMEZ. !!!

Veri kaynagi / GitHub Actions uyumlulugu:
  Binance SPOT public uc noktasi (api.binance.com) bazi bolgelerden/bulut
  IP'lerinden engellenebiliyor (HTTP 451). Bu yuzden varsayilan olarak
  Binance'in resmi, kimlik dogrulama gerektirmeyen, engelsiz spot veri
  aynasi 'data-api.binance.vision' kullanilir (USE_BINANCE_DATA_MIRROR).
  main.py'deki futures/fapi icin BOYLE BIR AYNA YOK -- bu yuzden bu dosya
  yalnizca SPOT calisir ve GitHub Actions/cron-job.org hattinda main.py'nin
  YERINE bu dosya calistirilir (main.py futures oldugu icin hala yalnizca
  senin kendi agindan/PC'nden calisir).

Kullanim:
    python main_spot.py --once          -> tek tur tarama
    python main_spot.py                 -> surekli tarama dongusu
    python main_spot.py --selftest      -> API'siz sahte veri testi
    python main_spot.py --check         -> hizli baglanti testi
    python main_spot.py --backtest      -> gecmis veriyle geriye donuk test
    python main_spot.py --perf          -> sanal performans ozeti
--------------------------------------------------------------------------------
"""

from __future__ import annotations

import os
import io
import sys
import time
import json
import math
import bisect
import asyncio
import logging
import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Any

import numpy as np
import pandas as pd

try:
    import ccxt.async_support as ccxt_async  # type: ignore
except Exception:  # pragma: no cover
    ccxt_async = None

import ssl
try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover
    aiohttp = None
try:
    import certifi  # type: ignore
except Exception:  # pragma: no cover
    certifi = None

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None

# matplotlib: Telegram'a gonderilen fiyat+kanal grafigi icin (main.py ile
# ayni mantik). Yuklu degilse bot cokmez, foto yerine sade metne duser.
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # type: ignore
except Exception:  # pragma: no cover
    plt = None


# ------------------------------------------------------------------------------
# .ENV YUKLEYICI (main.py ile ayni; python-dotenv gerektirmez)
# ------------------------------------------------------------------------------
_ENV_LOADED_FROM: str = ""
_ENV_KEYS_LOADED: list = []


def _load_env_files() -> None:
    global _ENV_LOADED_FROM, _ENV_KEYS_LOADED
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = []
    for d in (here, os.getcwd()):
        for name in (".env", ".env.txt", "env.txt"):
            p = os.path.join(d, name)
            if os.path.isfile(p) and p not in candidates:
                candidates.append(p)
    for path in candidates:
        try:
            with open(path, "rb") as f:
                raw = f.read()
            if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
                text = raw.decode("utf-16")
            else:
                text = raw.decode("utf-8-sig", errors="replace")
            keys = []
            for line in text.splitlines():
                line = line.strip().lstrip("﻿")
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and v and k not in os.environ:
                    os.environ[k] = v
                    keys.append(k)
            if keys:
                _ENV_LOADED_FROM = path
                _ENV_KEYS_LOADED = keys
                return
        except Exception:
            continue


_load_env_files()


# ==============================================================================
# 1) KONFIGURASYON
# ==============================================================================
def _env(key: str, default: str) -> str:
    return os.getenv(key, default)


TELEGRAM_BOT_TOKEN_SABIT = ""
TELEGRAM_CHAT_ID_SABIT = ""


class Config:
    # --- Borsa (yalnizca SPOT) ---
    EXCHANGE_NAME: str = _env("EXCHANGE_NAME", "binance")

    # Binance spot public verisini geo-engelsiz mirror uzerinden cek.
    USE_BINANCE_DATA_MIRROR: bool = _env("USE_BINANCE_DATA_MIRROR", "true").lower() == "true"
    BINANCE_DATA_MIRROR_URL: str = _env("BINANCE_DATA_MIRROR_URL",
                                        "https://data-api.binance.vision/api/v3")

    # --- Zaman dilimleri ---
    TIMEFRAMES: list[str] = [t.strip() for t in _env("TIMEFRAMES", "1h,4h").split(",") if t.strip()]
    OHLCV_LIMIT: int = int(_env("OHLCV_LIMIT", "200"))
    VOLUME_WINDOW: int = int(_env("VOLUME_WINDOW", "6"))   # OI_WINDOW karsiligi

    # --- Async paralellik ---
    MAX_CONCURRENCY: int = int(_env("MAX_CONCURRENCY", "10"))

    # --- Ag / baglanti ---
    REQUEST_TIMEOUT_MS: int = int(_env("REQUEST_TIMEOUT_MS", "20000"))
    SSL_VERIFY: bool = _env("SSL_VERIFY", "true").lower() == "true"

    # --- Dongu ---
    SCAN_INTERVAL_SECONDS: int = int(_env("SCAN_INTERVAL_SECONDS", "900"))

    # --- Hacim rejimi esikleri (yuzde) -- OI_SPIKE_*/OI_WINDOW_* karsiligi ---
    # NOT: Hacim, OI'ye gore cok daha oynak/gurultulu bir seri oldugu icin
    # esikler kasitli olarak OI esiklerinden yuksek secildi. Ilk-tahmin
    # degerleridir; canli veriyle kalibrasyon gerekebilir.
    VOLUME_SPIKE_STRONG: float = float(_env("VOLUME_SPIKE_STRONG", "40.0"))
    VOLUME_SPIKE_WATCH: float = float(_env("VOLUME_SPIKE_WATCH", "20.0"))
    VOLUME_WINDOW_STRONG: float = float(_env("VOLUME_WINDOW_STRONG", "60.0"))
    VOLUME_WINDOW_WATCH: float = float(_env("VOLUME_WINDOW_WATCH", "30.0"))
    PRICE_FLAT_TH: float = float(_env("PRICE_FLAT_TH", "0.20"))
    VOLUME_FLAT_TH: float = float(_env("VOLUME_FLAT_TH", "15.0"))

    # --- Taker buy ratio esikleri (0.5'ten sapma) -- FUNDING_* karsiligi ---
    TAKER_RATIO_EXTREME: float = float(_env("TAKER_RATIO_EXTREME", "0.15"))
    TAKER_RATIO_MODERATE: float = float(_env("TAKER_RATIO_MODERATE", "0.07"))

    # --- Skor esikleri ---
    MIN_SCORE_STRONG: float = float(_env("MIN_SCORE_STRONG", "75"))
    MIN_SCORE_WATCH: float = float(_env("MIN_SCORE_WATCH", "60"))
    MIN_VOLUME_RATIO_STRONG: float = float(_env("MIN_VOLUME_RATIO_STRONG", "1.2"))

    # --- Risk yonetimi: ATR bazli SL/TP ---
    ATR_SL_MULT: float = float(_env("ATR_SL_MULT", "1.5"))
    ATR_TP1_R: float = float(_env("ATR_TP1_R", "2.0"))
    ATR_TP2_R: float = float(_env("ATR_TP2_R", "3.0"))

    # --- Volatilite filtresi ---
    MIN_ATR_PCT: float = float(_env("MIN_ATR_PCT", "0.15"))

    # --- 144 periodluk lineer regresyon kanali filtresi ---
    # main.py ile ayni mantik: fiyat UST banda yakinken sadece SHORT, ALT
    # banda yakinken sadece LONG sinyali dikkate alinir (kanal ici/swing
    # trade R:R'i icin ters yonde sinyal bastirilir). Orta bolgede dokunulmaz.
    CHANNEL_FILTER_ENABLED: bool = _env("CHANNEL_FILTER_ENABLED", "true").lower() == "true"
    CHANNEL_PERIOD: int = int(_env("CHANNEL_PERIOD", "144"))
    CHANNEL_STDEV_MULT: float = float(_env("CHANNEL_STDEV_MULT", "2.0"))
    CHANNEL_EDGE_ZONE: float = float(_env("CHANNEL_EDGE_ZONE", "0.15"))

    # --- Coklu zaman dilimi confluence ---
    CONFLUENCE_BONUS: float = float(_env("CONFLUENCE_BONUS", "8"))
    CONFLUENCE_PENALTY: float = float(_env("CONFLUENCE_PENALTY", "12"))

    # --- BTC piyasa filtresi ---
    BTC_FILTER_ENABLED: bool = _env("BTC_FILTER_ENABLED", "true").lower() == "true"
    BTC_FILTER_BONUS: float = float(_env("BTC_FILTER_BONUS", "5"))
    BTC_FILTER_PENALTY: float = float(_env("BTC_FILTER_PENALTY", "8"))

    # --- Sanal performans takibi ---
    PERF_TRACKING_ENABLED: bool = _env("PERF_TRACKING_ENABLED", "true").lower() == "true"

    # --- Alarm tekrari onleme ---
    ALERT_COOLDOWN_MINUTES: int = int(_env("ALERT_COOLDOWN_MINUTES", "90"))
    SCORE_UPDATE_THRESHOLD: float = float(_env("SCORE_UPDATE_THRESHOLD", "10"))

    # --- Telegram ---
    TELEGRAM_BOT_TOKEN: str = _env("TELEGRAM_BOT_TOKEN", "") or TELEGRAM_BOT_TOKEN_SABIT
    TELEGRAM_CHAT_ID: str = _env("TELEGRAM_CHAT_ID", "") or TELEGRAM_CHAT_ID_SABIT
    TELEGRAM_ENABLED: bool = _env("TELEGRAM_ENABLED", "true").lower() == "true"

    # --- Binance API (opsiyonel; public veri icin gerekmez) ---
    BINANCE_API_KEY: str = _env("BINANCE_API_KEY", "")
    BINANCE_API_SECRET: str = _env("BINANCE_API_SECRET", "")

    # --- Indikator parametreleri ---
    RSI_PERIOD: int = 14
    ATR_PERIOD: int = 14
    EMA_FAST: int = 20
    EMA_SLOW: int = 50
    VOL_MA_PERIOD: int = 20

    DISCLAIMER: str = ("Bu mesaj yatirim tavsiyesi degildir. "
                       "Sadece spot hacim/taker-oran proxy verisine dayali tarama alarmidir.")


SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "AAVE/USDT", "CHZ/USDT",
    "1INCH/USDT", "ADA/USDT", "ALGO/USDT", "API3/USDT", "ARB/USDT",
    "ATOM/USDT", "AVAX/USDT", "BEL/USDT", "BNB/USDT", "CAKE/USDT",
    "CELO/USDT", "DOGE/USDT", "DOT/USDT", "DYDX/USDT", "EGLD/USDT",
    "EIGEN/USDT", "ENJ/USDT", "FET/USDT", "FIL/USDT", "FLUX/USDT",
    "GALA/USDT", "HBAR/USDT", "INJ/USDT", "KAVA/USDT", "LINK/USDT",
    "MANA/USDT", "NEAR/USDT", "NEO/USDT", "PEPE/USDT", "POL/USDT",
    "RARE/USDT", "RENDER/USDT", "RVN/USDT", "SAND/USDT", "SHIB/USDT",
    "SNX/USDT", "SOL/USDT", "SPK/USDT", "SUI/USDT", "TAO/USDT",
    "TRU/USDT", "VET/USDT", "XRP/USDT", "XTZ/USDT",
    "XVG/USDT", "ZRO/USDT",
]

STORAGE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spot_alerts_cache.json")
PERF_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spot_virtual_trades.json")
ALERT_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spot_alert_log.json")


# ==============================================================================
# 2) LOGLAMA
# ==============================================================================
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("spot_scanner")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%H:%M:%S")
    h.setFormatter(fmt)
    logger.addHandler(h)
    return logger


log = setup_logger()


# ==============================================================================
# 3) INDIKATORLER
# ==============================================================================
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def rma(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(alpha=1.0 / period, adjust=False).mean()


def true_range(df: pd.DataFrame) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr


def atr_percent(df: pd.DataFrame, period: int = 14) -> pd.Series:
    return rma(true_range(df), period) / df["close"] * 100.0


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = rma(gain, period)
    avg_loss = rma(loss, period)
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    out = 100.0 - (100.0 / (1.0 + rs))
    return out.fillna(100.0)


def linreg_channel(close: pd.Series, period: int, stdev_mult: float) -> tuple[float, float, float, float]:
    """Son `period` bar uzerinden lineer regresyon kanali.

    Doner: (mid, upper, lower, channel_pos). channel_pos: 0.0 = alt bant,
    1.0 = ust bant (kanal disinda <0/>1 olabilir). Yetersiz veri -> NaN.
    """
    if len(close) < period:
        nan = float("nan")
        return nan, nan, nan, nan
    y = close.tail(period).to_numpy(dtype=float)
    x = np.arange(period, dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    fitted = slope * x + intercept
    resid_std = float(np.std(y - fitted))
    mid = float(fitted[-1])
    upper = mid + stdev_mult * resid_std
    lower = mid - stdev_mult * resid_std
    price = float(y[-1])
    channel_pos = (price - lower) / (upper - lower) if upper > lower else 0.5
    return mid, upper, lower, channel_pos


def wick_ratio(row: pd.Series) -> float:
    rng = row["high"] - row["low"]
    if rng <= 0:
        return 0.0
    body_hi = max(row["open"], row["close"])
    body_lo = min(row["open"], row["close"])
    upper_wick = row["high"] - body_hi
    lower_wick = body_lo - row["low"]
    return float((upper_wick + lower_wick) / rng)


def pct_change(now: float, prev: float) -> float:
    if prev == 0 or math.isnan(prev):
        return 0.0
    return (now - prev) / prev * 100.0


# ==============================================================================
# 4) VERI MODELLERI
# ==============================================================================
REGIME_ACCUMULATION_BUILDUP = "ACCUMULATION_BUILDUP"   # fiyat UP + hacim UP (LONG_BUILDUP karsiligi)
REGIME_DISTRIBUTION_BUILDUP = "DISTRIBUTION_BUILDUP"   # fiyat DOWN + hacim UP (SHORT_BUILDUP karsiligi)
REGIME_WEAK_RALLY = "WEAK_RALLY"                       # fiyat UP + hacim DOWN (SHORT_COVERING karsiligi)
REGIME_WEAK_DECLINE = "WEAK_DECLINE"                   # fiyat DOWN + hacim DOWN (LONG_UNWIND karsiligi)
REGIME_NEUTRAL = "NEUTRAL"

REGIME_LABEL_TR = {
    REGIME_ACCUMULATION_BUILDUP: "ACCUMULATION BUILDUP (fiyat UP + hacim UP, gercek katilim)",
    REGIME_DISTRIBUTION_BUILDUP: "DISTRIBUTION BUILDUP (fiyat DOWN + hacim UP, gercek satis)",
    REGIME_WEAK_RALLY: "WEAK RALLY (fiyat UP + hacim DOWN, arkasi bos yukselis)",
    REGIME_WEAK_DECLINE: "WEAK DECLINE (fiyat DOWN + hacim DOWN, arkasi bos dusus)",
    REGIME_NEUTRAL: "NOTR (belirgin katilim degisimi yok)",
}


@dataclass
class MarketSnapshot:
    symbol: str
    price: float
    price_chg_1: float
    price_chg_w: float
    # hacim (OI proxy)
    vol_ratio: float             # guncel bar hacmi / VOL_MA_PERIOD ortalama
    vol_chg_1: float              # vol_ratio'nun son 1 bar degisimi %
    vol_chg_w: float              # vol_ratio'nun son VOLUME_WINDOW bar degisimi %
    vol_rising_streak: int
    # taker buy ratio (funding proxy)
    taker_buy_ratio: float        # NaN olabilir (ham veri yoksa)
    # rejim
    regime: str
    # teyit indikatorleri
    rsi: float
    atr_pct: float
    ema20: float
    ema50: float
    last_wick_ratio: float
    # 144 periodluk lineer regresyon kanali (yetersiz veri varsa NaN)
    channel_mid: float = float("nan")
    channel_upper: float = float("nan")
    channel_lower: float = float("nan")
    channel_pos: float = float("nan")
    # kanal grafigi cizmek icin son CHANNEL_PERIOD kapanis fiyati (bellek-ici)
    close_series: list = field(default_factory=list)


@dataclass
class Signal:
    symbol: str
    timeframe: str
    direction: str
    score: float
    volume_regime_score: float
    regime_score: float
    taker_score: float
    volume_score: float
    trend_score: float
    confluence_adj: float = 0.0
    btc_filter_adj: float = 0.0
    risk_penalty: float = 0.0
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    price: float = 0.0
    invalidation: str = ""
    ex_symbol: str = ""
    tier: str = "NONE"
    snap: Optional[MarketSnapshot] = None
    stop_loss: float = 0.0
    take_profit1: float = 0.0
    take_profit2: float = 0.0
    risk_pct: float = 0.0


# ==============================================================================
# 5) BORSA ISTEMCISI & VERI CEKME (ASENKRON, SADECE SPOT)
# ==============================================================================
class ExchangeClient:
    def __init__(self, cfg: Config):
        if ccxt_async is None:
            raise RuntimeError("ccxt yuklu degil. 'pip install ccxt' calistirin.")
        self.cfg = cfg
        name = cfg.EXCHANGE_NAME.strip().lower()
        self.name = name

        options: dict[str, Any] = {"adjustForTimeDifference": True}
        if name == "binance":
            options["defaultType"] = "spot"
            options["fetchMarkets"] = ["spot"]

        params: dict[str, Any] = {
            "enableRateLimit": True,
            "timeout": cfg.REQUEST_TIMEOUT_MS,
            "options": options,
        }
        if cfg.BINANCE_API_KEY and cfg.BINANCE_API_SECRET:
            params["apiKey"] = cfg.BINANCE_API_KEY
            params["secret"] = cfg.BINANCE_API_SECRET
        exchange_cls = getattr(ccxt_async, name)
        self.ex = exchange_cls(params)
        self.markets: dict = {}

        # Binance spot: public veri uc noktasini geo-engelsiz mirror'a yonlendir.
        if name == "binance" and cfg.USE_BINANCE_DATA_MIRROR:
            try:
                self.ex.urls["api"]["public"] = cfg.BINANCE_DATA_MIRROR_URL
                self._mirror = cfg.BINANCE_DATA_MIRROR_URL
            except Exception:
                self._mirror = ""
        else:
            self._mirror = ""

        self._ssl_context: Optional[ssl.SSLContext] = None
        self._session = None
        if not cfg.SSL_VERIFY:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            self._ssl_context = ctx
        elif certifi is not None:
            self._ssl_context = ssl.create_default_context(cafile=certifi.where())

    async def _prepare_session(self) -> None:
        if self._session is None and aiohttp is not None:
            try:
                resolver = aiohttp.ThreadedResolver()
            except Exception:
                resolver = None
            conn_kwargs: dict[str, Any] = {}
            if self._ssl_context is not None:
                conn_kwargs["ssl"] = self._ssl_context
            if resolver is not None:
                conn_kwargs["resolver"] = resolver
            connector = aiohttp.TCPConnector(**conn_kwargs)
            self._session = aiohttp.ClientSession(connector=connector, trust_env=True)
            self.ex.session = self._session

    async def load_markets(self) -> None:
        await self._prepare_session()
        if self._mirror:
            log.info("Binance spot public verisi mirror uzerinden: %s", self._mirror)
        self.markets = await self.ex.load_markets()
        log.info("Markets yuklendi: %d parite (%s | spot)", len(self.markets), self.name)

    async def close(self) -> None:
        try:
            await self.ex.close()
        except Exception:
            pass
        try:
            if self._session is not None:
                await self._session.close()
        except Exception:
            pass

    def resolve_symbol(self, symbol: str) -> Optional[str]:
        base, _, quote = symbol.partition("/")
        bases = [base]
        if not base.startswith("1000"):
            bases.append("1000" + base)
        if base.startswith("1000"):
            bases.append(base[4:])
        for b in bases:
            pair = f"{b}/{quote}"
            m = self.markets.get(pair)
            if m and m.get("active", True):
                return pair
        return None

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
        try:
            raw = await self.ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if not raw or len(raw) < 60:
                return None
            df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df = df.astype({"open": float, "high": float, "low": float,
                            "close": float, "volume": float})
            return df
        except Exception as e:
            log.warning("OHLCV alinamadi %s %s: %s", symbol, timeframe, e)
            return None

    async def fetch_ohlcv_with_taker(self, symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
        """Ham Binance kline'i ceker (12 kolon); 'taker buy base volume'
        kolonunu (index 9) da tasir. Bu, ccxt'nin unified fetch_ohlcv'sinde
        YOKTUR (o sadece ilk 6 kolonu doner). Ham istek basarisiz olursa
        (ornegin baska bir borsa/ccxt surumunde metod adi farkliysa) standart
        fetch_ohlcv'ye sessizce geri duser -- bu durumda taker_buy_ratio NaN
        kalir, ilgili skor bileseni 0 katki verir, sistem COKMEZ.
        """
        try:
            market = self.ex.market(symbol)
            params = {"symbol": market["id"], "interval": timeframe, "limit": limit}
            raw = await self.ex.publicGetKlines(params)
            if not raw or len(raw) < 60:
                return None
            rows = []
            for k in raw:
                rows.append([int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]),
                            float(k[5]), float(k[9])])
            df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close",
                                              "volume", "taker_buy_base"])
            return df
        except Exception as e:
            log.warning("Ham kline (taker verisi) alinamadi %s %s: %s -- standart OHLCV'ye "
                       "dusuluyor (taker orani bu turda hesaplanamayacak)", symbol, timeframe, e)
            return await self.fetch_ohlcv(symbol, timeframe, limit)


# ==============================================================================
# 6) VERI KALITE KONTROLLERI
# ==============================================================================
def validate_ohlcv(df: Optional[pd.DataFrame]) -> tuple[bool, str]:
    if df is None or len(df) < 60:
        return False, "yetersiz mum"
    if df[["open", "high", "low", "close"]].le(0).any().any():
        return False, "OHLC sifir/negatif"
    if df["close"].isna().any():
        return False, "NaN close"
    diffs = df["timestamp"].diff().dropna()
    if len(diffs) > 5:
        common = diffs.mode().iloc[0]
        if (diffs > common * 2.5).sum() > 2:
            return False, "eksik mum (bosluk)"
    return True, "ok"


# ==============================================================================
# 7) SNAPSHOT INSA
# ==============================================================================
def determine_volume_regime(cfg: Config, price_chg: float, vol_chg: float) -> str:
    p_up = price_chg >= cfg.PRICE_FLAT_TH
    p_dn = price_chg <= -cfg.PRICE_FLAT_TH
    v_up = vol_chg >= cfg.VOLUME_FLAT_TH
    v_dn = vol_chg <= -cfg.VOLUME_FLAT_TH
    if v_up and p_up:
        return REGIME_ACCUMULATION_BUILDUP
    if v_up and p_dn:
        return REGIME_DISTRIBUTION_BUILDUP
    if v_dn and p_up:
        return REGIME_WEAK_RALLY
    if v_dn and p_dn:
        return REGIME_WEAK_DECLINE
    return REGIME_NEUTRAL


def rising_streak(values: pd.Series) -> int:
    streak = 0
    v = values.values
    for i in range(len(v) - 1, 0, -1):
        if v[i] > v[i - 1]:
            streak += 1
        else:
            break
    return streak


def build_snapshot(cfg: Config, symbol: str, df: pd.DataFrame) -> MarketSnapshot:
    close = df["close"]
    price = float(close.iloc[-1])
    w = cfg.VOLUME_WINDOW

    price_chg_1 = pct_change(price, float(close.iloc[-2]))
    price_chg_w = pct_change(price, float(close.iloc[-1 - w])) if len(close) > w else price_chg_1

    # --- hacim rejimi (OI proxy): vol_ratio = bar hacmi / hareketli ortalama ---
    vol_ma = df["volume"].rolling(cfg.VOL_MA_PERIOD).mean()
    vol_ratio_series = (df["volume"] / vol_ma.replace(0.0, np.nan)).fillna(0.0)
    vol_ratio_now = float(vol_ratio_series.iloc[-1])
    vol_ratio_prev = float(vol_ratio_series.iloc[-2]) if len(vol_ratio_series) > 1 else vol_ratio_now
    vol_chg_1 = pct_change(vol_ratio_now, vol_ratio_prev)
    vol_ratio_w_ago = float(vol_ratio_series.iloc[-1 - w]) if len(vol_ratio_series) > w else vol_ratio_prev
    vol_chg_w = pct_change(vol_ratio_now, vol_ratio_w_ago)
    streak = rising_streak(vol_ratio_series.tail(w + 1))

    # --- taker buy ratio (funding proxy) ---
    if "taker_buy_base" in df.columns and df["volume"].iloc[-1] > 0:
        taker_buy_ratio = float(df["taker_buy_base"].iloc[-1] / df["volume"].iloc[-1])
    else:
        taker_buy_ratio = float("nan")

    regime = determine_volume_regime(cfg, price_chg_w, vol_chg_w)

    rsi_now = float(rsi(close, cfg.RSI_PERIOD).iloc[-1])
    atrp = float(atr_percent(df, cfg.ATR_PERIOD).iloc[-1])
    ema20_now = float(ema(close, cfg.EMA_FAST).iloc[-1])
    ema50_now = float(ema(close, cfg.EMA_SLOW).iloc[-1])
    last_wick = wick_ratio(df.iloc[-1])

    # --- 144 periodluk lineer regresyon kanali ---
    ch_mid, ch_upper, ch_lower, ch_pos = linreg_channel(close, cfg.CHANNEL_PERIOD, cfg.CHANNEL_STDEV_MULT)
    ch_closes = close.tail(cfg.CHANNEL_PERIOD).tolist() if len(close) >= cfg.CHANNEL_PERIOD else []

    return MarketSnapshot(
        symbol=symbol, price=price,
        price_chg_1=price_chg_1, price_chg_w=price_chg_w,
        vol_ratio=vol_ratio_now, vol_chg_1=vol_chg_1, vol_chg_w=vol_chg_w,
        vol_rising_streak=streak,
        taker_buy_ratio=taker_buy_ratio,
        regime=regime,
        rsi=rsi_now, atr_pct=atrp,
        ema20=ema20_now, ema50=ema50_now,
        last_wick_ratio=last_wick,
        channel_mid=ch_mid, channel_upper=ch_upper, channel_lower=ch_lower,
        channel_pos=ch_pos, close_series=ch_closes,
    )


# ==============================================================================
# 8) SKORLAMA MOTORU (toplam 100: hacim-rejimi 30 + rejim 20 + taker 20 + hacim 15 + trend 15)
# ==============================================================================
def score_volume_regime(cfg: Config, s: MarketSnapshot) -> tuple[float, list[str]]:
    """OI-degisimi karsiligi: vol_ratio'nun (bar hacmi/ortalama) DEGISIM HIZI."""
    pts = 0.0
    reasons: list[str] = []
    if s.vol_chg_1 >= cfg.VOLUME_SPIKE_STRONG:
        pts += 18
        reasons.append(f"Hacim orani tek barda +{s.vol_chg_1:.0f}% sicradi")
    elif s.vol_chg_1 >= cfg.VOLUME_SPIKE_WATCH:
        pts += 10
        reasons.append(f"Hacim orani tek barda +{s.vol_chg_1:.0f}% artti")
    if s.vol_chg_w >= cfg.VOLUME_WINDOW_STRONG:
        pts += 12
        reasons.append(f"Hacim orani son {cfg.VOLUME_WINDOW} barda +{s.vol_chg_w:.0f}%")
    elif s.vol_chg_w >= cfg.VOLUME_WINDOW_WATCH:
        pts += 6
        reasons.append(f"Hacim orani son {cfg.VOLUME_WINDOW} barda +{s.vol_chg_w:.0f}%")
    if s.vol_rising_streak >= 4:
        pts += 4
        reasons.append(f"Hacim orani {s.vol_rising_streak} bardir kesintisiz yukseliyor")
    return min(pts, 30.0), reasons


def score_regime(s: MarketSnapshot) -> tuple[float, list[str]]:
    pts = 0.0
    reasons: list[str] = []
    if s.regime in (REGIME_ACCUMULATION_BUILDUP, REGIME_DISTRIBUTION_BUILDUP):
        pts = 20.0
        reasons.append(f"Rejim: {REGIME_LABEL_TR[s.regime]}")
    return pts, reasons


def score_taker_imbalance(cfg: Config, s: MarketSnapshot, direction: str) -> tuple[float, list[str]]:
    """Funding karsiligi: taker buy ratio'nun %50'den sapmasi.

    LONG icin: agresif SATIS hacmi baskinken (oran dusuk) fiyat/rejim yine de
    yukari ise -> satislar sessizce yutuluyor demektir (gizli guc, squeeze-
    benzeri yakit). SHORT icin simetrik (agresif ALIM baskinken fiyat asagi).
    """
    pts = 0.0
    reasons: list[str] = []
    r = s.taker_buy_ratio
    if math.isnan(r):
        return 0.0, reasons
    dev = r - 0.5   # pozitif: agresif alim baskin, negatif: agresif satis baskin
    if direction == "LONG":
        if dev <= -cfg.TAKER_RATIO_EXTREME:
            pts = 20.0
            reasons.append(f"Taker satis baskin ({r:.2f}) ama fiyat/rejim yukari -> gizli alim gucu")
        elif dev <= -cfg.TAKER_RATIO_MODERATE:
            pts = 12.0
            reasons.append(f"Taker orani satis agirlikli ({r:.2f})")
        elif abs(dev) < cfg.TAKER_RATIO_MODERATE:
            pts = 6.0
            reasons.append(f"Taker orani notr ({r:.2f})")
    elif direction == "SHORT":
        if dev >= cfg.TAKER_RATIO_EXTREME:
            pts = 20.0
            reasons.append(f"Taker alim baskin ({r:.2f}) ama fiyat/rejim asagi -> gizli satis gucu")
        elif dev >= cfg.TAKER_RATIO_MODERATE:
            pts = 12.0
            reasons.append(f"Taker orani alim agirlikli ({r:.2f})")
        elif abs(dev) < cfg.TAKER_RATIO_MODERATE:
            pts = 6.0
            reasons.append(f"Taker orani notr ({r:.2f})")
    return min(pts, 20.0), reasons


def score_volume(s: MarketSnapshot) -> tuple[float, list[str]]:
    """Candle-seviyesi hacim spike'i (vol_ratio'nun MUTLAK seviyesi -- yukaridaki
    score_volume_regime'in DEGISIM HIZINDAN farkli, orijinal OI-botundaki
    ayrik score_volume ile ayni rolde)."""
    pts = 0.0
    reasons: list[str] = []
    vr = s.vol_ratio
    if vr >= 3.0:
        pts = 15.0
    elif vr >= 2.0:
        pts = 12.0
    elif vr >= 1.5:
        pts = 8.0
    elif vr >= 1.2:
        pts = 4.0
    if vr >= 1.2:
        reasons.append(f"Volume Ratio: {vr:.2f}x")
    return min(pts, 15.0), reasons


def score_trend(s: MarketSnapshot, direction: str) -> tuple[float, list[str]]:
    pts = 0.0
    reasons: list[str] = []
    if direction == "LONG":
        if s.price > s.ema20 > s.ema50:
            pts += 8; reasons.append("Close > EMA20 > EMA50")
        if 55 < s.rsi <= 70:
            pts += 4; reasons.append(f"RSI: {s.rsi:.0f} (saglikli momentum)")
        if s.price_chg_w > 0:
            pts += 3; reasons.append(f"Fiyat son pencerede {s.price_chg_w:+.2f}%")
    elif direction == "SHORT":
        if s.price < s.ema20 < s.ema50:
            pts += 8; reasons.append("Close < EMA20 < EMA50")
        if 30 <= s.rsi < 45:
            pts += 4; reasons.append(f"RSI: {s.rsi:.0f} (saglikli momentum)")
        if s.price_chg_w < 0:
            pts += 3; reasons.append(f"Fiyat son pencerede {s.price_chg_w:+.2f}%")
    return min(pts, 15.0), reasons


def score_risk(cfg: Config, s: MarketSnapshot, direction: str) -> tuple[float, list[str]]:
    penalty = 0.0
    warnings: list[str] = []
    r = s.taker_buy_ratio
    if not math.isnan(r):
        dev = r - 0.5
        # sinyal yonuyle AYNI tarafta asiri taker baskinligi -> zaten kalabalik/
        # kovalanmis hareket (funding-crowding cezasinin karsiligi)
        if direction == "LONG" and dev >= cfg.TAKER_RATIO_EXTREME:
            penalty += 10; warnings.append(f"Taker alim zaten asiri baskin ({r:.2f}) - kovalama riski")
        elif direction == "SHORT" and dev <= -cfg.TAKER_RATIO_EXTREME:
            penalty += 10; warnings.append(f"Taker satis zaten asiri baskin ({r:.2f}) - kovalama riski")
    else:
        penalty += 5; warnings.append("Taker orani olculemedi")
    if s.last_wick_ratio > 0.60:
        penalty += 5; warnings.append(f"Asiri fitil ({s.last_wick_ratio:.2f})")
    if s.vol_ratio < 0.8:
        penalty += 5; warnings.append("Hacim teyidi yok (ortalama alti)")
    if direction == "LONG" and s.rsi > 70:
        extra = 13 if s.rsi > 80 else 8
        penalty += extra; warnings.append(f"RSI asiri alim ({s.rsi:.0f}) - hareket uzamis, pullback riski")
    if direction == "SHORT" and s.rsi < 30:
        extra = 13 if s.rsi < 20 else 8
        penalty += extra; warnings.append(f"RSI asiri satim ({s.rsi:.0f}) - hareket uzamis, pullback riski")
    if s.vol_chg_w > 0 and s.vol_chg_1 < 0:
        penalty += 3; warnings.append("Hacim orani son barda geriliyor")
    # climax/tukenme paterni (OI botundakiyle ayni mantik, hacim uzerinden)
    if s.vol_chg_1 >= cfg.VOLUME_SPIKE_STRONG * 1.5 and s.last_wick_ratio > 0.35:
        penalty += 10
        warnings.append(f"Ani hacim sicramasi (+{s.vol_chg_1:.0f}%) + belirgin fitil "
                       f"({s.last_wick_ratio:.2f}) - climax/tukenme riski")
    if s.atr_pct < cfg.MIN_ATR_PCT:
        penalty += 8; warnings.append(f"Volatilite dusuk (ATR%% {s.atr_pct:.2f} < {cfg.MIN_ATR_PCT:.2f}) - SL/TP mesafesi dar")
    return min(penalty, 30.0), warnings


def determine_direction(s: MarketSnapshot) -> str:
    if s.regime == REGIME_ACCUMULATION_BUILDUP:
        return "LONG"
    if s.regime == REGIME_DISTRIBUTION_BUILDUP:
        return "SHORT"
    return "NEUTRAL"


def compute_sl_tp(cfg: Config, s: MarketSnapshot, direction: str) -> tuple[float, float, float, float]:
    if direction not in ("LONG", "SHORT") or s.atr_pct <= 0 or s.price <= 0:
        return 0.0, 0.0, 0.0, 0.0
    atr_abs = s.price * (s.atr_pct / 100.0)
    risk_abs = cfg.ATR_SL_MULT * atr_abs
    if risk_abs <= 0:
        return 0.0, 0.0, 0.0, 0.0
    if direction == "LONG":
        sl = s.price - risk_abs
        tp1 = s.price + cfg.ATR_TP1_R * risk_abs
        tp2 = s.price + cfg.ATR_TP2_R * risk_abs
    else:
        sl = s.price + risk_abs
        tp1 = s.price - cfg.ATR_TP1_R * risk_abs
        tp2 = s.price - cfg.ATR_TP2_R * risk_abs
    risk_pct = risk_abs / s.price * 100.0
    return sl, tp1, tp2, risk_pct


def evaluate(cfg: Config, s: MarketSnapshot, timeframe: str) -> Signal:
    direction = determine_direction(s)

    vr_s, vr_r = score_volume_regime(cfg, s)
    reg_s, reg_r = score_regime(s)
    tak_s, tak_r = score_taker_imbalance(cfg, s, direction)
    volu_s, volu_r = score_volume(s)
    trend_s, trend_r = score_trend(s, direction)
    risk_p, risk_w = score_risk(cfg, s, direction)

    raw = vr_s + reg_s + tak_s + volu_s + trend_s - risk_p
    score = float(max(0.0, min(100.0, raw)))

    reasons = reg_r + vr_r + tak_r + volu_r + trend_r

    sl, tp1, tp2, risk_pct = compute_sl_tp(cfg, s, direction)

    if direction == "LONG":
        invalidation = f"Hacim rejimi dususu + EMA20 alti {timeframe} kapanis ({fmt_price(s.ema20)})"
    elif direction == "SHORT":
        invalidation = f"Hacim rejimi dususu + EMA20 ustu {timeframe} kapanis ({fmt_price(s.ema20)})"
    else:
        invalidation = "-"

    return Signal(
        symbol=s.symbol, timeframe=timeframe, direction=direction, score=score,
        volume_regime_score=vr_s, regime_score=reg_s, taker_score=tak_s,
        volume_score=volu_s, trend_score=trend_s,
        risk_penalty=risk_p,
        reasons=reasons, warnings=risk_w, price=s.price,
        invalidation=invalidation, snap=s,
        stop_loss=sl, take_profit1=tp1, take_profit2=tp2, risk_pct=risk_pct,
    )


# ==============================================================================
# 8b) 144 PERIODLUK REGRESYON KANALI FILTRESI (main.py ile ayni mantik)
# ==============================================================================
def apply_channel_filter(cfg: Config, sig: Signal) -> None:
    """Fiyat UST banda yakinken LONG, ALT banda yakinken SHORT sinyalini
    yerinde bastirir (sig.direction -> NEUTRAL). Bkz. main.py apply_channel_filter."""
    if not cfg.CHANNEL_FILTER_ENABLED or sig.snap is None or sig.direction == "NEUTRAL":
        return
    pos = sig.snap.channel_pos
    if math.isnan(pos):
        return
    if sig.direction == "LONG" and pos >= (1.0 - cfg.CHANNEL_EDGE_ZONE):
        sig.warnings.append(
            f"144-periyot kanalda UST banda yakin (poz {pos:.2f}) - LONG sinyali bastirildi, "
            f"bu bolgede yalnizca SHORT dikkate alinir"
        )
        sig.direction = "NEUTRAL"
    elif sig.direction == "SHORT" and pos <= cfg.CHANNEL_EDGE_ZONE:
        sig.warnings.append(
            f"144-periyot kanalda ALT banda yakin (poz {pos:.2f}) - SHORT sinyali bastirildi, "
            f"bu bolgede yalnizca LONG dikkate alinir"
        )
        sig.direction = "NEUTRAL"


# ==============================================================================
# 9) ALARM SEVIYESI
# ==============================================================================
def classify_alert(cfg: Config, sig: Signal) -> str:
    s = sig.snap
    if s is None:
        return "NONE"
    if sig.direction == "NEUTRAL":
        return "NONE"
    if s.vol_chg_1 < cfg.VOLUME_SPIKE_WATCH and s.vol_chg_w < cfg.VOLUME_WINDOW_WATCH:
        return "NONE"
    if s.atr_pct < cfg.MIN_ATR_PCT:
        return "NONE"
    if sig.score < cfg.MIN_SCORE_WATCH:
        return "NONE"

    vol_strong = (
        (s.vol_chg_1 >= cfg.VOLUME_SPIKE_STRONG and s.vol_rising_streak >= 2)
        or s.vol_chg_w >= cfg.VOLUME_WINDOW_STRONG
    )
    if (sig.score >= cfg.MIN_SCORE_STRONG
            and vol_strong
            and s.vol_ratio >= cfg.MIN_VOLUME_RATIO_STRONG
            and sig.risk_penalty <= 15):
        return "STRONG"

    if sig.score >= cfg.MIN_SCORE_WATCH:
        return "WATCH"

    return "NONE"


# ==============================================================================
# 9b) COKLU ZAMAN DILIMI CONFLUENCE
# ==============================================================================
_TF_ORDER = ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"]


def _tf_rank(tf: str) -> int:
    try:
        return _TF_ORDER.index(tf)
    except ValueError:
        return -1


def apply_confluence(cfg: Config, signals: list[Signal]) -> None:
    if len(signals) < 2:
        return
    by_tf = {sig.timeframe: sig for sig in signals}
    context_tf = max((sig.timeframe for sig in signals), key=_tf_rank)
    context = by_tf.get(context_tf)
    if context is None or context.direction == "NEUTRAL":
        return
    for sig in signals:
        if sig.timeframe == context_tf or sig.direction == "NEUTRAL":
            continue
        if sig.direction == context.direction:
            adj = cfg.CONFLUENCE_BONUS
            sig.reasons.append(f"{context_tf} rejimiyle uyumlu ({context.direction}) -> confluence bonus")
        else:
            adj = -cfg.CONFLUENCE_PENALTY
            sig.warnings.append(f"{context_tf} rejimiyle celisiyor ({context.direction} vs {sig.direction}) -> confluence cezasi")
        sig.confluence_adj = adj
        sig.score = float(max(0.0, min(100.0, sig.score + adj)))
        sig.tier = classify_alert(cfg, sig)


# ==============================================================================
# 9c) BTC PIYASA FILTRESI
# ==============================================================================
def _ema_bias(s: MarketSnapshot) -> str:
    if s.price > s.ema20 > s.ema50:
        return "UP"
    if s.price < s.ema20 < s.ema50:
        return "DOWN"
    return "FLAT"


def apply_btc_filter(cfg: Config, signals: list[Signal]) -> None:
    if not cfg.BTC_FILTER_ENABLED:
        return
    btc_bias_by_tf: dict[str, str] = {}
    for sig in signals:
        if sig.symbol == "BTC/USDT" and sig.snap is not None:
            btc_bias_by_tf[sig.timeframe] = _ema_bias(sig.snap)
    if not btc_bias_by_tf:
        return
    for sig in signals:
        if sig.symbol == "BTC/USDT" or sig.direction == "NEUTRAL":
            continue
        bias = btc_bias_by_tf.get(sig.timeframe)
        if not bias or bias == "FLAT":
            continue
        adj = 0.0
        if sig.direction == "LONG":
            if bias == "UP":
                adj = cfg.BTC_FILTER_BONUS
                sig.reasons.append(f"BTC {sig.timeframe} yapisal UP trend ile uyumlu")
            elif bias == "DOWN":
                adj = -cfg.BTC_FILTER_PENALTY
                sig.warnings.append(f"BTC {sig.timeframe} DOWN trendde iken LONG sinyali - piyasa geneline aykiri")
        elif sig.direction == "SHORT":
            if bias == "DOWN":
                adj = cfg.BTC_FILTER_BONUS
                sig.reasons.append(f"BTC {sig.timeframe} yapisal DOWN trend ile uyumlu")
            elif bias == "UP":
                adj = -cfg.BTC_FILTER_PENALTY
                sig.warnings.append(f"BTC {sig.timeframe} UP trendde iken SHORT sinyali - piyasa geneline aykiri")
        if adj != 0.0:
            sig.btc_filter_adj = adj
            sig.score = float(max(0.0, min(100.0, sig.score + adj)))
            sig.tier = classify_alert(cfg, sig)


# ==============================================================================
# 10) TELEGRAM
# ==============================================================================
def fmt_price(x: float) -> str:
    if x == 0 or (isinstance(x, float) and math.isnan(x)):
        return "0"
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:.4g}"
    return f"{x:.6g}"


_TV_INTERVAL = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
    "1d": "D", "1w": "W",
}


def tradingview_link(cfg: Config, sig: "Signal") -> str:
    ex = sig.ex_symbol or sig.symbol
    core = ex.split(":")[0]
    pair = core.replace("/", "").upper()
    ex_name = cfg.EXCHANGE_NAME.strip().lower()
    tv_ex = {"binance": "BINANCE", "okx": "OKX", "bybit": "BYBIT", "kucoin": "KUCOIN",
             "gateio": "GATEIO", "mexc": "MEXC", "bitget": "BITGET"}.get(ex_name, ex_name.upper())
    tv_symbol = f"{tv_ex}:{pair}"
    url = f"https://www.tradingview.com/chart/?symbol={tv_symbol}"
    interval = _TV_INTERVAL.get(sig.timeframe, "")
    if interval:
        url += f"&interval={interval}"
    return url


def _regime_short(regime: str) -> str:
    label = REGIME_LABEL_TR.get(regime, regime)
    return label.split(" (")[0]


def generate_channel_chart_png(cfg: Config, sig: Signal) -> Optional[bytes]:
    """main.py ile ayni mantik: son CHANNEL_PERIOD bar uzerinden fiyat +
    lineer regresyon kanali grafigi. matplotlib yoksa/yetersiz veri varsa
    None doner (cagiran taraf sade metne duser)."""
    if plt is None or sig.snap is None:
        return None
    closes = sig.snap.close_series
    period = cfg.CHANNEL_PERIOD
    if len(closes) < period:
        return None
    try:
        y = np.array(closes[-period:], dtype=float)
        x = np.arange(period, dtype=float)
        slope, intercept = np.polyfit(x, y, 1)
        fitted = slope * x + intercept
        resid_std = float(np.std(y - fitted))
        upper = fitted + cfg.CHANNEL_STDEV_MULT * resid_std
        lower = fitted - cfg.CHANNEL_STDEV_MULT * resid_std

        fig, ax = plt.subplots(figsize=(6.4, 3.6), dpi=110)
        ax.plot(x, y, color="#1f77b4", linewidth=1.3, label="Fiyat")
        ax.plot(x, fitted, color="#888888", linewidth=1.0, linestyle="--", label="Orta")
        ax.plot(x, upper, color="#d62728", linewidth=1.0, label="Ust bant")
        ax.plot(x, lower, color="#2ca02c", linewidth=1.0, label="Alt bant")
        ax.fill_between(x, lower, upper, color="#888888", alpha=0.08)
        ax.scatter([x[-1]], [y[-1]], color="#000000", zorder=5, s=28)
        ax.set_title(f"{sig.symbol} | {sig.timeframe} | {period}-periyot kanal", fontsize=10)
        ax.set_xticks([])
        ax.legend(loc="upper left", fontsize=7, frameon=False)
        ax.tick_params(axis="y", labelsize=8)
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()
    except Exception as e:
        log.warning("Kanal grafigi uretilemedi %s [%s]: %s", sig.symbol, sig.timeframe, e)
        return None


# Butun Telegram mesajlarinin basinda ayni sabit baslik (main.py ile ayni).
BOT_HEADER = "OI FUNDRATE KANAL BOT"


def build_message(cfg: Config, sig: Signal) -> str:
    """Sade Telegram mesaji: yon/skor/fiyat/kanal pozisyonu/SL + (varsa) en
    kritik tek uyari. TP ve TradingView linki kaldirildi (kanal grafigi
    zaten ayri foto olarak gidiyor)."""
    s = sig.snap
    tier_emoji = "🚨" if sig.tier == "STRONG" else "👀"

    lines = [
        BOT_HEADER,
        f"{tier_emoji} {sig.tier} | {sig.symbol} | {sig.direction} | {sig.timeframe}",
        f"Skor: {sig.score:.0f}/100 | Fiyat: {fmt_price(s.price)}",
    ]
    if not math.isnan(s.channel_pos):
        lines.append(f"Kanal poz: {s.channel_pos * 100:.0f}% (0=alt bant, 100=ust bant)")
    if sig.stop_loss > 0:
        lines.append(f"SL: {fmt_price(sig.stop_loss)} ({sig.risk_pct:.1f}%)")
    if sig.warnings:
        lines.append(f"⚠️ {sig.warnings[0]}")
    return "\n".join(lines)


def build_exit_message(cfg: Config, symbol: str, timeframe: str, prev_direction: str,
                       current_regime: str, price: float) -> str:
    return (
        f"{BOT_HEADER}\n"
        f"🔔 REJIM DEGISTI | {symbol} | {timeframe}\n"
        f"{prev_direction} artik teyit edilmiyor -> {_regime_short(current_regime)}\n"
        f"Fiyat: {fmt_price(price)}"
    )


def send_telegram(cfg: Config, text: str) -> bool:
    if not cfg.TELEGRAM_ENABLED:
        log.info("[TG kapali] mesaj gonderilmedi")
        return False
    if requests is None:
        log.warning("requests yuklu degil; Telegram atlaniyor")
        return False
    if not cfg.TELEGRAM_BOT_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        log.warning("Telegram token/chat_id eksik")
        return False
    url = f"https://api.telegram.org/bot{cfg.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": cfg.TELEGRAM_CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        }, timeout=15)
        if r.status_code == 200 and r.json().get("ok"):
            return True
        log.warning("Telegram hata: %s %s", r.status_code, r.text[:200])
        return False
    except Exception as e:
        log.warning("Telegram gonderim hatasi: %s", e)
        return False


def send_telegram_photo(cfg: Config, photo_png: bytes, caption: str) -> bool:
    """Kanal grafigini caption'la birlikte Telegram'a foto olarak gonderir
    (main.py ile ayni mantik)."""
    if not cfg.TELEGRAM_ENABLED:
        log.info("[TG kapali] foto gonderilmedi")
        return False
    if requests is None:
        log.warning("requests yuklu degil; Telegram atlaniyor")
        return False
    if not cfg.TELEGRAM_BOT_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        log.warning("Telegram token/chat_id eksik")
        return False
    url = f"https://api.telegram.org/bot{cfg.TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        files = {"photo": ("kanal.png", photo_png, "image/png")}
        data = {"chat_id": cfg.TELEGRAM_CHAT_ID, "caption": caption[:1024]}
        r = requests.post(url, data=data, files=files, timeout=25)
        if r.status_code == 200 and r.json().get("ok"):
            return True
        log.warning("Telegram foto hata: %s %s", r.status_code, r.text[:200])
        return False
    except Exception as e:
        log.warning("Telegram foto gonderim hatasi: %s", e)
        return False


# ==============================================================================
# 11) COOLDOWN / ALARM CACHE
# ==============================================================================
class CooldownManager:
    def __init__(self, cfg: Config, path: str = STORAGE_FILE):
        self.cfg = cfg
        self.path = path
        self.state: dict = self._load()

    def _load(self) -> dict:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning("Cache yazilamadi: %s", e)

    @staticmethod
    def _key(sig: Signal) -> str:
        return f"{sig.symbol}|{sig.timeframe}"

    def should_alert(self, sig: Signal) -> bool:
        key = self._key(sig)
        now = time.time()
        prev = self.state.get(key)
        if prev is None:
            return True
        if prev.get("direction") != sig.direction:
            return True
        elapsed_min = (now - prev.get("ts", 0)) / 60.0
        if elapsed_min >= self.cfg.ALERT_COOLDOWN_MINUTES:
            return True
        if sig.score - prev.get("score", 0) >= self.cfg.SCORE_UPDATE_THRESHOLD:
            return True
        return False

    def record(self, sig: Signal) -> None:
        self.state[self._key(sig)] = {
            "symbol": sig.symbol,
            "timeframe": sig.timeframe,
            "direction": sig.direction,
            "score": sig.score,
            "tier": sig.tier,
            "ts": time.time(),
        }
        self._save()

    def clear(self, key: str) -> None:
        if key in self.state:
            del self.state[key]
            self._save()


# ==============================================================================
# 11a) ALARM GECMISI (son N saat ozet mesaji icin, main.py ile ayni mantik)
# ==============================================================================
class AlertLog:
    def __init__(self, cfg: Config, path: str = ALERT_LOG_FILE, keep_hours: float = 48.0):
        self.cfg = cfg
        self.path = path
        self.keep_hours = keep_hours
        self.entries: list = self._load()

    def _load(self) -> list:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    def _save(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.entries, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning("Alarm gecmisi yazilamadi: %s", e)

    def append(self, sig: Signal) -> None:
        self.entries.append({
            "symbol": sig.symbol, "timeframe": sig.timeframe, "direction": sig.direction,
            "tier": sig.tier, "score": sig.score, "ts": time.time(),
        })
        cutoff = time.time() - self.keep_hours * 3600.0
        self.entries = [e for e in self.entries if e.get("ts", 0) >= cutoff]
        self._save()

    def recent(self, hours: float) -> list:
        cutoff = time.time() - hours * 3600.0
        return [e for e in self.entries if e.get("ts", 0) >= cutoff]


def build_summary_message(entries: list, hours: float) -> str:
    lines = [BOT_HEADER, f"🕐 Son {hours:.0f} saat sinyal ozeti"]
    if not entries:
        lines.append("Bu surede sinyal olusmadi.")
        return "\n".join(lines)
    for e in sorted(entries, key=lambda x: x.get("ts", 0)):
        t = datetime.fromtimestamp(e.get("ts", 0), timezone.utc).strftime("%H:%M")
        tier_emoji = "🚨" if e.get("tier") == "STRONG" else "👀"
        lines.append(
            f"{tier_emoji} {e.get('symbol')} {e.get('direction')} {e.get('timeframe')} "
            f"skor={e.get('score', 0):.0f} | {t} UTC"
        )
    return "\n".join(lines)


# ==============================================================================
# 11b) SANAL PERFORMANS TAKIBI
# ==============================================================================
class PerformanceTracker:
    def __init__(self, cfg: Config, path: str = PERF_FILE):
        self.cfg = cfg
        self.path = path
        self.trades: list[dict] = self._load()

    def _load(self) -> list[dict]:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    def _save(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.trades, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning("Perf kaydi yazilamadi: %s", e)

    def open_trade(self, sig: Signal) -> None:
        if not self.cfg.PERF_TRACKING_ENABLED or sig.stop_loss <= 0:
            return
        already_open = any(
            t for t in self.trades
            if t["status"] == "OPEN" and t["symbol"] == sig.symbol and t["timeframe"] == sig.timeframe
        )
        if already_open:
            return
        self.trades.append({
            "symbol": sig.symbol, "timeframe": sig.timeframe, "direction": sig.direction,
            "tier": sig.tier, "entry": sig.price, "sl": sig.stop_loss,
            "tp1": sig.take_profit1, "tp2": sig.take_profit2,
            "tp1_r": self.cfg.ATR_TP1_R, "tp2_r": self.cfg.ATR_TP2_R,
            "status": "OPEN", "stage": "INIT",
            "opened_ts": time.time(), "closed_ts": None, "result_r": None,
        })
        self._save()

    def update(self, signals: list[Signal]) -> None:
        if not self.cfg.PERF_TRACKING_ENABLED:
            return
        price_map = {(s.symbol, s.timeframe): s.price for s in signals}
        dirty = False
        for t in self.trades:
            if t["status"] != "OPEN":
                continue
            price = price_map.get((t["symbol"], t["timeframe"]))
            if price is None:
                continue
            direction = t["direction"]
            if t["stage"] == "INIT":
                if direction == "LONG":
                    if price <= t["sl"]:
                        t["status"] = "CLOSED"; t["result_r"] = -1.0; t["closed_ts"] = time.time()
                    elif price >= t["tp1"]:
                        t["stage"] = "TP1_HIT"; t["sl"] = t["entry"]
                else:
                    if price >= t["sl"]:
                        t["status"] = "CLOSED"; t["result_r"] = -1.0; t["closed_ts"] = time.time()
                    elif price <= t["tp1"]:
                        t["stage"] = "TP1_HIT"; t["sl"] = t["entry"]
                dirty = True
            elif t["stage"] == "TP1_HIT":
                if direction == "LONG":
                    if price >= t["tp2"]:
                        t["status"] = "CLOSED"; t["result_r"] = (t["tp1_r"] + t["tp2_r"]) / 2.0; t["closed_ts"] = time.time()
                    elif price <= t["sl"]:
                        t["status"] = "CLOSED"; t["result_r"] = t["tp1_r"] / 2.0; t["closed_ts"] = time.time()
                else:
                    if price <= t["tp2"]:
                        t["status"] = "CLOSED"; t["result_r"] = (t["tp1_r"] + t["tp2_r"]) / 2.0; t["closed_ts"] = time.time()
                    elif price >= t["sl"]:
                        t["status"] = "CLOSED"; t["result_r"] = t["tp1_r"] / 2.0; t["closed_ts"] = time.time()
                dirty = True
        if dirty:
            self._save()

    def summary(self) -> str:
        closed = [t for t in self.trades if t["status"] == "CLOSED"]
        open_ = [t for t in self.trades if t["status"] == "OPEN"]
        if not closed:
            return f"Henuz kapanan sanal islem yok. Acik: {len(open_)}"
        wins = [t for t in closed if (t["result_r"] or 0) > 0]
        avg_r = sum((t["result_r"] or 0) for t in closed) / len(closed)
        win_rate = len(wins) / len(closed) * 100.0
        return (f"Kapanan islem: {len(closed)} | Kazanan: {len(wins)} (%{win_rate:.1f}) | "
                f"Ortalama R: {avg_r:+.2f} | Acik pozisyon: {len(open_)}")


# ==============================================================================
# 12) TERMINAL OZET
# ==============================================================================
def print_summary(sig: Signal) -> None:
    s = sig.snap
    taker = f"{s.taker_buy_ratio:.2f}" if s and not math.isnan(s.taker_buy_ratio) else "n/a"
    tag = {"STRONG": "🚨", "WATCH": "👀", "NONE": "  "}.get(sig.tier, "  ")
    log.info("%s %-13s %-4s %-7s skor=%5.1f volreg=%.0f rejim=%.0f taker_s=%.0f volu=%.0f trend=%.0f "
             "conf=%+.0f btc=%+.0f risk=-%.0f | V1=%+.0f%% Vw=%+.0f%% taker=%s VR=%.2f [%s] SL=%s TP1=%s",
             tag, sig.symbol, sig.timeframe, sig.direction, sig.score,
             sig.volume_regime_score, sig.regime_score, sig.taker_score,
             sig.volume_score, sig.trend_score,
             sig.confluence_adj, sig.btc_filter_adj, sig.risk_penalty,
             s.vol_chg_1 if s else 0.0, s.vol_chg_w if s else 0.0, taker,
             s.vol_ratio if s else 0.0, s.regime if s else "-",
             fmt_price(sig.stop_loss) if sig.stop_loss else "-",
             fmt_price(sig.take_profit1) if sig.take_profit1 else "-")


# ==============================================================================
# 13) TEK COIN ISLEME (async, coklu zaman dilimi paralel)
# ==============================================================================
async def process_symbol(cfg: Config, client: ExchangeClient, symbol: str,
                         sem: asyncio.Semaphore) -> list[Signal]:
    async with sem:
        ex_symbol = client.resolve_symbol(symbol)
        if ex_symbol is None:
            log.debug("Borsa'da yok/pasif: %s (atlandi)", symbol)
            return []

        tasks = [client.fetch_ohlcv_with_taker(ex_symbol, tf, cfg.OHLCV_LIMIT) for tf in cfg.TIMEFRAMES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        signals: list[Signal] = []
        for tf, df in zip(cfg.TIMEFRAMES, results):
            if isinstance(df, Exception):
                log.warning("OHLCV hata %s [%s]: %s (atlandi)", symbol, tf, df)
                continue
            ok, reason = validate_ohlcv(df)
            if not ok:
                log.warning("OHLCV kalitesi dusuk %s [%s]: %s (atlandi)", symbol, tf, reason)
                continue
            snap = build_snapshot(cfg, symbol, df)
            sig = evaluate(cfg, snap, tf)
            sig.ex_symbol = ex_symbol
            apply_channel_filter(cfg, sig)     # 144-periyot kanal: banda yakinken ters yon bastirilir
            sig.tier = classify_alert(cfg, sig)
            signals.append(sig)

        apply_confluence(cfg, signals)
        return signals


# ==============================================================================
# 14) TARAMA TURU (async)
# ==============================================================================
async def scan_once(cfg: Config, client: ExchangeClient, cooldown: CooldownManager,
                    symbols: list[str], perf: Optional["PerformanceTracker"] = None,
                    alert_log: Optional["AlertLog"] = None) -> None:
    t0 = time.time()
    log.info("=" * 78)
    log.info("Spot taramasi basladi | %s | %d coin | tf=%s | paralellik=%d",
             datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
             len(symbols), ",".join(cfg.TIMEFRAMES), cfg.MAX_CONCURRENCY)
    log.info("=" * 78)

    sem = asyncio.Semaphore(cfg.MAX_CONCURRENCY)
    tasks = [process_symbol(cfg, client, s, sem) for s in symbols]
    per_symbol = await asyncio.gather(*tasks, return_exceptions=True)

    signals: list[Signal] = []
    for symbol, res in zip(symbols, per_symbol):
        if isinstance(res, Exception):
            log.warning("Islem hatasi %s: %s (atlandi)", symbol, res)
            continue
        for sig in res:
            signals.append(sig)

    apply_btc_filter(cfg, signals)

    for sig in sorted(signals, key=lambda x: x.score, reverse=True):
        print_summary(sig)

    sig_by_key = {(s.symbol, s.timeframe): s for s in signals}
    exit_alerts = 0
    for key, prev in list(cooldown.state.items()):
        prev_dir = prev.get("direction")
        if prev_dir not in ("LONG", "SHORT"):
            continue
        symbol, _, timeframe = key.partition("|")
        cur = sig_by_key.get((symbol, timeframe))
        if cur is None:
            continue
        if cur.direction != prev_dir:
            msg = build_exit_message(cfg, symbol, timeframe, prev_dir, cur.snap.regime if cur.snap else "-", cur.price)
            ok = await asyncio.to_thread(send_telegram, cfg, msg)
            if ok:
                log.info("🔔 Exit/gozden-gecir bildirimi: %s [%s] (%s -> %s)",
                         symbol, timeframe, prev_dir, cur.direction)
                exit_alerts += 1
            cooldown.clear(key)

    alerts = 0
    for sig in sorted(signals, key=lambda x: x.score, reverse=True):
        if sig.tier in ("STRONG", "WATCH") and cooldown.should_alert(sig):
            msg = build_message(cfg, sig)
            png = generate_channel_chart_png(cfg, sig)
            if png is not None:
                ok = await asyncio.to_thread(send_telegram_photo, cfg, png, msg)
                if not ok:
                    ok = await asyncio.to_thread(send_telegram, cfg, msg)
            else:
                ok = await asyncio.to_thread(send_telegram, cfg, msg)
            if ok:
                log.info("📨 Telegram gonderildi: %s [%s] (%s)", sig.symbol, sig.timeframe, sig.tier)
                cooldown.record(sig)
                alerts += 1
                if perf is not None:
                    perf.open_trade(sig)
                if alert_log is not None:
                    alert_log.append(sig)

    if perf is not None:
        perf.update(signals)

    if alert_log is not None:
        summary_msg = build_summary_message(alert_log.recent(12), 12)
        ok = await asyncio.to_thread(send_telegram, cfg, summary_msg)
        if ok:
            log.info("🕐 12 saatlik ozet mesaji gonderildi.")

    dt = time.time() - t0
    log.info("-" * 78)
    log.info("Tarama bitti. Sinyal(coin x TF)=%d, alarm=%d, exit=%d, sure=%.1fs",
             len(signals), alerts, exit_alerts, dt)


# ==============================================================================
# 15) SELFTEST (API'siz sahte veri)
# ==============================================================================
def make_fake_df(n: int = 200, trend: float = 0.0, vol: float = 0.01,
                 seed: int = 1, volume_spike: bool = True, taker_bias: float = 0.5) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    price = 100.0
    rows = []
    start = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    for i in range(n):
        ret = trend + rng.normal(0, vol)
        open_ = price
        close = max(0.01, open_ * (1 + ret))
        high = max(open_, close) * (1 + abs(rng.normal(0, vol / 2)))
        low = min(open_, close) * (1 - abs(rng.normal(0, vol / 2)))
        base_vol = 1000 * (1 + abs(rng.normal(0, 0.3)))
        volume = base_vol * (3.2 if (volume_spike and i == n - 1) else 1.0)
        taker_buy_base = volume * min(max(taker_bias + rng.normal(0, 0.03), 0.0), 1.0)
        rows.append([start + i * 3_600_000, open_, high, low, close, volume, taker_buy_base])
        price = close
    return pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume", "taker_buy_base"]).astype(
        {"open": float, "high": float, "low": float, "close": float, "volume": float, "taker_buy_base": float})


def run_selftest(cfg: Config) -> None:
    log.info("SELFTEST: sahte veriyle hacim-rejimi/taker-orani skorlama testi")
    scenarios = [
        # (isim, fiyat trendi, hacim spike, taker_bias, seed)
        ("ACCUM/USDT", 0.006, True, 0.35, 7),     # fiyat UP + hacim UP, taker satis-agirlikli (gizli guc)
        ("DISTRIB/USDT", -0.006, True, 0.65, 13), # fiyat DOWN + hacim UP, taker alim-agirlikli (gizli zayiflik)
        ("WEAKRALLY/USDT", 0.005, False, 0.5, 5), # fiyat UP + hacim DOWN -> alarm yok
        ("FLAT/USDT", 0.0, False, 0.5, 3),        # yatay -> alarm yok
    ]
    printed = False
    for name, trend, spike, taker_bias, seed in scenarios:
        df = make_fake_df(trend=trend, seed=seed, volume_spike=spike, taker_bias=taker_bias)
        snap = build_snapshot(cfg, name, df)
        sig = evaluate(cfg, snap, cfg.TIMEFRAMES[0])
        sig.tier = classify_alert(cfg, sig)
        print_summary(sig)
        if sig.tier != "NONE" and not printed:
            print("\n----- ORNEK TELEGRAM MESAJI -----")
            print(build_message(cfg, sig))
            print("---------------------------------\n")
            printed = True
    log.info("SELFTEST tamam. (Gercek veri icin: python main_spot.py --once)")


# ==============================================================================
# 15b) BACKTEST
# ==============================================================================
def simulate_forward(cfg: Config, sig: Signal, future: pd.DataFrame) -> Optional[float]:
    if sig.stop_loss <= 0 or len(future) == 0:
        return None
    direction = sig.direction
    sl = sig.stop_loss
    tp1 = sig.take_profit1
    tp2 = sig.take_profit2
    stage = "INIT"
    for _, row in future.iterrows():
        hi, lo = float(row["high"]), float(row["low"])
        if direction == "LONG":
            if stage == "INIT":
                if lo <= sl:
                    return -1.0
                if hi >= tp1:
                    stage = "TP1_HIT"; sl = sig.price
            else:
                if hi >= tp2:
                    return (cfg.ATR_TP1_R + cfg.ATR_TP2_R) / 2.0
                if lo <= sl:
                    return cfg.ATR_TP1_R / 2.0
        else:
            if stage == "INIT":
                if hi >= sl:
                    return -1.0
                if lo <= tp1:
                    stage = "TP1_HIT"; sl = sig.price
            else:
                if lo <= tp2:
                    return (cfg.ATR_TP1_R + cfg.ATR_TP2_R) / 2.0
                if hi >= sl:
                    return cfg.ATR_TP1_R / 2.0
    return None


def _build_btc_bias_lookup(btc_df: Optional[pd.DataFrame], cfg: Config) -> tuple[list[int], list[str]]:
    ts_list: list[int] = []
    bias_list: list[str] = []
    if btc_df is None or len(btc_df) < cfg.EMA_SLOW + 2:
        return ts_list, bias_list
    e20 = ema(btc_df["close"], cfg.EMA_FAST)
    e50 = ema(btc_df["close"], cfg.EMA_SLOW)
    for idx in range(len(btc_df)):
        price = float(btc_df["close"].iloc[idx])
        v20, v50 = float(e20.iloc[idx]), float(e50.iloc[idx])
        if price > v20 > v50:
            bias = "UP"
        elif price < v20 < v50:
            bias = "DOWN"
        else:
            bias = "FLAT"
        ts_list.append(int(btc_df["timestamp"].iloc[idx]))
        bias_list.append(bias)
    return ts_list, bias_list


def _btc_bias_at(ts_list: list[int], bias_list: list[str], ts: int) -> str:
    if not ts_list:
        return "FLAT"
    idx = bisect.bisect_right(ts_list, ts) - 1
    if idx < 0:
        return "FLAT"
    return bias_list[idx]


async def run_backtest(cfg: Config, symbols: list[str], timeframe: str,
                       forward_bars: int = 20, ohlcv_limit: int = 500) -> None:
    client = ExchangeClient(cfg)
    all_results: list[dict] = []
    try:
        await client.load_markets()

        btc_ts_list: list[int] = []
        btc_bias_list: list[str] = []
        if cfg.BTC_FILTER_ENABLED:
            btc_ex = client.resolve_symbol("BTC/USDT")
            if btc_ex:
                btc_df = await client.fetch_ohlcv(btc_ex, timeframe, ohlcv_limit)
                btc_ts_list, btc_bias_list = _build_btc_bias_lookup(btc_df, cfg)
                if btc_ts_list:
                    log.info("Backtest: BTC piyasa filtresi verisi hazir (%d bar).", len(btc_ts_list))
                else:
                    log.warning("Backtest: BTC verisi alinamadi, piyasa filtresi devre disi.")

        for symbol in symbols:
            ex_symbol = client.resolve_symbol(symbol)
            if ex_symbol is None:
                continue
            df = await client.fetch_ohlcv_with_taker(ex_symbol, timeframe, ohlcv_limit)
            if df is None:
                continue
            ok, _ = validate_ohlcv(df)
            if not ok:
                continue

            n = len(df)
            start = max(cfg.VOL_MA_PERIOD, cfg.VOLUME_WINDOW + 2, cfg.EMA_SLOW) + 1
            for i in range(start, n - forward_bars):
                sub_df = df.iloc[: i + 1]
                try:
                    snap = build_snapshot(cfg, symbol, sub_df)
                except Exception:
                    continue
                sig = evaluate(cfg, snap, timeframe)
                sig.tier = classify_alert(cfg, sig)
                if sig.tier == "NONE" or sig.direction == "NEUTRAL":
                    continue

                if symbol != "BTC/USDT" and btc_ts_list:
                    ts = int(sub_df["timestamp"].iloc[-1])
                    bias = _btc_bias_at(btc_ts_list, btc_bias_list, ts)
                    adj = 0.0
                    if bias != "FLAT":
                        if sig.direction == "LONG":
                            adj = cfg.BTC_FILTER_BONUS if bias == "UP" else -cfg.BTC_FILTER_PENALTY
                        elif sig.direction == "SHORT":
                            adj = cfg.BTC_FILTER_BONUS if bias == "DOWN" else -cfg.BTC_FILTER_PENALTY
                    if adj != 0.0:
                        sig.btc_filter_adj = adj
                        sig.score = float(max(0.0, min(100.0, sig.score + adj)))
                        sig.tier = classify_alert(cfg, sig)
                        if sig.tier == "NONE":
                            continue

                future = df.iloc[i + 1: i + 1 + forward_bars]
                r = simulate_forward(cfg, sig, future)
                if r is None:
                    continue
                all_results.append({"symbol": symbol, "tier": sig.tier, "direction": sig.direction,
                                    "score": sig.score, "r": r})
            log.info("Backtest: %s islendi (%d bar tarandi)", symbol, max(0, n - forward_bars - start))
    finally:
        await client.close()

    if not all_results:
        log.warning("Backtest: hic sonuc uretilmedi (veri yetersiz olabilir).")
        return

    bt_df = pd.DataFrame(all_results)
    log.info("=" * 78)
    log.info("BACKTEST SONUCU | TF=%s | forward_bars=%d | toplam sinyal=%d", timeframe, forward_bars, len(bt_df))
    log.info("=" * 78)
    for label, grp in [("TUMU", bt_df), ("STRONG", bt_df[bt_df.tier == "STRONG"]), ("WATCH", bt_df[bt_df.tier == "WATCH"])]:
        if len(grp) == 0:
            log.info("%-8s: sinyal yok", label)
            continue
        win_rate = (grp["r"] > 0).mean() * 100.0
        avg_r = grp["r"].mean()
        log.info("%-8s: n=%-4d win_rate=%%%-5.1f avg_R=%+.2f (beklenti/islem)", label, len(grp), win_rate, avg_r)
    for direction in ("LONG", "SHORT"):
        grp = bt_df[bt_df.direction == direction]
        if len(grp) == 0:
            continue
        win_rate = (grp["r"] > 0).mean() * 100.0
        log.info("Yon=%-5s: n=%-4d win_rate=%%%-5.1f avg_R=%+.2f", direction, len(grp), win_rate, grp["r"].mean())
    log.info("-" * 78)


# ==============================================================================
# 16) ASYNC RUNNER
# ==============================================================================
def seconds_to_next_tick(interval: int) -> float:
    now = time.time()
    wait = interval - (now % interval)
    if wait < 1.0:
        wait += interval
    return wait


async def ensure_markets(client: ExchangeClient) -> None:
    if not client.markets:
        await client.load_markets()


async def run_check(cfg: Config) -> None:
    client = ExchangeClient(cfg)
    try:
        log.info("BAGLANTI TESTI | borsa=%s | spot", client.name)
        await client.load_markets()
        for s in SYMBOLS[:3]:
            ex_s = client.resolve_symbol(s)
            if not ex_s:
                log.warning("  %-10s -> borsada bulunamadi (atlanir)", s)
                continue
            df = await client.fetch_ohlcv_with_taker(ex_s, cfg.TIMEFRAMES[0], 10)
            if df is not None and len(df) > 0:
                has_taker = "taker_buy_base" in df.columns
                log.info("  %-10s (%s) OHLCV OK | son kapanis=%s | taker_verisi=%s",
                         s, ex_s, fmt_price(float(df['close'].iloc[-1])), has_taker)
            else:
                log.warning("  %-10s (%s) OHLCV BOS", s, ex_s)
        log.info("BAGLANTI TESTI BASARILI ✅")
    except Exception as e:
        log.error("BAGLANTI TESTI BASARISIZ [%s]: %s", type(e).__name__, e)
    finally:
        await client.close()


async def run_scanner(cfg: Config, once: bool) -> None:
    client = ExchangeClient(cfg)
    try:
        cooldown = CooldownManager(cfg)
        perf = PerformanceTracker(cfg)
        alert_log = AlertLog(cfg)

        if once:
            await ensure_markets(client)
            await scan_once(cfg, client, cooldown, SYMBOLS, perf, alert_log)
            log.info("Sanal performans ozeti: %s", perf.summary())
            return

        interval = cfg.SCAN_INTERVAL_SECONDS
        log.info("Surekli tarama | her %ds (epoch'a hizali) | tf=%s | paralellik=%d | Ctrl+C ile durdur",
                 interval, ",".join(cfg.TIMEFRAMES), cfg.MAX_CONCURRENCY)
        while True:
            try:
                await ensure_markets(client)
                await scan_once(cfg, client, cooldown, SYMBOLS, perf, alert_log)
                log.info("Sanal performans ozeti: %s", perf.summary())
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error("Tarama hatasi [%s]: %s | sonraki tik'te tekrar denenecek",
                          type(e).__name__, e)
            wait = seconds_to_next_tick(interval)
            next_utc = datetime.fromtimestamp(time.time() + wait, timezone.utc).strftime("%H:%M:%S")
            log.info("Sonraki tarama ~%.0f sn sonra (%s UTC)", wait, next_utc)
            await asyncio.sleep(wait)
    finally:
        await client.close()


# ==============================================================================
# 17) MAIN
# ==============================================================================
def _run(coro) -> None:
    if sys.platform == "win32":
        loop = asyncio.SelectorEventLoop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(coro)
        finally:
            loop.close()
    else:
        asyncio.run(coro)


def main() -> None:
    parser = argparse.ArgumentParser(description="Crypto Spot Positioning-Proxy Scanner (async)")
    parser.add_argument("--once", action="store_true", help="Tek tur tarama yap")
    parser.add_argument("--check", action="store_true", help="Hizli baglanti/veri testi")
    parser.add_argument("--selftest", action="store_true", help="API'siz sahte veri testi")
    parser.add_argument("--tg-test", action="store_true", help="Telegram baglanti testi")
    parser.add_argument("--backtest", action="store_true", help="Gecmis veriyle geriye donuk sinyal testi")
    parser.add_argument("--bt-tf", type=str, default=None, help="Backtest zaman dilimi")
    parser.add_argument("--bt-forward", type=int, default=20, help="Backtest: ileri kac bar sonucu kontrol edilsin")
    parser.add_argument("--bt-symbols", type=int, default=0, help="Backtest: sadece ilk N sembolu tara (0=hepsi)")
    parser.add_argument("--perf", action="store_true", help="Sanal performans takibi ozetini goster ve cik")
    args = parser.parse_args()

    cfg = Config()

    if _ENV_LOADED_FROM:
        log.info("Env dosyasi yuklendi: %s | anahtarlar: %s",
                 _ENV_LOADED_FROM, ",".join(_ENV_KEYS_LOADED))
    else:
        log.warning("Hicbir .env dosyasi bulunamadi/okunamadi.")
    if cfg.TELEGRAM_BOT_TOKEN and cfg.TELEGRAM_CHAT_ID:
        log.info("Telegram ayarlari OK (token: %d karakter, chat_id: %s...)",
                 len(cfg.TELEGRAM_BOT_TOKEN), cfg.TELEGRAM_CHAT_ID[:4])
    else:
        log.warning("Telegram token/chat_id BULUNAMADI!")

    if args.selftest:
        run_selftest(cfg)
        return

    if args.check:
        _run(run_check(cfg))
        return

    if args.tg_test:
        ok = send_telegram(cfg, "✅ Spot Scanner Telegram testi.\n" + cfg.DISCLAIMER)
        log.info("Telegram testi: %s", "BASARILI" if ok else "BASARISIZ")
        return

    if args.perf:
        perf = PerformanceTracker(cfg)
        log.info("Sanal Performans Ozeti: %s", perf.summary())
        closed = [t for t in perf.trades if t["status"] == "CLOSED"]
        for t in closed[-10:]:
            log.info("  %-13s %-4s %-5s R=%+.2f entry=%s",
                     t["symbol"], t["timeframe"], t["direction"], t["result_r"] or 0.0, fmt_price(t["entry"]))
        return

    if args.backtest:
        bt_symbols = SYMBOLS[: args.bt_symbols] if args.bt_symbols > 0 else SYMBOLS
        bt_tf = args.bt_tf or cfg.TIMEFRAMES[0]
        _run(run_backtest(cfg, bt_symbols, bt_tf, forward_bars=args.bt_forward))
        return

    try:
        _run(run_scanner(cfg, once=args.once))
    except KeyboardInterrupt:
        log.info("Durduruldu (kullanici).")


if __name__ == "__main__":
    main()
