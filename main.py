#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
  CRYPTO OPEN INTEREST + FUNDING RATE SCANNER  (tek dosya / single file MVP)
================================================================================

Binance USDT-M FUTURES uzerinde Open Interest (OI) degisimi + Funding Rate
asiriliklarini tarayan; fiyat-OI rejimi (long buildup / short buildup /
short covering / long unwind) uretip 0-100 arasi "Positioning Score" hesaplayan
ve kriterlere uyan coinler icin Telegram'a alarm gonderen bir tarama motoru.

  !!! Bu sistem YATIRIM TAVSIYESI URETMEZ. !!!
  Sadece turev piyasasi pozisyonlanma verisine dayali "izleme alarmi" gonderir.

Mantik ozeti:
  - Fiyat YUKARI + OI YUKARI  -> LONG BUILDUP  (yeni longlar, trend guclu)  -> LONG
  - Fiyat ASAGI  + OI YUKARI  -> SHORT BUILDUP (yeni shortlar)              -> SHORT
  - Fiyat YUKARI + OI ASAGI   -> SHORT COVERING (zayif ralli, alarm yok)
  - Fiyat ASAGI  + OI ASAGI   -> LONG UNWIND    (pozisyon kapama, alarm yok)
  - Funding asiri NEGATIF + yon LONG  -> short squeeze yakiti (skor artar)
  - Funding asiri POZITIF + yon SHORT -> long squeeze yakiti  (skor artar)
  - Funding, sinyal YONUYLE AYNI tarafta asiri kalabaliksa -> risk cezasi

Coklu zaman dilimi: her coin, TIMEFRAMES listesindeki HER zaman diliminde
(varsayilan 1h, 4h) ayri ayri taranir ve her biri icin bagimsiz skor,
yon ve alarm uretilir. (Binance OI gecmisi periyotlari: 5m, 15m, 30m, 1h,
2h, 4h, 6h, 12h, 1d — TIMEFRAMES bunlardan secilmelidir.)

ASENKRON: veri cekimi ccxt.async_support + asyncio ile PARALEL yapilir.
  - Bir coinin funding + tum TF mumlari + tum TF OI gecmisi es zamanli cekilir.
  - Coinler, MAX_CONCURRENCY semaphore'u ile sinirli paralellikte taranir.
  - Bu sayede 50-100 coin taramasi saniyeler mertebesine iner.

SWING REVIZYONU (v2) - eklenen ozellikler:
  - ATR bazli SL / iki kademeli TP (TP1'de yarim kapat + breakeven, TP2'de tam kapat)
  - 1h/4h coklu TF confluence: kucuk TF sinyali buyuk TF rejimiyle uyumluysa bonus,
    celisirse ceza
  - BTC piyasa filtresi: BTC'nin yapisal (EMA) trendine aykiri altcoin sinyalleri
    cezalandirilir
  - Rejim degisimi / "pozisyonu gozden gecir" exit bildirimi (once LONG/SHORT
    alarmi verilmis coin artik teyit edilmiyorsa)
  - Likidasyon verisi (Binance forceOrder websocket, best-effort; internet/erisim
    yoksa sessizce devre disi kalir, skor katkisi 0 olur)
  - Sanal performans takibi: her alarm icin SL/TP'ye gore otomatik sonuc kaydi
    (--perf ile ozet)
  - Backtest motoru: gecmis OI+fiyat verisiyle CANLI KODLA AYNI mantik uzerinden
    geriye donuk hit-rate/ortalama R testi (--backtest)
  - Volatilite filtresi: ATR% cok dusukse (piyasa hareketsiz) sinyal bastirilir

SWING REVIZYONU (v3) - RSI asiri bolge duzeltmesi:
  - score_trend: RSI momentum bonusu artik SADECE 'saglikli' bantta verilir
    (LONG: 55-70, SHORT: 30-45). Ust/alt sinir disinda (asiri alim/satim)
    bonus verilmez -> hareket zaten uzamis demek, teyit degil.
  - score_risk: asiri alim/satim ceza esigi 78/22'den 70/30'a cekildi ve
    ceza kademeli artirildi (70-80 arasi -8, 80+ icin -13). Backtest'te
    (5 sembol, 1h) LONG sinyallerinin sistemik olarak SL'e gitmesi ("tepede
    kovalama") bu degisiklikle hedeflendi.

Kullanim:
    1) pip install -r requirements.txt
    2) (opsiyonel) .env dosyasi olustur; Telegram anahtarlarini gir.
    3) python main.py                 -> surekli tarama dongusu (async)
       python main.py --once          -> tek tur tarama (test icin)
       python main.py --check         -> hizli baglanti/veri testi
       python main.py --selftest      -> API'siz, sahte veriyle fonksiyon testi
       python main.py --tg-test       -> Telegram baglantisini test et
       python main.py --backtest      -> gecmis veriyle geriye donuk sinyal testi
                                          (--bt-tf 4h --bt-forward 20 --bt-symbols 5)
       python main.py --perf          -> sanal performans ozetini goster

Bagimliliklar (requirements.txt):
    ccxt, pandas, numpy, requests, python-dotenv, aiohttp, certifi
--------------------------------------------------------------------------------
"""

from __future__ import annotations

import os
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

# --- opsiyonel bagimliliklar (selftest anahtarsiz calissin diye korumali) ----
try:
    import ccxt.async_support as ccxt_async  # type: ignore
except Exception:  # pragma: no cover
    ccxt_async = None

# aiohttp + certifi: Windows'ta ccxt/aiohttp SSL sertifikasi bulamayabilir
# (CERTIFICATE_VERIFY_FAILED). certifi tabanli SSL context enjekte edecegiz.
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

# ------------------------------------------------------------------------------
# .ENV YUKLEYICI (python-dotenv GEREKTIRMEZ; dahili, hataya dayanikli)
# Sirasiyla su dosyalari arar: main.py'nin klasorunde ve calisma dizininde
# '.env', '.env.txt', 'env.txt'. Windows Not Defteri'nin ekledigi BOM'u,
# UTF-16 kaydedilmis dosyalari, tirnaklari ve 'KEY = deger' bosluklarini tolere eder.
# ------------------------------------------------------------------------------
_ENV_LOADED_FROM: str = ""       # teshis icin: hangi dosya yuklendi
_ENV_KEYS_LOADED: list = []      # teshis icin: hangi anahtarlar bulundu


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
            # encoding tespiti: UTF-16 BOM (Not Defteri 'Unicode') / UTF-8 BOM / duz
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


# ==============================================================================
# >>> GARANTI COZUM: .env hic okunamazsa Telegram bilgilerini BURAYA yapistir <<<
# Ornek:
#   TELEGRAM_BOT_TOKEN_SABIT = "123456789:AAHfj...tam_token"
#   TELEGRAM_CHAT_ID_SABIT   = "-1001234567890"
# Bu alanlar dolu ise .env'e gerek kalmaz (oncelik .env'dedir, bos kalirsa
# buradakiler kullanilir).
# ==============================================================================
TELEGRAM_BOT_TOKEN_SABIT = ""
TELEGRAM_CHAT_ID_SABIT = ""


class Config:
    # --- Borsa ---
    # OI ve funding SADECE vadeli (futures) piyasada vardir. Binance secilirse
    # otomatik olarak 'binanceusdm' (fapi.binance.com, USDT-M) kullanilir;
    # cogu bolgede geo-engelli olan api.binance.com'a (spot) HIC gidilmez.
    EXCHANGE_NAME: str = _env("EXCHANGE_NAME", "binance")
    MARKET_TYPE: str = "future"   # bu bot dogasi geregi hep futures

    # --- Zaman dilimleri ---
    # Her coin bu zaman dilimlerinin HER BIRINDE ayri ayri taranir.
    # Binance OI gecmisi yalnizca su periyotlari destekler:
    #   5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
    TIMEFRAMES: list[str] = [t.strip() for t in _env("TIMEFRAMES", "1h,4h").split(",") if t.strip()]
    OHLCV_LIMIT: int = int(_env("OHLCV_LIMIT", "200"))
    OI_HIST_LIMIT: int = int(_env("OI_HIST_LIMIT", "48"))     # kac OI barı cekilsin
    OI_WINDOW: int = int(_env("OI_WINDOW", "6"))              # pencere OI/fiyat degisimi (bar)

    # --- Async paralellik ---
    # Ayni anda kac coinin verisi cekilsin (semaphore). Rate-limit'e dikkat;
    # 8-15 arasi Binance icin guvenli baslangic.
    MAX_CONCURRENCY: int = int(_env("MAX_CONCURRENCY", "10"))

    # --- Ag / baglanti ---
    REQUEST_TIMEOUT_MS: int = int(_env("REQUEST_TIMEOUT_MS", "20000"))   # tek istek zaman asimi
    # SSL dogrulamasi: True (guvenli, certifi CA paketi kullanilir). Son care olarak
    # SSL_VERIFY=false yaparak dogrulamayi kapatabilirsin (onerilmez, sadece test).
    SSL_VERIFY: bool = _env("SSL_VERIFY", "true").lower() == "true"

    # --- Dongu ---
    # Sabit kadans: epoch'a hizali (:00, :15, :30, :45). Internet kesintisi olsa
    # bile bir sonraki tik'te otomatik tekrar denenir. 1h/4h veri icin 15 dk yeterli.
    SCAN_INTERVAL_SECONDS: int = int(_env("SCAN_INTERVAL_SECONDS", "900"))

    # --- OI degisim esikleri (yuzde) ---
    OI_SPIKE_STRONG: float = float(_env("OI_SPIKE_STRONG", "3.0"))    # tek mumda %
    OI_SPIKE_WATCH: float = float(_env("OI_SPIKE_WATCH", "1.5"))
    OI_WINDOW_STRONG: float = float(_env("OI_WINDOW_STRONG", "8.0"))  # OI_WINDOW mumda %
    OI_WINDOW_WATCH: float = float(_env("OI_WINDOW_WATCH", "4.0"))
    # rejim tespiti icin "yatay" kabul esikleri
    PRICE_FLAT_TH: float = float(_env("PRICE_FLAT_TH", "0.20"))       # fiyat degisimi %
    OI_FLAT_TH: float = float(_env("OI_FLAT_TH", "0.50"))             # OI degisimi %

    # --- Funding esikleri (%, 8 saatlik periyot basina) ---
    # Binance tipik funding ~0.01%. 0.05%+ asiri kalabalik kabul edilir.
    FUNDING_EXTREME: float = float(_env("FUNDING_EXTREME", "0.05"))
    FUNDING_MODERATE: float = float(_env("FUNDING_MODERATE", "0.02"))

    # --- Skor esikleri ---
    MIN_SCORE_STRONG: float = float(_env("MIN_SCORE_STRONG", "75"))
    MIN_SCORE_WATCH: float = float(_env("MIN_SCORE_WATCH", "60"))
    MIN_VOLUME_RATIO_STRONG: float = float(_env("MIN_VOLUME_RATIO_STRONG", "1.2"))

    # --- Risk yonetimi: ATR bazli SL/TP (swing revizyonu) ---
    # SL: entry'den ATR_SL_MULT x ATR kadar uzakta. TP1/TP2: risk'in (SL mesafesi)
    # ATR_TP1_R / ATR_TP2_R katlari kadar uzakta. TP1'de yarim pozisyon kapatilip
    # stop breakeven'e cekilir varsayimi ile performans takibi/backtest yapilir.
    ATR_SL_MULT: float = float(_env("ATR_SL_MULT", "1.5"))
    ATR_TP1_R: float = float(_env("ATR_TP1_R", "2.0"))
    ATR_TP2_R: float = float(_env("ATR_TP2_R", "3.0"))

    # --- Volatilite filtresi ---
    # ATR% bu esigin altindaysa (piyasa fiilen hareketsiz) sinyal bastirilir;
    # hareketsiz piyasada SL/TP mesafeleri de anlamsizlasir.
    MIN_ATR_PCT: float = float(_env("MIN_ATR_PCT", "0.15"))

    # --- Coklu zaman dilimi confluence (1h/4h uyum kontrolu) ---
    # En buyuk TIMEFRAME "baglam/rejim" TF'i olarak kullanilir; digerlerinin
    # yonu onunla ayni ise bonus, ters ise ceza uygulanir.
    CONFLUENCE_BONUS: float = float(_env("CONFLUENCE_BONUS", "8"))
    CONFLUENCE_PENALTY: float = float(_env("CONFLUENCE_PENALTY", "12"))

    # --- BTC piyasa filtresi ---
    # BTC/USDT'nin EMA20/EMA50 egilimine gore diger coinlerin sinyaline
    # bonus/ceza uygulanir (ayni TF'de).
    BTC_FILTER_ENABLED: bool = _env("BTC_FILTER_ENABLED", "true").lower() == "true"
    BTC_FILTER_BONUS: float = float(_env("BTC_FILTER_BONUS", "5"))
    BTC_FILTER_PENALTY: float = float(_env("BTC_FILTER_PENALTY", "8"))

    # --- Likidasyon verisi (Binance forceOrder websocket, best-effort) ---
    LIQUIDATION_ENABLED: bool = _env("LIQUIDATION_ENABLED", "true").lower() == "true"
    LIQ_WINDOW_SECONDS: int = int(_env("LIQ_WINDOW_SECONDS", "1800"))   # son 30 dk
    LIQ_MIN_NOTIONAL_BONUS: float = float(_env("LIQ_MIN_NOTIONAL_BONUS", "50000"))  # USDT
    LIQ_SCORE_MAX: float = float(_env("LIQ_SCORE_MAX", "10"))

    # --- Sanal performans takibi ---
    PERF_TRACKING_ENABLED: bool = _env("PERF_TRACKING_ENABLED", "true").lower() == "true"

    # --- Alarm tekrari onleme ---
    # 1h/4h sinyalleri icin 90 dk makul; ayni rejim tekrar tekrar mesaj atmasin.
    ALERT_COOLDOWN_MINUTES: int = int(_env("ALERT_COOLDOWN_MINUTES", "90"))
    SCORE_UPDATE_THRESHOLD: float = float(_env("SCORE_UPDATE_THRESHOLD", "10"))

    # --- Telegram ---
    # Oncelik: .env / ortam degiskeni -> bos ise dosya basindaki SABIT degerler
    TELEGRAM_BOT_TOKEN: str = _env("TELEGRAM_BOT_TOKEN", "") or TELEGRAM_BOT_TOKEN_SABIT
    TELEGRAM_CHAT_ID: str = _env("TELEGRAM_CHAT_ID", "") or TELEGRAM_CHAT_ID_SABIT
    TELEGRAM_ENABLED: bool = _env("TELEGRAM_ENABLED", "true").lower() == "true"

    # --- Binance API (opsiyonel; public veriler icin gerekmez) ---
    BINANCE_API_KEY: str = _env("BINANCE_API_KEY", "")
    BINANCE_API_SECRET: str = _env("BINANCE_API_SECRET", "")

    # --- Indikator parametreleri (teyit amacli) ---
    RSI_PERIOD: int = 14
    ATR_PERIOD: int = 14
    EMA_FAST: int = 20
    EMA_SLOW: int = 50
    VOL_MA_PERIOD: int = 20

    # --- Disclaimer ---
    DISCLAIMER: str = ("Bu mesaj yatirim tavsiyesi degildir. "
                       "Sadece OI/funding pozisyonlanma verisine dayali tarama alarmidir.")


# Binance OI gecmisinin destekledigi periyotlar
OI_SUPPORTED_TIMEFRAMES = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}


# --- Taranacak coin listesi (kolayca degistirilebilir) -------------------------
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

STORAGE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "oi_funding_alerts_cache.json")
PERF_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "virtual_trades.json")


# ==============================================================================
# 2) LOGLAMA
# ==============================================================================
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("oi_funding_scanner")
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
# 3) INDIKATORLER  (pandas/numpy ile elle; ekstra bagimlilik yok)
# ==============================================================================
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def rma(series: pd.Series, period: int) -> pd.Series:
    """Wilder's smoothing (RSI/ATR icin)."""
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
# Fiyat-OI rejimleri
REGIME_LONG_BUILDUP = "LONG_BUILDUP"      # fiyat↑ + OI↑ : yeni longlar
REGIME_SHORT_BUILDUP = "SHORT_BUILDUP"    # fiyat↓ + OI↑ : yeni shortlar
REGIME_SHORT_COVERING = "SHORT_COVERING"  # fiyat↑ + OI↓ : short kapanisi (zayif ralli)
REGIME_LONG_UNWIND = "LONG_UNWIND"        # fiyat↓ + OI↓ : long kapanisi
REGIME_NEUTRAL = "NEUTRAL"

REGIME_LABEL_TR = {
    REGIME_LONG_BUILDUP: "LONG BUILDUP (fiyat↑ + OI↑, yeni longlar)",
    REGIME_SHORT_BUILDUP: "SHORT BUILDUP (fiyat↓ + OI↑, yeni shortlar)",
    REGIME_SHORT_COVERING: "SHORT COVERING (fiyat↑ + OI↓, zayif ralli)",
    REGIME_LONG_UNWIND: "LONG UNWIND (fiyat↓ + OI↓, pozisyon kapama)",
    REGIME_NEUTRAL: "NOTR (belirgin pozisyonlanma yok)",
}


@dataclass
class PositioningSnapshot:
    """Bir coin/zaman-dilimi icin tum ham + turetilmis OI/funding verileri."""
    symbol: str
    price: float
    # fiyat degisimi
    price_chg_1: float          # son 1 mum %
    price_chg_w: float          # son OI_WINDOW mum %
    # open interest
    oi_value: float             # guncel OI (USDT, notional)
    oi_chg_1: float             # son 1 OI barı %
    oi_chg_w: float             # son OI_WINDOW barı %
    oi_rising_streak: int       # ardisik kac bar OI yukseliyor
    # funding
    funding_pct: float          # guncel funding (%, 8 saatlik)
    next_funding: str           # bir sonraki funding zamani (UTC, bilgi amacli)
    # rejim
    regime: str
    # teyit indikatorleri (OHLCV'den)
    volume_ratio: float
    rsi: float
    atr_pct: float
    ema20: float
    ema50: float
    last_wick_ratio: float
    # likidasyon (best-effort, websocket verisi yoksa 0)
    long_liq_usdt: float = 0.0
    short_liq_usdt: float = 0.0


@dataclass
class Signal:
    symbol: str
    timeframe: str          # 1h | 4h ...
    direction: str          # LONG | SHORT | NEUTRAL
    score: float
    oi_score: float
    regime_score: float
    funding_score: float
    volume_score: float
    trend_score: float
    liquidation_score: float = 0.0
    confluence_adj: float = 0.0
    btc_filter_adj: float = 0.0
    risk_penalty: float = 0.0
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    price: float = 0.0
    invalidation: str = ""
    ex_symbol: str = ""     # borsadaki gercek parite (TradingView linki icin)
    tier: str = "NONE"      # STRONG | WATCH | NONE
    snap: Optional[PositioningSnapshot] = None
    # risk yonetimi: ATR bazli SL / iki kademeli TP
    stop_loss: float = 0.0
    take_profit1: float = 0.0
    take_profit2: float = 0.0
    risk_pct: float = 0.0   # SL mesafesi, fiyatin yuzdesi olarak


# ==============================================================================
# 5) BORSA ISTEMCISI & VERI CEKME  (ASENKRON)
# ==============================================================================
class ExchangeClient:
    def __init__(self, cfg: Config):
        if ccxt_async is None:
            raise RuntimeError("ccxt yuklu degil. 'pip install ccxt' calistirin.")
        self.cfg = cfg
        # OI/funding icin USDT-M futures gerekir. binance -> 'binanceusdm':
        # bu sinif YALNIZCA fapi.binance.com uc noktalarini kullanir ve cogu
        # bolgede geo-engelli olan api.binance.com'a (spot) HIC gitmez.
        name = cfg.EXCHANGE_NAME.strip().lower()   # ccxt sinif adlari KUCUK harf
        self.market_type = "future"
        if name == "binance":
            name = "binanceusdm"
        self.name = name

        options: dict[str, Any] = {"adjustForTimeDifference": True}

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

        # SSL context: certifi CA paketiyle (Windows'ta aiohttp'nin sertifika
        # bulamama sorununu -CERTIFICATE_VERIFY_FAILED- cozer).
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
        """ccxt'nin kullanacagi aiohttp oturumunu ozel ayarlarla olustur.

        1) ThreadedResolver: aiohttp varsayilan olarak aiodns (c-ares) kullanir;
           bu, Windows'ta sistem DNS sunucularini okuyamayip "Could not contact
           DNS servers" hatasi verir. ThreadedResolver, isletim sisteminin normal
           getaddrinfo'sunu (urllib gibi) kullanir -> DNS sorunu cozulur.
        2) certifi SSL context: Windows'ta sertifika bulamama sorununu onler.
        3) trust_env=True: sistem proxy ayarlarini kullanir (varsa)."""
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
            self.ex.session = self._session   # ccxt bu oturumu kullanir

    async def load_markets(self) -> None:
        await self._prepare_session()
        self.markets = await self.ex.load_markets()
        log.info("Markets yuklendi: %d parite (%s | %s)",
                 len(self.markets), self.name, self.market_type)

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
        """Verilen 'BASE/USDT' sembolunu aktif borsa sembolune cevir; yoksa None.

        Not: Binance Futures'ta bazi meme coinler 1000x katli listelenir
        (PEPE -> 1000PEPE, SHIB -> 1000SHIB, BONK -> 1000BONK, FLOKI -> 1000FLOKI...).
        Bu yuzden hem duz hem '1000' onekli varyantlar denenir.
        """
        base, _, quote = symbol.partition("/")
        bases = [base]
        if not base.startswith("1000"):
            bases.append("1000" + base)          # 1000PEPE, 1000SHIB, ...
        if base.startswith("1000"):
            bases.append(base[4:])               # tersi de denensin

        candidates: list[str] = []
        for b in bases:
            pair = f"{b}/{quote}"
            candidates.append(f"{pair}:{quote}")   # USDT-M perpetual (ccxt formati)
            candidates.append(pair)

        for c in candidates:
            m = self.markets.get(c)
            if m and m.get("active", True):
                return c
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

    async def fetch_oi_history(self, symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
        """Open Interest gecmisi -> DataFrame[timestamp, oi_value(USDT)].

        Binance: /futures/data/openInterestHist (sumOpenInterestValue).
        Not: Binance bu gecmisi yalnizca son ~30 gun icin verir; 1h/4h tarama
        icin fazlasiyla yeterli.
        """
        try:
            raw = await self.ex.fetch_open_interest_history(symbol, timeframe, limit=limit)
            if not raw or len(raw) < 3:
                return None
            rows = []
            for e in raw:
                ts = e.get("timestamp")
                val = e.get("openInterestValue")
                if val is None:
                    info = e.get("info") or {}
                    v = info.get("sumOpenInterestValue")
                    val = float(v) if v is not None else None
                if ts is None or val is None:
                    continue
                rows.append([int(ts), float(val)])
            if len(rows) < 3:
                return None
            df = pd.DataFrame(rows, columns=["timestamp", "oi_value"])
            df = df.sort_values("timestamp").reset_index(drop=True)
            return df
        except Exception as e:
            log.warning("OI gecmisi alinamadi %s %s: %s", symbol, timeframe, e)
            return None

    async def fetch_funding(self, symbol: str) -> Optional[dict]:
        """Guncel funding rate. Doner: {'funding_pct': float, 'next_funding': str}."""
        try:
            r = await self.ex.fetch_funding_rate(symbol)
            rate = r.get("fundingRate")
            if rate is None:
                info = r.get("info") or {}
                lr = info.get("lastFundingRate")
                rate = float(lr) if lr is not None else None
            if rate is None:
                return None
            nxt = r.get("fundingDatetime") or ""
            return {"funding_pct": float(rate) * 100.0, "next_funding": str(nxt)}
        except Exception as e:
            log.warning("Funding alinamadi %s: %s", symbol, e)
            return None


# ==============================================================================
# 5b) LIKIDASYON TAKIPCISI (Binance forceOrder websocket, best-effort)
# ==============================================================================
class LiquidationTracker:
    """Binance USDT-M futures likidasyon (forceOrder) akisini dinler ve sembol
    basina son N saniyedeki likidasyon notional'ini biriktirir.

    NOT: Bu, canli internet erisimi ve Binance websocket'e ulasilabilirlik
    gerektirir (bkz. main.py basindaki geo-engel notlari). Baglanti kurulamazsa
    veya kesilirse sessizce (uyari loglayarak) yeniden dener; bot bu veri
    olmadan da normal calismaya devam eder (skor katkisi sadece 0 olur).
    """

    WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        # sembol (ornek: BTCUSDT) -> [(ts, notional_usdt, side), ...]
        self.events: dict[str, list[tuple[float, float, str]]] = {}
        self._task: Optional[asyncio.Task] = None
        self._session = None
        self._stop = False

    def start(self) -> None:
        if not self.cfg.LIQUIDATION_ENABLED or aiohttp is None:
            return
        self._task = asyncio.ensure_future(self._run())

    async def stop(self) -> None:
        self._stop = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass

    async def _run(self) -> None:
        backoff = 2.0
        while not self._stop:
            try:
                # ExchangeClient._prepare_session ile AYNI sebep: aiohttp'nin
                # varsayilan aiodns/c-ares cozumleyicisi Windows'ta sistem DNS
                # sunucularini okuyamayip "Could not contact DNS servers" hatasi
                # verebiliyor. ThreadedResolver, isletim sisteminin normal
                # getaddrinfo'sunu kullanir -> sorunu cozer. certifi CA paketi
                # de ayni sekilde SSL dogrulama hatalarini onler.
                try:
                    resolver = aiohttp.ThreadedResolver()
                except Exception:
                    resolver = None
                conn_kwargs: dict = {}
                if self.cfg.SSL_VERIFY and certifi is not None:
                    conn_kwargs["ssl"] = ssl.create_default_context(cafile=certifi.where())
                elif not self.cfg.SSL_VERIFY:
                    ctx = ssl.create_default_context()
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                    conn_kwargs["ssl"] = ctx
                if resolver is not None:
                    conn_kwargs["resolver"] = resolver
                connector = aiohttp.TCPConnector(**conn_kwargs)
                self._session = aiohttp.ClientSession(connector=connector, trust_env=True)
                async with self._session.ws_connect(self.WS_URL, heartbeat=30) as ws:
                    log.info("Likidasyon websocket baglandi.")
                    backoff = 2.0
                    async for msg in ws:
                        if self._stop:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._handle(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("Likidasyon websocket hatasi: %s (yeniden denenecek)", e)
            finally:
                try:
                    if self._session is not None:
                        await self._session.close()
                except Exception:
                    pass
            if self._stop:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    def _handle(self, raw: str) -> None:
        try:
            data = json.loads(raw)
            o = data.get("o") or {}
            sym = o.get("s")
            side = o.get("S")             # BUY -> short likide oldu, SELL -> long likide oldu
            qty = float(o.get("q", 0) or 0)
            price = float(o.get("ap", 0) or o.get("p", 0) or 0)
            if not sym or qty <= 0 or price <= 0 or side not in ("BUY", "SELL"):
                return
            notional = qty * price
            bucket = self.events.setdefault(sym, [])
            bucket.append((time.time(), notional, side))
            if len(bucket) > 500:                       # bellek sisirmesin
                del bucket[: len(bucket) - 500]
        except Exception:
            pass

    def recent_liquidation(self, ex_symbol: str, window_sec: int) -> dict:
        """Son window_sec icindeki likidasyon notional toplami (long/short ayri).

        ex_symbol ccxt formatinda olabilir ('BTC/USDT:USDT' veya 'BTC/USDT');
        Binance ham sembolune ('BTCUSDT') cevrilir.
        """
        base_sym = ex_symbol.split(":")[0].replace("/", "").upper()
        now = time.time()
        bucket = self.events.get(base_sym)
        if not bucket:
            return {"long_liq": 0.0, "short_liq": 0.0}
        bucket[:] = [e for e in bucket if now - e[0] <= max(window_sec, 60) * 2]  # gevsek temizlik
        active = [e for e in bucket if now - e[0] <= window_sec]
        long_liq = sum(n for _, n, side in active if side == "SELL")
        short_liq = sum(n for _, n, side in active if side == "BUY")
        return {"long_liq": long_liq, "short_liq": short_liq}


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
    # eksik mum (bosluk) kontrolu
    diffs = df["timestamp"].diff().dropna()
    if len(diffs) > 5:
        common = diffs.mode().iloc[0]
        if (diffs > common * 2.5).sum() > 2:
            return False, "eksik mum (bosluk)"
    return True, "ok"


def validate_oi(cfg: Config, df: Optional[pd.DataFrame]) -> tuple[bool, str]:
    if df is None or len(df) < cfg.OI_WINDOW + 2:
        return False, "yetersiz OI barı"
    if df["oi_value"].le(0).any():
        return False, "OI sifir/negatif"
    if df["oi_value"].isna().any():
        return False, "NaN OI"
    return True, "ok"


# ==============================================================================
# 7) SNAPSHOT INSA (OI/funding + teyit indikatorleri)
# ==============================================================================
def determine_regime(cfg: Config, price_chg: float, oi_chg: float) -> str:
    """Pencere bazli fiyat-OI rejimi. Kucuk degisimler 'yatay' sayilir."""
    p_up = price_chg >= cfg.PRICE_FLAT_TH
    p_dn = price_chg <= -cfg.PRICE_FLAT_TH
    o_up = oi_chg >= cfg.OI_FLAT_TH
    o_dn = oi_chg <= -cfg.OI_FLAT_TH
    if o_up and p_up:
        return REGIME_LONG_BUILDUP
    if o_up and p_dn:
        return REGIME_SHORT_BUILDUP
    if o_dn and p_up:
        return REGIME_SHORT_COVERING
    if o_dn and p_dn:
        return REGIME_LONG_UNWIND
    return REGIME_NEUTRAL


def rising_streak(values: pd.Series) -> int:
    """Serinin sonundan geriye ardisik kac deger bir oncekinden buyuk."""
    streak = 0
    v = values.values
    for i in range(len(v) - 1, 0, -1):
        if v[i] > v[i - 1]:
            streak += 1
        else:
            break
    return streak


def build_snapshot(cfg: Config, symbol: str, df: pd.DataFrame,
                   oi_df: pd.DataFrame, funding: Optional[dict],
                   liq: Optional[dict] = None) -> PositioningSnapshot:
    close = df["close"]
    price = float(close.iloc[-1])

    # --- fiyat degisimi (OI penceresiyle ayni bar sayisi) ---
    w = cfg.OI_WINDOW
    price_chg_1 = pct_change(price, float(close.iloc[-2]))
    price_chg_w = pct_change(price, float(close.iloc[-1 - w])) if len(close) > w else price_chg_1

    # --- open interest ---
    oi = oi_df["oi_value"]
    oi_now = float(oi.iloc[-1])
    oi_chg_1 = pct_change(oi_now, float(oi.iloc[-2]))
    oi_chg_w = pct_change(oi_now, float(oi.iloc[-1 - w])) if len(oi) > w else oi_chg_1
    streak = rising_streak(oi.tail(w + 1))

    # --- funding ---
    funding_pct = float(funding["funding_pct"]) if funding else float("nan")
    next_funding = funding.get("next_funding", "") if funding else ""

    # --- rejim (pencere bazli; daha stabil) ---
    regime = determine_regime(cfg, price_chg_w, oi_chg_w)

    # --- teyit indikatorleri ---
    vol_ma = df["volume"].rolling(cfg.VOL_MA_PERIOD).mean()
    vol_ratio = float(df["volume"].iloc[-1] / vol_ma.iloc[-1]) if vol_ma.iloc[-1] > 0 else 0.0
    rsi_now = float(rsi(close, cfg.RSI_PERIOD).iloc[-1])
    atrp = float(atr_percent(df, cfg.ATR_PERIOD).iloc[-1])
    ema20_now = float(ema(close, cfg.EMA_FAST).iloc[-1])
    ema50_now = float(ema(close, cfg.EMA_SLOW).iloc[-1])
    last_wick = wick_ratio(df.iloc[-1])

    long_liq = float(liq.get("long_liq", 0.0)) if liq else 0.0
    short_liq = float(liq.get("short_liq", 0.0)) if liq else 0.0

    return PositioningSnapshot(
        symbol=symbol, price=price,
        price_chg_1=price_chg_1, price_chg_w=price_chg_w,
        oi_value=oi_now, oi_chg_1=oi_chg_1, oi_chg_w=oi_chg_w,
        oi_rising_streak=streak,
        funding_pct=funding_pct, next_funding=next_funding,
        regime=regime,
        volume_ratio=vol_ratio, rsi=rsi_now, atr_pct=atrp,
        ema20=ema20_now, ema50=ema50_now,
        last_wick_ratio=last_wick,
        long_liq_usdt=long_liq, short_liq_usdt=short_liq,
    )


# ==============================================================================
# 8) SKORLAMA MOTORU  (toplam 100: OI 30 + rejim 20 + funding 20 + hacim 15 + trend 15)
# ==============================================================================
def score_oi(cfg: Config, s: PositioningSnapshot) -> tuple[float, list[str]]:
    """OI degisiminin buyuklugu (yon farketmeksizin OI ARTISI onemli)."""
    pts = 0.0
    reasons: list[str] = []
    if s.oi_chg_1 >= cfg.OI_SPIKE_STRONG:
        pts += 18
        reasons.append(f"OI tek mumda +{s.oi_chg_1:.1f}% sicradi")
    elif s.oi_chg_1 >= cfg.OI_SPIKE_WATCH:
        pts += 10
        reasons.append(f"OI tek mumda +{s.oi_chg_1:.1f}% artti")
    if s.oi_chg_w >= cfg.OI_WINDOW_STRONG:
        pts += 12
        reasons.append(f"OI son {cfg.OI_WINDOW} mumda +{s.oi_chg_w:.1f}%")
    elif s.oi_chg_w >= cfg.OI_WINDOW_WATCH:
        pts += 6
        reasons.append(f"OI son {cfg.OI_WINDOW} mumda +{s.oi_chg_w:.1f}%")
    if s.oi_rising_streak >= 4:
        pts += 4
        reasons.append(f"OI {s.oi_rising_streak} bardir kesintisiz yukseliyor")
    return min(pts, 30.0), reasons


def score_regime(s: PositioningSnapshot) -> tuple[float, list[str]]:
    """Fiyat-OI rejiminin netligi."""
    pts = 0.0
    reasons: list[str] = []
    if s.regime in (REGIME_LONG_BUILDUP, REGIME_SHORT_BUILDUP):
        pts = 20.0
        reasons.append(f"Rejim: {REGIME_LABEL_TR[s.regime]}")
    return pts, reasons


def score_funding(cfg: Config, s: PositioningSnapshot, direction: str) -> tuple[float, list[str]]:
    """Funding, sinyal yonunun TERSINE asiri kalabaliksa squeeze yakitidir."""
    pts = 0.0
    reasons: list[str] = []
    f = s.funding_pct
    if math.isnan(f):
        return 0.0, reasons
    if direction == "LONG":
        if f <= -cfg.FUNDING_EXTREME:
            pts = 20.0
            reasons.append(f"Funding asiri negatif ({f:+.3f}%) -> short squeeze yakiti")
        elif f <= -cfg.FUNDING_MODERATE:
            pts = 12.0
            reasons.append(f"Funding negatif ({f:+.3f}%) -> shortlar odeme yapiyor")
        elif abs(f) < cfg.FUNDING_MODERATE:
            pts = 6.0
            reasons.append(f"Funding notr ({f:+.3f}%)")
    elif direction == "SHORT":
        if f >= cfg.FUNDING_EXTREME:
            pts = 20.0
            reasons.append(f"Funding asiri pozitif ({f:+.3f}%) -> long squeeze yakiti")
        elif f >= cfg.FUNDING_MODERATE:
            pts = 12.0
            reasons.append(f"Funding pozitif ({f:+.3f}%) -> longlar odeme yapiyor")
        elif abs(f) < cfg.FUNDING_MODERATE:
            pts = 6.0
            reasons.append(f"Funding notr ({f:+.3f}%)")
    return min(pts, 20.0), reasons


def score_volume(s: PositioningSnapshot) -> tuple[float, list[str]]:
    pts = 0.0
    reasons: list[str] = []
    vr = s.volume_ratio
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


def score_trend(s: PositioningSnapshot, direction: str) -> tuple[float, list[str]]:
    """EMA/RSI/fiyat degisimi sinyal yonunu teyit ediyor mu?

    RSI bonusu SADECE 'saglikli momentum' bandinda verilir (LONG: 55-70,
    SHORT: 30-45). Bandin disinda (asiri alim/satim) hareket zaten uzamis
    demektir; bunu 'teyit' olarak odullendirmek tepe/dip kovalamayi tesvik
    eder (bkz. backtest bulgusu: RSI ust siniri olmadan LONG'lar sistemik
    olarak SL'e gidiyordu). Asiri bolgede bonus verilmez; ayrica score_risk
    tarafinda ayrica cezalandirilir.
    """
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


def score_liquidation(cfg: Config, s: PositioningSnapshot, direction: str) -> tuple[float, list[str]]:
    """Sinyal yonuyle AYNI tarafa yardimci likidasyon var mi (gercek squeeze teyidi).

    LONG sinyali icin SHORT likidasyonlari (kisa pozisyonlar zorla kapatiliyor,
    fiyati yukari itiyor) destekleyicidir; SHORT icin LONG likidasyonlari.
    Veri yalnizca websocket akisi calisirken ve semboller icin biriken olaylar
    varsa doludur; yoksa 0 puan doner (fonksiyon devre disi kalmaz, sadece sessiz).
    """
    pts = 0.0
    reasons: list[str] = []
    if direction == "LONG" and s.short_liq_usdt >= cfg.LIQ_MIN_NOTIONAL_BONUS:
        pts = min(cfg.LIQ_SCORE_MAX, cfg.LIQ_SCORE_MAX * (s.short_liq_usdt / (cfg.LIQ_MIN_NOTIONAL_BONUS * 3)))
        reasons.append(f"Son {cfg.LIQ_WINDOW_SECONDS // 60}dk short likidasyon: {fmt_oi(s.short_liq_usdt)} (squeeze teyidi)")
    elif direction == "SHORT" and s.long_liq_usdt >= cfg.LIQ_MIN_NOTIONAL_BONUS:
        pts = min(cfg.LIQ_SCORE_MAX, cfg.LIQ_SCORE_MAX * (s.long_liq_usdt / (cfg.LIQ_MIN_NOTIONAL_BONUS * 3)))
        reasons.append(f"Son {cfg.LIQ_WINDOW_SECONDS // 60}dk long likidasyon: {fmt_oi(s.long_liq_usdt)} (squeeze teyidi)")
    return round(min(pts, cfg.LIQ_SCORE_MAX), 2), reasons


def score_risk(cfg: Config, s: PositioningSnapshot, direction: str) -> tuple[float, list[str]]:
    penalty = 0.0
    warnings: list[str] = []
    f = s.funding_pct
    # sinyal yonuyle AYNI tarafta asiri kalabalik funding -> likidasyon riski
    if not math.isnan(f):
        if direction == "LONG" and f >= cfg.FUNDING_EXTREME:
            penalty += 10; warnings.append(f"Funding asiri pozitif ({f:+.3f}%) - longlar kalabalik, likidasyon riski")
        elif direction == "SHORT" and f <= -cfg.FUNDING_EXTREME:
            penalty += 10; warnings.append(f"Funding asiri negatif ({f:+.3f}%) - shortlar kalabalik, squeeze riski")
    else:
        penalty += 5; warnings.append("Funding olculemedi")
    if s.last_wick_ratio > 0.60:
        penalty += 5; warnings.append(f"Asiri fitil ({s.last_wick_ratio:.2f})")
    if s.volume_ratio < 0.8:
        penalty += 5; warnings.append("Hacim teyidi yok (ortalama alti)")
    if direction == "LONG" and s.rsi > 70:
        extra = 13 if s.rsi > 80 else 8
        penalty += extra; warnings.append(f"RSI asiri alim ({s.rsi:.0f}) - hareket uzamis, pullback riski")
    if direction == "SHORT" and s.rsi < 30:
        extra = 13 if s.rsi < 20 else 8
        penalty += extra; warnings.append(f"RSI asiri satim ({s.rsi:.0f}) - hareket uzamis, pullback riski")
    if s.oi_chg_w > 0 and s.oi_chg_1 < 0:
        penalty += 3; warnings.append("OI son barda geriliyor")
    # "climax/tukenme" paterni: tek bar'da COK buyuk OI sicramasi + belirgin fitil
    # -> pozisyon zaten aciliyor ama fiyat kismen reddediliyor demek, bu genelde
    # devam degil TERSINE DONUS sinyali (backtest bulgusu: STRONG katman -tam da
    # bu profildeki sinyaller yuzunden- WATCH katmanindan daha kotu performans
    # gosteriyordu; bkz. classify_alert'teki oi_strong + streak sarti).
    if s.oi_chg_1 >= cfg.OI_SPIKE_STRONG * 1.5 and s.last_wick_ratio > 0.35:
        penalty += 10
        warnings.append(f"Ani OI sicramasi (+{s.oi_chg_1:.1f}%) + belirgin fitil ({s.last_wick_ratio:.2f}) - climax/tukenme riski")
    if s.atr_pct < cfg.MIN_ATR_PCT:
        penalty += 8; warnings.append(f"Volatilite dusuk (ATR%% {s.atr_pct:.2f} < {cfg.MIN_ATR_PCT:.2f}) - SL/TP mesafesi dar")
    return min(penalty, 30.0), warnings


def determine_direction(s: PositioningSnapshot) -> str:
    """Rejim tabanli yon: yalnizca buildup rejimleri islenebilir sinyaldir."""
    if s.regime == REGIME_LONG_BUILDUP:
        return "LONG"
    if s.regime == REGIME_SHORT_BUILDUP:
        return "SHORT"
    return "NEUTRAL"


def compute_sl_tp(cfg: Config, s: PositioningSnapshot, direction: str) -> tuple[float, float, float, float]:
    """ATR bazli SL + iki kademeli TP hesapla. Doner: (sl, tp1, tp2, risk_pct)."""
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


def evaluate(cfg: Config, s: PositioningSnapshot, timeframe: str) -> Signal:
    direction = determine_direction(s)

    oi_s, oi_r = score_oi(cfg, s)
    reg_s, reg_r = score_regime(s)
    fund_s, fund_r = score_funding(cfg, s, direction)
    volu_s, volu_r = score_volume(s)
    trend_s, trend_r = score_trend(s, direction)
    liq_s, liq_r = score_liquidation(cfg, s, direction)
    risk_p, risk_w = score_risk(cfg, s, direction)

    raw = oi_s + reg_s + fund_s + volu_s + trend_s + liq_s - risk_p
    score = float(max(0.0, min(100.0, raw)))

    reasons = reg_r + oi_r + fund_r + volu_r + trend_r + liq_r

    sl, tp1, tp2, risk_pct = compute_sl_tp(cfg, s, direction)

    # teknik invalidation seviyesi (tavsiye degil, sadece teknik referans)
    if direction == "LONG":
        invalidation = f"OI dususu + EMA20 alti {timeframe} kapanis ({fmt_price(s.ema20)})"
    elif direction == "SHORT":
        invalidation = f"OI dususu + EMA20 ustu {timeframe} kapanis ({fmt_price(s.ema20)})"
    else:
        invalidation = "-"

    return Signal(
        symbol=s.symbol, timeframe=timeframe, direction=direction, score=score,
        oi_score=oi_s, regime_score=reg_s, funding_score=fund_s,
        volume_score=volu_s, trend_score=trend_s, liquidation_score=liq_s,
        risk_penalty=risk_p,
        reasons=reasons, warnings=risk_w, price=s.price,
        invalidation=invalidation, snap=s,
        stop_loss=sl, take_profit1=tp1, take_profit2=tp2, risk_pct=risk_pct,
    )


# ==============================================================================
# 9) ALARM SEVIYESI (STRONG / WATCH / NONE)
# ==============================================================================
def classify_alert(cfg: Config, sig: Signal) -> str:
    s = sig.snap
    if s is None:
        return "NONE"

    # --- bastirma kurallari ---
    if sig.direction == "NEUTRAL":
        return "NONE"
    if s.oi_chg_1 < cfg.OI_SPIKE_WATCH and s.oi_chg_w < cfg.OI_WINDOW_WATCH:
        return "NONE"                      # OI hareketi yoksa alarm yok
    if s.atr_pct < cfg.MIN_ATR_PCT:
        return "NONE"                      # piyasa fiilen hareketsiz, SL/TP anlamsiz
    if sig.score < cfg.MIN_SCORE_WATCH:
        return "NONE"

    # --- guclu firsat ---
    # Tek bar'lik izole bir OI sicramasi TEK BASINA "guclu" sayilmaz; bu genelde
    # climax/tukenme paterni olabilir (bkz. score_risk). STRONG icin ya sicrama
    # ONCESINDEN de OI zaten yukseliyor olmali (streak>=2, yani "sicrama"
    # aslinda devam eden bir birikimin uzerine gelmis) ya da pencere bazli
    # (daha yavas/istikrarli) buyume kriteri saglanmali.
    oi_strong = (
        (s.oi_chg_1 >= cfg.OI_SPIKE_STRONG and s.oi_rising_streak >= 2)
        or s.oi_chg_w >= cfg.OI_WINDOW_STRONG
    )
    if (sig.score >= cfg.MIN_SCORE_STRONG
            and oi_strong
            and s.volume_ratio >= cfg.MIN_VOLUME_RATIO_STRONG
            and sig.risk_penalty <= 15):
        return "STRONG"

    # --- izleme ---
    # Ust sinir YOK: GUCLU olamayan ama skoru >= WATCH esigi olan sinyaller
    # olu bolgeye dusmesin, en azindan IZLEME olarak bildirilsin.
    if sig.score >= cfg.MIN_SCORE_WATCH:
        return "WATCH"

    return "NONE"


# ==============================================================================
# 9b) COKLU ZAMAN DILIMI CONFLUENCE (1h/4h uyum kontrolu)
# ==============================================================================
_TF_ORDER = ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"]


def _tf_rank(tf: str) -> int:
    try:
        return _TF_ORDER.index(tf)
    except ValueError:
        return -1


def apply_confluence(cfg: Config, signals: list[Signal]) -> None:
    """Ayni sembolun farkli TF sinyalleri arasinda uyum kontrolu (yerinde gunceller).

    En buyuk TIMEFRAME 'baglam/rejim' TF'i sayilir. Kucuk TF'lerdeki sinyal
    onunla ayni yondeyse bonus, ters yondeyse ceza alir (skor + tier yeniden
    hesaplanir). Baglam TF'i NOTR ise dokunulmaz (yeterli bilgi yok).
    """
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
def _ema_bias(s: PositioningSnapshot) -> str:
    """EMA20/EMA50/fiyat siralamasina gore yapisal egilim (OI'dan bagimsiz)."""
    if s.price > s.ema20 > s.ema50:
        return "UP"
    if s.price < s.ema20 < s.ema50:
        return "DOWN"
    return "FLAT"


def apply_btc_filter(cfg: Config, signals: list[Signal]) -> None:
    """BTC/USDT'nin ayni TF'deki yapisal egilimine gore diger coinlerin
    sinyaline bonus/ceza uygular (yerinde gunceller)."""
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


def fmt_oi(x: float) -> str:
    """OI notional degerini insan-okur formatta yaz (USDT)."""
    if x >= 1e9:
        return f"{x / 1e9:.2f}B USDT"
    if x >= 1e6:
        return f"{x / 1e6:.1f}M USDT"
    if x >= 1e3:
        return f"{x / 1e3:.0f}K USDT"
    return f"{x:.0f} USDT"


# TradingView zaman dilimi kodlari
_TV_INTERVAL = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
    "1d": "D", "1w": "W",
}


def tradingview_link(cfg: Config, sig: "Signal") -> str:
    """Coinin ilgili paritedeki TradingView 'Super Grafik' (Supercharts) linki.

    Ornek: BINANCE:BTCUSDT.P (USDT-M perpetual), dogru zaman dilimi (interval)
    parametresiyle. 1000x'li pariteler (1000PEPEUSDT vb.) borsadaki gercek
    sembol uzerinden dogru olusturulur.
    """
    ex = sig.ex_symbol or sig.symbol
    is_future = ":" in ex                      # 'BTC/USDT:USDT' -> perpetual
    core = ex.split(":")[0]                    # "BASE/QUOTE"
    pair = core.replace("/", "").upper()       # "BASEQUOTE"
    # borsa onekini TradingView koduna cevir (binance/binanceusdm -> BINANCE)
    ex_name = cfg.EXCHANGE_NAME.strip().lower()
    tv_ex = {"binance": "BINANCE", "binanceusdm": "BINANCE",
             "okx": "OKX", "bybit": "BYBIT", "kucoin": "KUCOIN",
             "gateio": "GATEIO", "mexc": "MEXC", "bitget": "BITGET"}.get(ex_name, ex_name.upper())
    tv_symbol = f"{tv_ex}:{pair}"
    if is_future:
        tv_symbol += ".P"                      # perpetual son eki
    url = f"https://www.tradingview.com/chart/?symbol={tv_symbol}"
    interval = _TV_INTERVAL.get(sig.timeframe, "")
    if interval:
        url += f"&interval={interval}"
    return url


def _regime_short(regime: str) -> str:
    """REGIME_LABEL_TR aciklamasindan parantez icindeki uzun notu atip
    sadece kisa etiketi doner (ozet Telegram mesaji icin)."""
    label = REGIME_LABEL_TR.get(regime, regime)
    return label.split(" (")[0]


def build_message(cfg: Config, sig: Signal) -> str:
    """Kisa/ozet Telegram mesaji: sadece en onemli parametreler.

    Detayli neden listesi, tam pozisyonlanma dokumu ve risk-notu satirlari
    KALDIRILDI; sadece yon/skor/rejim/fiyat/OI/funding/SL-TP ve (varsa) en
    kritik tek uyari kaliyor.
    """
    s = sig.snap
    tier_emoji = "🚨" if sig.tier == "STRONG" else "👀"
    fund_txt = f"{s.funding_pct:+.3f}%" if not math.isnan(s.funding_pct) else "n/a"

    lines = [
        f"{tier_emoji} {sig.tier} | {sig.symbol} | {sig.direction} | {sig.timeframe}",
        f"Skor: {sig.score:.0f}/100 | {_regime_short(s.regime)}",
        f"Fiyat: {fmt_price(s.price)} | OI: {s.oi_chg_1:+.1f}%/{s.oi_chg_w:+.1f}% | Funding: {fund_txt}",
    ]
    if sig.stop_loss > 0:
        lines.append(
            f"SL: {fmt_price(sig.stop_loss)} ({sig.risk_pct:.1f}%) | "
            f"TP1: {fmt_price(sig.take_profit1)} | TP2: {fmt_price(sig.take_profit2)}"
        )
    if sig.warnings:
        lines.append(f"⚠️ {sig.warnings[0]}")
    lines.append(f"📊 {tradingview_link(cfg, sig)}")
    return "\n".join(lines)


def build_exit_message(cfg: Config, symbol: str, timeframe: str, prev_direction: str,
                       current_regime: str, price: float) -> str:
    """Daha once LONG/SHORT alarmi verilmis bir coin icin rejim degistiginde/
    notrlestiginde gonderilen kisa 'gozden gecir/cik' bildirimi."""
    return (
        f"🔔 REJIM DEGISTI | {symbol} | {timeframe}\n"
        f"{prev_direction} artik teyit edilmiyor -> {_regime_short(current_regime)}\n"
        f"Fiyat: {fmt_price(price)}"
    )


def send_telegram(cfg: Config, text: str) -> bool:
    """Senkron HTTP (requests). Async dongude asyncio.to_thread ile cagrilir."""
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
        # zaman dilimi bazli: ayni coin farkli TF'lerde ayri alarm atabilir
        return f"{sig.symbol}|{sig.timeframe}"

    def should_alert(self, sig: Signal) -> bool:
        key = self._key(sig)
        now = time.time()
        prev = self.state.get(key)
        if prev is None:
            return True
        # yon degistiyse yeni mesaj
        if prev.get("direction") != sig.direction:
            return True
        elapsed_min = (now - prev.get("ts", 0)) / 60.0
        if elapsed_min >= self.cfg.ALERT_COOLDOWN_MINUTES:
            return True
        # cooldown icinde ama skor belirgin artmissa guncelleme
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
# 11b) SANAL PERFORMANS TAKIBI
# ==============================================================================
class PerformanceTracker:
    """STRONG/WATCH sinyallerini 'sanal islem' olarak kaydedip SL/TP'ye gore
    otomatik kapatir. Boylece skorlama mantiginin gercekte ne kadar isabetli
    oldugu zaman icinde OLCULEBILIR (bu, gecmiste botta hic yoktu).

    Basit iki-kademeli yonetim: TP1'de 'yarim pozisyon' kapanir varsayilir ve
    stop girise (breakeven) cekilir; sonra TP2 ya da breakeven'e donus ile
    islem tamamen kapanir. R = risk biriminin katlari.
    """

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
    fund = f"{s.funding_pct:+.4f}%" if s and not math.isnan(s.funding_pct) else "n/a"
    tag = {"STRONG": "🚨", "WATCH": "👀", "NONE": "  "}.get(sig.tier, "  ")
    log.info("%s %-13s %-4s %-7s skor=%5.1f oi=%.0f rejim=%.0f fund=%.0f volu=%.0f trend=%.0f liq=%.0f conf=%+.0f btc=%+.0f risk=-%.0f | OI1=%+.2f%% OIw=%+.2f%% F=%s VR=%.2f [%s] SL=%s TP1=%s",
             tag, sig.symbol, sig.timeframe, sig.direction, sig.score,
             sig.oi_score, sig.regime_score, sig.funding_score,
             sig.volume_score, sig.trend_score, sig.liquidation_score,
             sig.confluence_adj, sig.btc_filter_adj, sig.risk_penalty,
             s.oi_chg_1 if s else 0.0, s.oi_chg_w if s else 0.0, fund,
             s.volume_ratio if s else 0.0, s.regime if s else "-",
             fmt_price(sig.stop_loss) if sig.stop_loss else "-",
             fmt_price(sig.take_profit1) if sig.take_profit1 else "-")


# ==============================================================================
# 13) TEK COIN ISLEME  (async, coklu zaman dilimi paralel)
# ==============================================================================
async def process_symbol(cfg: Config, client: ExchangeClient, symbol: str,
                         sem: asyncio.Semaphore,
                         liq_tracker: Optional["LiquidationTracker"] = None) -> list[Signal]:
    """Bir coini TIMEFRAMES listesindeki HER zaman diliminde ayri ayri tarar.

    Funding + tum TF OHLCV + tum TF OI gecmisi istekleri TEK SEFERDE, es zamanli
    (asyncio.gather) cekilir. Coinler arasi paralellik `sem` ile sinirlanir.
    """
    async with sem:
        ex_symbol = client.resolve_symbol(symbol)
        if ex_symbol is None:
            log.debug("Borsa'da yok/pasif: %s (atlandi)", symbol)
            return []

        # tum istekleri paralel baslat: [funding, ohlcv(tf1..), oihist(tf1..)]
        tasks: list = [client.fetch_funding(ex_symbol)]
        tasks += [client.fetch_ohlcv(ex_symbol, tf, cfg.OHLCV_LIMIT) for tf in cfg.TIMEFRAMES]
        tasks += [client.fetch_oi_history(ex_symbol, tf, cfg.OI_HIST_LIMIT) for tf in cfg.TIMEFRAMES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        funding = results[0] if not isinstance(results[0], Exception) else None
        n = len(cfg.TIMEFRAMES)
        ohlcv_results = results[1:1 + n]
        oi_results = results[1 + n:1 + 2 * n]

        signals: list[Signal] = []
        for tf, df, oi_df in zip(cfg.TIMEFRAMES, ohlcv_results, oi_results):
            if isinstance(df, Exception):
                log.warning("OHLCV hata %s [%s]: %s (atlandi)", symbol, tf, df)
                continue
            if isinstance(oi_df, Exception):
                log.warning("OI hata %s [%s]: %s (atlandi)", symbol, tf, oi_df)
                continue
            ok, reason = validate_ohlcv(df)
            if not ok:
                log.warning("OHLCV kalitesi dusuk %s [%s]: %s (atlandi)", symbol, tf, reason)
                continue
            ok, reason = validate_oi(cfg, oi_df)
            if not ok:
                log.warning("OI kalitesi dusuk %s [%s]: %s (atlandi)", symbol, tf, reason)
                continue
            liq = liq_tracker.recent_liquidation(ex_symbol, cfg.LIQ_WINDOW_SECONDS) if liq_tracker else None
            snap = build_snapshot(cfg, symbol, df, oi_df, funding, liq)
            sig = evaluate(cfg, snap, tf)
            sig.ex_symbol = ex_symbol          # TradingView linki icin gercek parite
            sig.tier = classify_alert(cfg, sig)
            signals.append(sig)

        # ayni sembolun TF'leri arasinda 1h/4h uyum kontrolu (skor/tier gunceller)
        apply_confluence(cfg, signals)
        return signals


# ==============================================================================
# 14) TARAMA TURU  (async)
# ==============================================================================
async def scan_once(cfg: Config, client: ExchangeClient, cooldown: CooldownManager,
                    symbols: list[str], liq_tracker: Optional["LiquidationTracker"] = None,
                    perf: Optional["PerformanceTracker"] = None) -> None:
    t0 = time.time()
    log.info("=" * 78)
    log.info("OI/Funding taramasi basladi | %s | %d coin | tf=%s | paralellik=%d",
             datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
             len(symbols), ",".join(cfg.TIMEFRAMES), cfg.MAX_CONCURRENCY)
    log.info("=" * 78)

    sem = asyncio.Semaphore(cfg.MAX_CONCURRENCY)
    tasks = [process_symbol(cfg, client, s, sem, liq_tracker) for s in symbols]
    per_symbol = await asyncio.gather(*tasks, return_exceptions=True)

    signals: list[Signal] = []
    for symbol, res in zip(symbols, per_symbol):
        if isinstance(res, Exception):
            log.warning("Islem hatasi %s: %s (atlandi)", symbol, res)
            continue
        for sig in res:
            signals.append(sig)

    # BTC piyasa filtresi: tum semboller toplandiktan sonra (BTC sinyaline ihtiyac var)
    apply_btc_filter(cfg, signals)

    # terminal ozeti (skora gore sirali)
    for sig in sorted(signals, key=lambda x: x.score, reverse=True):
        print_summary(sig)

    # rejim degisimi / exit bildirimleri: daha once LONG/SHORT alarmi verilmis
    # bir coin/TF artik ayni yonde teyit edilmiyorsa ayri bir "gozden gecir" mesaji
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
            cooldown.clear(key)   # ayni yon tekrar olustugunda yeni giris sayilsin

    # alarm gonder (Telegram HTTP'yi ayri thread'de calistir; dongu bloklanmasin)
    alerts = 0
    for sig in sorted(signals, key=lambda x: x.score, reverse=True):
        if sig.tier in ("STRONG", "WATCH") and cooldown.should_alert(sig):
            msg = build_message(cfg, sig)
            ok = await asyncio.to_thread(send_telegram, cfg, msg)
            if ok:
                log.info("📨 Telegram gonderildi: %s [%s] (%s)", sig.symbol, sig.timeframe, sig.tier)
                cooldown.record(sig)
                alerts += 1
                if perf is not None:
                    perf.open_trade(sig)

    # sanal performans takibini guncel fiyatlarla senkronize et
    if perf is not None:
        perf.update(signals)

    dt = time.time() - t0
    log.info("-" * 78)
    log.info("Tarama bitti. Sinyal(coin x TF)=%d, alarm=%d, exit=%d, sure=%.1fs",
             len(signals), alerts, exit_alerts, dt)


# ==============================================================================
# 15) SELFTEST (API'siz sahte veri)
# ==============================================================================
def make_fake_df(n: int = 200, trend: float = 0.0, vol: float = 0.01,
                 seed: int = 1, volume_spike: bool = True) -> pd.DataFrame:
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
        rows.append([start + i * 3_600_000, open_, high, low, close, volume])
        price = close
    return pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"]).astype(
        {"open": float, "high": float, "low": float, "close": float, "volume": float})


def make_fake_oi(n: int = 48, growth: float = 0.0, seed: int = 1,
                 spike: bool = False) -> pd.DataFrame:
    """Sahte OI serisi. growth: bar basina ort. % buyume; spike: son barda sicrama."""
    rng = np.random.default_rng(seed)
    oi = 50_000_000.0
    rows = []
    start = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    for i in range(n):
        oi *= (1 + growth / 100.0 + rng.normal(0, 0.002))
        if spike and i == n - 1:
            oi *= 1.045                       # son barda +%4.5 OI sicramasi
        rows.append([start + i * 3_600_000, max(oi, 1.0)])
    return pd.DataFrame(rows, columns=["timestamp", "oi_value"])


def run_selftest(cfg: Config) -> None:
    log.info("SELFTEST: sahte veriyle OI/funding skorlama testi")
    scenarios = [
        # (isim, fiyat trendi, OI buyumesi, OI spike, funding%, seed)
        ("LONGBUILD/USDT", 0.006, 1.2, True, -0.08, 7),    # fiyat↑ OI↑ + negatif funding (squeeze)
        ("SHORTBUILD/USDT", -0.006, 1.2, True, 0.09, 13),  # fiyat↓ OI↑ + pozitif funding
        ("COVERING/USDT", 0.005, -1.0, False, 0.01, 5),    # fiyat↑ OI↓ -> alarm yok
        ("FLAT/USDT", 0.0, 0.0, False, 0.005, 3),          # yatay -> alarm yok
    ]
    printed = False
    for name, trend, growth, spike, fund, seed in scenarios:
        df = make_fake_df(trend=trend, seed=seed)
        oi_df = make_fake_oi(n=cfg.OI_HIST_LIMIT, growth=growth, seed=seed, spike=spike)
        funding = {"funding_pct": fund, "next_funding": "2025-01-09T08:00:00Z"}
        snap = build_snapshot(cfg, name, df, oi_df, funding)
        sig = evaluate(cfg, snap, cfg.TIMEFRAMES[0])
        sig.tier = classify_alert(cfg, sig)
        print_summary(sig)
        if sig.tier != "NONE" and not printed:
            print("\n----- ORNEK TELEGRAM MESAJI -----")
            print(build_message(cfg, sig))
            print("---------------------------------\n")
            printed = True
    log.info("SELFTEST tamam. (Gercek veri icin: python main.py --once)")


# ==============================================================================
# 15b) BACKTEST (gecmis veriyle geriye donuk sinyal testi)
# ==============================================================================
def simulate_forward(cfg: Config, sig: Signal, future: pd.DataFrame) -> Optional[float]:
    """Sinyal sonrasi 'future' barlarinda SL/TP1/TP2'nin (high/low ile) hangisinin
    once vurdugunu simule eder ve R sonucunu doner. PerformanceTracker.update()
    ile AYNI iki-kademeli mantik (TP1'de breakeven'e cekme) kullanilir; boylece
    canli takip ile backtest sonuclari tutarlidir. Sonuc bulunamazsa (bar bitti,
    ne SL ne TP) None doner -> "belirsiz/acik kaldi" sayilir, istatistige girmez.
    """
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
    """BTC'nin tarihsel EMA20/EMA50/fiyat siralamasindan, her bar icin yapisal
    egilimi (UP/DOWN/FLAT) onceden hesaplayip zaman damgasi ile birlikte
    listeler halinde doner. bisect ile 'ts anindaki EN SON bilinen bias'
    aranabilir (lookahead yok - sadece o ana kadarki veri kullanilir)."""
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
                       forward_bars: int = 20, ohlcv_limit: int = 500, oi_limit: int = 500) -> None:
    """Her sembol icin gecmis OHLCV + OI verisini ceker, her gecerli bar noktasinda
    CANLI KODLA AYNI fonksiyonlari (build_snapshot/evaluate/classify_alert)
    kullanarak sinyal uretir ve ileri `forward_bars` mumda SL/TP sonucunu kontrol
    eder. Boylece skorlama mantiginin GECMISTE ne kadar isabetli oldugu olculur.

    BTC piyasa filtresi ARTIK BACKTEST'E DAHIL (canli davranisla tutarlilik
    icin): BTC'nin o zamandaki EMA20/EMA50 yapisal egilimine gore sinyale ayni
    bonus/ceza uygulanir (apply_btc_filter ile ayni mantik, lookahead yok -
    sadece sinyal barina kadarki BTC verisi kullanilir).

    ONEMLI SINIRLAMALAR (hala gecerli):
      - Funding gecmisi kullanilmiyor (funding_score bu testte hep dusuk/notr
        katkida bulunur); gercek canli taramada funding ek bir sinyal kaynagidir.
      - Likidasyon ve 1h/4h confluence backtest'e dahil DEGIL (confluence,
        ayni sembolun iki TF'sinin ES ZAMANLI degerlendirilmesini gerektirir;
        bu tek-TF backtest dongusunde karsiligi yok).
      - Binance OI gecmisi yalnizca ~son 30 gunu saklar; bu yuzden test penceresi
        kisa kalabilir (ozellikle kucuk TF'lerde uzun donem sonuc vermez).
    """
    client = ExchangeClient(cfg)
    all_results: list[dict] = []
    try:
        await client.load_markets()

        # BTC verisini BIR KEZ cek (piyasa filtresi icin); bulunamazsa/cekilemezse
        # filtre sessizce devre disi kalir (FLAT donuyor -> etkisiz).
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
            df = await client.fetch_ohlcv(ex_symbol, timeframe, ohlcv_limit)
            oi_df = await client.fetch_oi_history(ex_symbol, timeframe, oi_limit)
            if df is None or oi_df is None:
                continue
            ok, _ = validate_ohlcv(df)
            if not ok:
                continue
            ok, _ = validate_oi(cfg, oi_df)
            if not ok:
                continue

            n = min(len(df), len(oi_df))
            df_a = df.tail(n).reset_index(drop=True)
            oi_a = oi_df.tail(n).reset_index(drop=True)
            start = max(cfg.VOL_MA_PERIOD, cfg.OI_WINDOW + 2, cfg.EMA_SLOW) + 1
            for i in range(start, n - forward_bars):
                sub_df = df_a.iloc[: i + 1]
                sub_oi = oi_a.iloc[: i + 1]
                try:
                    snap = build_snapshot(cfg, symbol, sub_df, sub_oi, None)
                except Exception:
                    continue
                sig = evaluate(cfg, snap, timeframe)
                sig.tier = classify_alert(cfg, sig)
                if sig.tier == "NONE" or sig.direction == "NEUTRAL":
                    continue

                # BTC piyasa filtresi (canli scan_once/apply_btc_filter ile ayni mantik)
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

                future = df_a.iloc[i + 1: i + 1 + forward_bars]
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
        expectancy = avg_r  # basit: ortalama R = beklenti (sabit risk birimi varsayimiyla)
        log.info("%-8s: n=%-4d win_rate=%%%-5.1f avg_R=%+.2f (beklenti/islem)", label, len(grp), win_rate, expectancy)
    for direction in ("LONG", "SHORT"):
        grp = bt_df[bt_df.direction == direction]
        if len(grp) == 0:
            continue
        win_rate = (grp["r"] > 0).mean() * 100.0
        log.info("Yon=%-5s: n=%-4d win_rate=%%%-5.1f avg_R=%+.2f", direction, len(grp), win_rate, grp["r"].mean())
    log.info("-" * 78)
    log.info("NOT: Bu backtest artik BTC piyasa filtresini ICERIR. Funding ve "
             "1h/4h confluence katkilari HALA DAHIL DEGIL (bkz. run_backtest docstring).")


# ==============================================================================
# 16) ASYNC RUNNER
# ==============================================================================
def seconds_to_next_tick(interval: int) -> float:
    """Epoch'a hizali bir sonraki tik'e kadar kalan saniye.

    interval=900 icin tikler saat :00, :15, :30, :45 noktalarina denk gelir.
    Tarama ne kadar surerse sursun ya da hata alsa da kadans kaymaz; her zaman
    bir sonraki sinira hizalanir.
    """
    now = time.time()
    wait = interval - (now % interval)
    if wait < 1.0:            # tam sinirdaysak cift-tetiklemeyi onle
        wait += interval
    return wait


def check_timeframes(cfg: Config) -> None:
    """OI gecmisinin desteklemedigi TF'leri basta yakala."""
    bad = [tf for tf in cfg.TIMEFRAMES if tf not in OI_SUPPORTED_TIMEFRAMES]
    if bad:
        log.warning("Su TF'ler OI gecmisi tarafindan desteklenmez ve atlanir: %s "
                    "(desteklenen: %s)", ",".join(bad), ",".join(sorted(OI_SUPPORTED_TIMEFRAMES)))
        cfg.TIMEFRAMES = [tf for tf in cfg.TIMEFRAMES if tf in OI_SUPPORTED_TIMEFRAMES]
    if not cfg.TIMEFRAMES:
        raise RuntimeError("Gecerli zaman dilimi kalmadi. TIMEFRAMES ayarini duzeltin.")


async def ensure_markets(client: ExchangeClient) -> None:
    """Markets yuklu degilse yukle. Internet ilk acilista yoksa, gelince toparlar."""
    if not client.markets:
        await client.load_markets()


async def run_check(cfg: Config) -> None:
    """Hizli teshis: markets yuklenir ve birkac coinde OI + funding cekilir.

    Tarama beklemeden baglantinin/verinin calisip calismadigini gorursun.
    """
    client = ExchangeClient(cfg)
    try:
        log.info("BAGLANTI TESTI | borsa=%s | tip=%s", client.name, cfg.MARKET_TYPE)
        await client.load_markets()
        for s in SYMBOLS[:3]:
            ex_s = client.resolve_symbol(s)
            if not ex_s:
                log.warning("  %-10s -> borsada bulunamadi (atlanir)", s)
                continue
            oi_df = await client.fetch_oi_history(ex_s, cfg.TIMEFRAMES[0], 5)
            fund = await client.fetch_funding(ex_s)
            oi_txt = fmt_oi(float(oi_df["oi_value"].iloc[-1])) if oi_df is not None else "BOS"
            f_txt = f"{fund['funding_pct']:+.4f}%" if fund else "BOS"
            log.info("  %-10s (%s) OI=%s | funding=%s", s, ex_s, oi_txt, f_txt)
        log.info("BAGLANTI TESTI BASARILI ✅  (normal calistir: python main.py)")
    except Exception as e:
        log.error("BAGLANTI TESTI BASARISIZ [%s]: %s", type(e).__name__, e)
        log.error("-> Bu borsa/uc nokta senin agindan erisilemiyor olabilir. "
                  ".env'de EXCHANGE_NAME=bybit ya da okx deneyebilirsin.")
    finally:
        await client.close()


async def run_scanner(cfg: Config, once: bool) -> None:
    client = ExchangeClient(cfg)
    liq_tracker = LiquidationTracker(cfg)
    liq_tracker.start()
    try:
        cooldown = CooldownManager(cfg)
        perf = PerformanceTracker(cfg)

        if once:
            await ensure_markets(client)
            await scan_once(cfg, client, cooldown, SYMBOLS, liq_tracker, perf)
            log.info("Sanal performans ozeti: %s", perf.summary())
            return

        interval = cfg.SCAN_INTERVAL_SECONDS
        log.info("Surekli tarama | her %ds (epoch'a hizali) | tf=%s | paralellik=%d | "
                 "internet kesintisine dayanikli | Ctrl+C ile durdur",
                 interval, ",".join(cfg.TIMEFRAMES), cfg.MAX_CONCURRENCY)
        while True:
            try:
                # markets yoksa yukle (ilk acilista internet yoksa burada tekrar denenir)
                await ensure_markets(client)
                await scan_once(cfg, client, cooldown, SYMBOLS, liq_tracker, perf)
                log.info("Sanal performans ozeti: %s", perf.summary())
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # baglanti / API hatasi: SISTEM DURMAZ; sonraki tik'te otomatik
                # tekrar denenir (kadans korunur). Gercek nedeni gormek icin
                # hata tipi + tam mesaj birlikte yazilir.
                log.error("Tarama hatasi [%s]: %s | sonraki tik'te tekrar denenecek",
                          type(e).__name__, e)

            # bir sonraki sabit sinira kadar bekle (kayma yok)
            wait = seconds_to_next_tick(interval)
            next_utc = datetime.fromtimestamp(time.time() + wait, timezone.utc).strftime("%H:%M:%S")
            log.info("Sonraki tarama ~%.0f sn sonra (%s UTC)", wait, next_utc)
            await asyncio.sleep(wait)
    finally:
        await liq_tracker.stop()
        await client.close()


# ==============================================================================
# 17) MAIN
# ==============================================================================
def _run(coro) -> None:
    """Async giris noktasi.

    Windows'ta aiohttp/aiodns DNS cozumlemesi varsayilan ProactorEventLoop ile
    sorun cikarabilir; SelectorEventLoop daha guvenlidir. Python 3.14'te
    deprecated olan set_event_loop_policy/WindowsSelectorEventLoopPolicy yerine
    dogrudan SelectorEventLoop olusturulur (uyari cikmaz)."""
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
    parser = argparse.ArgumentParser(description="Crypto OI + Funding Scanner (async)")
    parser.add_argument("--once", action="store_true", help="Tek tur tarama yap")
    parser.add_argument("--check", action="store_true", help="Hizli baglanti/veri testi")
    parser.add_argument("--selftest", action="store_true", help="API'siz sahte veri testi")
    parser.add_argument("--tg-test", action="store_true", help="Telegram baglanti testi")
    parser.add_argument("--backtest", action="store_true", help="Gecmis veriyle geriye donuk sinyal testi")
    parser.add_argument("--bt-tf", type=str, default=None, help="Backtest zaman dilimi (varsayilan: ilk TIMEFRAMES)")
    parser.add_argument("--bt-forward", type=int, default=20, help="Backtest: ileri kac bar sonucu kontrol edilsin")
    parser.add_argument("--bt-symbols", type=int, default=0, help="Backtest: sadece ilk N sembolu tara (0=hepsi, hizli test icin ornek: 5)")
    parser.add_argument("--perf", action="store_true", help="Sanal performans takibi ozetini goster ve cik")
    args = parser.parse_args()

    cfg = Config()
    check_timeframes(cfg)

    # --- Telegram konfigurasyon teshisi (degerler loglanmaz, sadece durum) ---
    if _ENV_LOADED_FROM:
        log.info("Env dosyasi yuklendi: %s | anahtarlar: %s",
                 _ENV_LOADED_FROM, ",".join(_ENV_KEYS_LOADED))
    else:
        log.warning("Hicbir .env dosyasi bulunamadi/okunamadi. Aranan yerler: "
                    "main.py klasoru ve calisma dizini (.env / .env.txt / env.txt)")
    if cfg.TELEGRAM_BOT_TOKEN and cfg.TELEGRAM_CHAT_ID:
        log.info("Telegram ayarlari OK (token: %d karakter, chat_id: %s...)",
                 len(cfg.TELEGRAM_BOT_TOKEN), cfg.TELEGRAM_CHAT_ID[:4])
    else:
        log.warning("Telegram token/chat_id BULUNAMADI! Cozumler: "
                    "1) .env icinde 'TELEGRAM_BOT_TOKEN=...' ve 'TELEGRAM_CHAT_ID=...' satirlari olsun. "
                    "2) YA DA main.py'nin basindaki TELEGRAM_BOT_TOKEN_SABIT / "
                    "TELEGRAM_CHAT_ID_SABIT alanlarina degerleri dogrudan yapistir.")

    if args.selftest:
        run_selftest(cfg)
        return

    if args.check:
        _run(run_check(cfg))
        return

    if args.tg_test:
        ok = send_telegram(cfg, "✅ OI/Funding Scanner Telegram testi.\n" + cfg.DISCLAIMER)
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
        if bt_tf not in OI_SUPPORTED_TIMEFRAMES:
            log.error("Gecersiz backtest TF: %s (desteklenen: %s)", bt_tf, ",".join(sorted(OI_SUPPORTED_TIMEFRAMES)))
            return
        _run(run_backtest(cfg, bt_symbols, bt_tf, forward_bars=args.bt_forward))
        return

    try:
        _run(run_scanner(cfg, once=args.once))
    except KeyboardInterrupt:
        log.info("Durduruldu (kullanici).")


if __name__ == "__main__":
    main()
