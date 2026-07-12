# Crypto OI + Funding Rate Scanner

Binance USDT-M Futures üzerinde **Open Interest (OI)** değişimi ve **Funding
Rate** aşırılıklarını tarayan, fiyat-OI rejimini (long buildup / short
buildup / short covering / long unwind) sınıflandırıp 0-100 arası bir
"positioning score" üreten ve kriterlere uyan coinler için Telegram'a alarm
gönderen bir tarama botu.

> ⚠️ **Bu proje yatırım tavsiyesi üretmez.** Sadece türev piyasası
> pozisyonlanma verisine dayalı bir izleme/alarm aracıdır. Kendi risk
> yönetim kararlarını kendin ver.

## Özellikler

- Binance USDT-M Futures OI + funding taraması (1h/4h, çoklu zaman dilimi)
- Fiyat-OI rejim sınıflandırması ve ağırlıklı skorlama (OI, rejim, funding,
  hacim, trend, likidasyon)
- ATR bazlı iki kademeli stop-loss / take-profit planı
- 1h/4h confluence (zaman dilimleri arası uyum) bonus/ceza
- BTC piyasa filtresi (BTC yapısal trendine aykırı sinyaller cezalandırılır)
- Rejim değişiminde "pozisyonu gözden geçir" uyarısı
- Binance likidasyon akışı (best-effort, websocket)
- Sanal performans takibi (`--perf`) ve geriye dönük backtest (`--backtest`)
- Kısa/özet Telegram mesajları

## Kurulum (yerel)

```bash
pip install -r requirements.txt
copy .env.example .env      # Windows
# cp .env.example .env      # Linux/Mac
```

`.env` dosyasını kendi Telegram bot token / chat id'n ile doldur.

## Kullanım

```bash
python main.py --selftest          # API'siz, sahte veriyle mantık testi
python main.py --check             # hızlı bağlantı/veri testi
python main.py --once              # tek tur tarama
python main.py                     # sürekli tarama (SCAN_INTERVAL_SECONDS)
python main.py --backtest --bt-tf 1h   # geriye dönük sinyal testi
python main.py --perf              # sanal performans özeti
```

## 7/24 çalıştırma (GitHub Actions + cron-job.org, sunucu gerekmez)

Botu kendi bilgisayarını açık tutmadan, herhangi bir hosting/sunucu
kullanmadan periyodik çalıştırmak için: adım adım rehbere bak →
[`DEPLOYMENT_REHBERI.md`](./DEPLOYMENT_REHBERI.md)

Özet: cron-job.org, GitHub Actions'daki `.github/workflows/scanner.yml`
workflow'unu düzenli aralıklarla tetikler; her tetiklemede `main.py --once`
tek bir tarama turu yapar.

## Dosyalar

- `main.py` — tüm tarama/skorlama/alarm mantığı
- `.github/workflows/scanner.yml` — GitHub Actions workflow (cron-job.org
  tarafından tetiklenir)
- `.env.example` — konfigürasyon şablonu (gerçek `.env` asla repoya girmez)
- `requirements.txt` — bağımlılıklar
- `web.py` — (opsiyonel, kullanılmıyor) Render tabanlı alternatif kuruluma
  aitti; bu akışta gerekmez

## Lisans / Sorumluluk Reddi

Bu yazılım "olduğu gibi" sunulur, herhangi bir garanti içermez. Kod
sahibinin veya katkıda bulunanların, bu yazılımın kullanımından doğan
herhangi bir finansal kayıptan sorumluluğu yoktur.
