# Notera

Notera, Microsoft Teams toplantilari icin canli transcript toplama, arsivleme ve disa aktarma odakli bir uygulamadir. Uygulama iki ana parcadan olusur:

- `app/`: Reflex tabanli web arayuzu ve SQLite veritabani
- `bot/`: Playwright ile Teams toplantisina katilip live captions verisini yakalayan bot

## Temel gereksinimler

- Python 3.11
- `reflex==0.8.27`
- `playwright>=1.40,<2`
- Playwright icin Chromium ve ilgili sistem kutuphaneleri
- Yazilabilir SQLite dosyasi: `app/reflex.db`
- Yazilabilir gecici/gorsel dizinleri:
  - `app/assets/live_meeting_frames/`
  - `bot/`
- Ag erisimi:
  - Teams toplanti baglantilarina cikis
  - Uygulama frontend portu: `3000`
  - Uygulama backend portu: `8000`

## Calisma notlari

- Web uygulamasi `app/` klasorunden Reflex ile calisir.
- Bot sureci, web uygulamasi tarafindan alt surec olarak baslatilir.
- Bot transcript satirlarini ve toplanti durumunu dogrudan `app/reflex.db` icine yazar.
- Docker icinde de `app/` ve `bot/` klasorlerinin ayni dosya sistemi altinda kalmasi gerekir.

## Lokal calistirma

1. Python bagimliliklarini kurun:

```bash
cd app
python -m pip install -r requirements.txt
python -m playwright install --with-deps chromium
```

2. Uygulamayi production modunda calistirin:

```bash
cd app
reflex run --env prod --backend-host 0.0.0.0 --frontend-port 3000 --backend-port 8000
```

Frontend varsayilan olarak `http://localhost:3000`, backend ise `http://localhost:8000` adresinde calisir.

## Docker

Imaji olusturmak icin:

```bash
docker build -t notera .
```

Container'i calistirmak icin:

```bash
docker run --rm -p 3000:3000 -p 8000:8000 -e API_URL=http://localhost:8000 notera
```

Uygulamayi uzak bir sunucuda yayina alacaksaniz `API_URL` degerini kullanicinin tarayicisindan erisilebilen backend adresi olacak sekilde guncelleyin.
