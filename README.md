# Notera

Notera, Microsoft Teams toplantılarına bot ile katılıp canlı caption, ses kaydı ve canlı önizleme toplayan; toplantı sonrasında WhisperX ile transcript'i netleştirip review akışıyla düzenlemeyi sağlayan bir uygulamadır.

## Ne Yapar?

- Teams toplantı linki ile yeni toplantı kaydı başlatır
- Botu toplantıya gönderir ve oturum boyunca caption, ses ve preview toplar
- Toplantı bitince WhisperX post-process çalıştırır
- Transcript ekranında satır bazlı review akışı sunar
- Review önerilerini uygular veya korur
- Transcript'i `TXT` ve `CSV` olarak dışa aktarır

## Bileşenler

- `frontend/`
  React + TypeScript + Vite arayüzü. Login, dashboard ve transcript ekranları burada çalışır.
- `backend/`
  FastAPI API katmanı. Auth, meeting lifecycle, snapshot üretimi, review işlemleri ve export mantığı burada bulunur.
- `backend/workers/`
  Playwright tabanlı Teams botu ve post-process worker'ları burada yer alır.
- `data/`
  Lokal veritabanı ve üretilen artefact'ların tutulduğu dizin.

## Gereksinimler

- Conda
- Python 3.11
- Node.js 22
- ffmpeg
- Playwright Chromium

`environment.yml` Python, Node ve backend bağımlılıklarını hazırlar.

## Kurulum

### 1. Conda ortamını hazırla

Yeni kurulum:

```bash
conda env create -f environment.yml
conda activate teams-bot
```

Mevcut ortamı güncellemek istersen:

```bash
conda env update -n teams-bot -f environment.yml --prune
conda activate teams-bot
```

### 2. Playwright browser kur

```bash
conda run -n teams-bot python -m playwright install chromium
```

### 3. Frontend paketlerini yükle

```bash
cd frontend
conda run -n teams-bot npm install
cd ..
```

### 4. Gerekirse `.env` oluştur

```bash
cp .env.example .env
```

Varsayılan ayarlar çoğu lokal kullanım için yeterlidir. Özelleştirme gerekiyorsa [`.env.example`](/Users/serdarcan/teams-meeting-transcript/.env.example) dosyasını temel al.

## Ortam Değişkenleri

Sık kullanılan backend değişkenleri:

- `NOTERA_API_HOST`
- `NOTERA_API_PORT`
- `NOTERA_SESSION_SECRET`
- `NOTERA_DB_PATH`
- `NOTERA_MEETING_AUDIO_ROOT`
- `NOTERA_LIVE_PREVIEW_ROOT`
- `NOTERA_REVIEW_CLIP_ROOT`
- `NOTERA_RUNTIME_CACHE_ROOT`
- `NOTERA_BOT_PYTHON_BIN`

Frontend için opsiyonel değişken:

- `VITE_API_BASE_URL`

Geliştirme modunda Vite zaten `/api` ve `/health` isteklerini `http://127.0.0.1:8000` adresine proxy eder; bu yüzden çoğu durumda `VITE_API_BASE_URL` tanımlamak gerekmez.

## Lokal Çalıştırma

Backend:

```bash
conda run -n teams-bot python -m backend
```

Frontend:

```bash
cd frontend
conda run -n teams-bot npm run dev
```

Adresler:

- Frontend: `http://localhost:5173`
- Backend health: `http://localhost:8000/health`

## Kullanım Akışı

1. Kullanıcı giriş yapar.
2. Dashboard üzerinden toplantı adı ve Teams toplantı linki girer.
3. Backend toplantı kaydını oluşturur ve bot sürecini başlatır.
4. Bot toplantıya katılır, caption, ses ve preview üretir.
5. Toplantı tamamlanınca post-process aşaması çalışır.
6. Transcript ekranında sonuçlar, review önerileri ve export aksiyonları görünür.

## Doğrulama

Frontend type-check:

```bash
cd frontend
./node_modules/.bin/tsc -b
```

Frontend production build:

```bash
cd frontend
conda run -n teams-bot npm run build
```

Backend syntax doğrulaması:

```bash
conda run -n teams-bot python -m compileall backend
```

## Production

Production compose dosyası:

- [`docker-compose.prod.yml`](/Users/serdarcan/teams-meeting-transcript/docker-compose.prod.yml)

Çalıştırma:

```bash
docker compose -f docker-compose.prod.yml up --build -d
```

Varsayılan davranış:

- frontend host üzerinde `3000` portunda açılır
- backend container içinde `8000` portunda çalışır
- kalıcı veri `notera-data` volume'unda tutulur
