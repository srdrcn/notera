# Notera

Notera, Microsoft Teams toplantılarına bir bot ile katılıp canlı caption ve ses artefact’ları toplayan, toplantı bittikten sonra WhisperX ile ikinci bir transcript üreten ve sonucu inline review akışıyla netleştiren bir operasyon uygulamasıdır.

Repo artık tek bir yeni mimariye hizmet eder:

- `frontend/`: React + TypeScript + Vite arayüzü
- `backend/`: FastAPI API, auth, meeting lifecycle, review ve export mantığı
- `backend/workers/`: Playwright tabanlı Teams botu ve WhisperX postprocess worker’ı

Eski Reflex uygulaması, eski Docker akışı ve legacy runtime katmanı repodan tamamen çıkarıldı.

## Mimari

### Bileşenler

- `frontend`
  Kullanıcı arayüzünü sunar. Login, dashboard ve transcript/review ekranları burada çalışır.
- `backend`
  Session auth, meeting CRUD, snapshot üretimi, review işlemleri, media stream ve worker orchestration burada bulunur.
- `backend/workers`
  Teams toplantısına katılır, caption event’leri yazar, preview üretir, ses kaydı alır ve postprocess aşamasını çalıştırır.

### Dizin Yapısı

```text
.
├── backend/
│   ├── app/
│   │   ├── api/
│   │   ├── db/
│   │   ├── models/
│   │   ├── orchestration/
│   │   ├── repositories/
│   │   ├── runtime/
│   │   ├── schemas/
│   │   └── services/
│   ├── workers/
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/
│   ├── public/
│   ├── src/
│   ├── Dockerfile
│   ├── nginx.conf
│   └── package.json
├── .github/workflows/docker-image.yml
├── .env.example
├── docker-compose.prod.yml
└── environment.yml
```

## Gereksinimler

Lokal geliştirme için gerekenler:

- Conda
- Node.js 22
- Python 3.11
- ffmpeg
- Playwright Chromium bağımlılıkları

Conda tarafı `environment.yml` ile kurulur. Frontend paketleri `npm` ile yüklenir. Playwright browser binary’si ayrıca indirilmelidir.

## Ortam Değişkenleri

Ana backend değişkenleri:

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

Referans değerler için [`.env.example`](.env.example) dosyasını kullan.

## Kurulum

### 1. Conda ortamını hazırla

Yeni kurulum için:

```bash
conda env create -f environment.yml
conda activate teams-bot
```

Mevcut `teams-bot` ortamını güncellemek istersen:

```bash
conda env update -n teams-bot -f environment.yml --prune
conda activate teams-bot
```

### 2. Backend bağımlılıklarını doğrula

`environment.yml` backend Python bağımlılıklarını yükler. Playwright browser binary’si için ayrıca şunu çalıştır:

```bash
conda run -n teams-bot python -m playwright install chromium
```

### 3. Frontend paketlerini kur

```bash
cd frontend
conda run -n teams-bot npm install
cd ..
```

### 4. İstersen `.env` oluştur

```bash
cp .env.example .env
```

Varsayılan yol kullanılacaksa `.env` zorunlu değildir. Backend aksi belirtilmedikçe veriyi repo kökündeki `data/` altında tutar.

## Çalıştırma Adımları

### Lokal geliştirme

Backend:

```bash
conda run -n teams-bot python -m backend.app
```

Frontend:

```bash
cd frontend
conda run -n teams-bot npm run dev
```

Adresler:

- Frontend: `http://localhost:5173`
- Backend health: `http://localhost:8000/health`

Geliştirme modunda Vite, `/api` isteklerini otomatik olarak backend’e proxy eder.

### Uygulama akışı

1. Kullanıcı e-posta ile giriş yapar veya kayıt olur.
2. Dashboard’dan yeni meeting oluşturur.
3. Frontend otomatik olarak `join` çağrısı yapar.
4. Backend bot subprocess’ini başlatır.
5. Bot canlı caption, preview ve audio artefact’larını üretir.
6. Bot tamamlanınca backend postprocess worker’ı başlatır.
7. WhisperX transcript ve alignment sonucu final transcript oluşur.
8. Review gereken satırlar transcript ekranında inline olarak görünür.
9. Kullanıcı review kararlarını verir, gerekirse duplicate kayıtları birleştirir.
10. Transcript `TXT` veya `CSV` olarak dışa aktarılır.

## Build / Test / Lint Komutları

Frontend build ve type-check:

```bash
cd frontend
conda run -n teams-bot npm run build
```

Backend ve worker syntax doğrulaması:

```bash
conda run -n teams-bot python -m compileall backend
```

Şu an repo içinde ayrı bir `lint` veya otomatik test framework’ü tanımlı değildir. Doğrulama akışı build, import ve syntax kontrolleri üzerinden yürür.

## Deployment

Üretim akışı iki container üzerinden çalışır:

- `frontend`: Nginx ile React build çıktısını sunar
- `backend`: FastAPI API ve local worker orchestration

Hazır compose dosyası:

- [`docker-compose.prod.yml`](docker-compose.prod.yml)

Çalıştırma:

```bash
docker compose -f docker-compose.prod.yml up --build -d
```

Varsayılan davranış:

- frontend hostta `3000` portundan açılır
- backend container içinde `8000` portunda çalışır
- kalıcı veri `/data` volume’unda tutulur

Docker image workflow’u iki ayrı image üretir:

- `chosenwar/notera-frontend`
- `chosenwar/notera-backend`

## Eski Yapıdan Kaldırılanlar

Bu refactor ile repodan tamamen çıkarılan başlıca alanlar:

- top-level `app/` klasörü ve Reflex uygulaması
- eski root `Dockerfile`
- eski single-image build akışı
- legacy runtime compatibility katmanı
- top-level `bot/` kaynak klasörü
- kullanılmayan asset/config/script kalıntıları

Repo artık yalnızca React + FastAPI + local worker mimarisine hizmet eder.

## Geliştirici Notları

- Worker’lar ayrı servis değil, backend tarafından subprocess olarak başlatılır.
- SQLite bilinçli olarak korunmuştur; bağlantılar WAL ve `busy_timeout` ile açılır.
- Runtime artefact’ları commit edilmez; `data/` ve eski lokal cache dizinleri ignore edilir.
- Yeni kod eklerken legacy katman geri getirilmeyecek. Tüm değişiklikler mevcut mimari doğrultusunda yapılmalı.
