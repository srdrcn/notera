# Notera

Notera, Microsoft Teams toplantıları için canlı caption toplama, ses kaydı alma, WhisperX ile doğrulama yapma ve review kuyruğu üzerinden final transcript üretme odaklı bir uygulamadır.

Repo iki ana parçadan oluşur:

- `app/`: Reflex tabanlı web arayüzü, state yönetimi ve SQLite veritabanı
- `bot/`: Playwright ile Teams toplantısına katılan bot, ses yakalama akışı ve toplantı sonrası WhisperX worker

## Neler var

- Dashboard üzerinden toplantı oluşturma ve oturumları takip etme
- Teams live caption event’lerini ham haliyle saklama
- Mümkünse Teams sesini `webm` olarak kaydetme ve `wav`a çevirme
- Canonical Teams transcript üretme
- WhisperX ile tüm ses dosyasını yeniden transcript etme
- Teams transcript ile WhisperX transcript’i hizalayıp final transcript oluşturma
- Şüpheli farkları review kuyruğuna bırakma
- TXT / CSV export
- Bot önizleme görseli ve kısa review ses klipleri

## Mimari

Akış kabaca şu şekilde çalışır:

1. Kullanıcı dashboard üzerinden toplantı oluşturur.
2. Web uygulaması bot sürecini alt süreç olarak başlatır.
3. Bot Teams toplantısına girer, caption event’lerini kaydeder.
4. Ses kaydı açıksa bot WebRTC ses track’lerini toplamaya çalışır.
5. Toplantı tamamlanınca `bot/postprocess_worker.py` çalışır.
6. Worker caption event’lerinden canonical Teams transcript üretir.
7. Worker ses kaydı varsa WhisperX ile transcript çıkarır ve hizalama yapar.
8. Güvenli düzeltmeler otomatik uygulanır, kararsız olanlar review kuyruğuna düşer.
9. Transcript sayfasında kullanıcı review kartları üzerinden `Uygula` / `Koru` kararı verir.

## Önemli dosyalar

- `app/app/app.py`
  UI bileşenleri ve sayfa yerleşimleri
- `app/app/state.py`
  Dashboard ve transcript sayfası state mantığı
- `app/app/models.py`
  SQLModel tabloları
- `app/app/meeting_runtime.py`
  runtime schema bootstrap, artifact yolları ve yardımcı fonksiyonlar
- `bot/bot.py`
  Teams bot süreci, Playwright akışı, caption ve ses kaydı
- `bot/postprocess_worker.py`
  canonical transcript, WhisperX, hizalama ve review üretimi
- `app/assets/notera.css`
  güncel UI stil dosyası

## Veritabanı ve schema

Ana veritabanı:

- `app/reflex.db`

Bu proje runtime schema bootstrap yaklaşımıyla çalışır.

- `ensure_runtime_schema(...)` eksik kolonları ekler
- yeni tabloları `CREATE TABLE IF NOT EXISTS` ile oluşturur
- app tarafında ve bot başlarken çağrılır

Pratik sonuç:

- mevcut kodla sıfırdan ayağa kalkarken ek migration adımı gerekmez
- `meeting`, `transcript` gibi ana tablolar Reflex / SQLModel tarafından oluşur
- review / audio / caption event alanları runtime sırasında tamamlanır

## Veri modeli özeti

Öne çıkan tablolar:

- `meeting`
  toplantı kaydı, bot durumu, ses ve postprocess durumu
- `teamscaptionevent`
  Teams’ten gelen ham caption event’leri
- `transcript`
  final transcript satırları
- `meetingaudioasset`
  master ses dosyası ve türevleri
- `transcriptreviewitem`
  kullanıcı kararı bekleyen düzeltme önerileri

## Artifact dizinleri

Önemli runtime çıktıları:

- `bot/meeting_audio/meeting_<id>/master.webm`
- `bot/meeting_audio/meeting_<id>/master_16k_mono.wav`
- `bot/meeting_audio/meeting_<id>/teams_canonical.json`
- `bot/meeting_audio/meeting_<id>/whisperx_result.json`
- `bot/meeting_audio/meeting_<id>/alignment_map.json`
- `app/assets/meeting_audio/`
  UI üzerinden oynatılabilen meeting ses dosyaları
- `app/assets/review_audio_clips/`
  review kartlarındaki kısa ses klipleri
- `app/assets/live_meeting_frames/`
  canlı bot önizleme görselleri

Bu dizinler `.gitignore` içinde dışlanmıştır.

## Gereksinimler

- Python 3.11
- `reflex==0.8.27`
- `playwright>=1.40,<2`
- `whisperx>=3.3,<4`
- `ffmpeg`
- Playwright için Chromium
- Teams ses yakalama için tercihen Microsoft Edge (`msedge` channel)

Yerel makinede yazılabilir olması gereken dizinler:

- `app/`
- `bot/`
- `app/assets/live_meeting_frames/`
- `app/assets/meeting_audio/`
- `app/assets/review_audio_clips/`
- `bot/meeting_audio/`
- `bot/runtime_cache/`

## Lokal kurulum

Projeyi yerelde genelde `teams-bot` conda env’i ile çalıştırıyoruz.

### 1. Conda ortamını açın

```bash
conda activate teams-bot
```

### 2. Python bağımlılıklarını kurun

```bash
cd app
python -m pip install -r requirements.txt
```

### 3. Playwright tarayıcılarını kurun

```bash
python -m playwright install --with-deps chromium
```

Not:

- Bot önce `msedge` channel ile açmayı dener.
- `msedge` yoksa bundled Chromium fallback’i kullanır.
- Teams ses yakalama tarafı için sistemde Microsoft Edge bulunması daha sağlıklıdır.

### 4. Uygulamayı çalıştırın

```bash
cd app
reflex run --env prod --backend-host 0.0.0.0 --frontend-port 3000 --backend-port 8000
```

Varsayılan adresler:

- frontend: `http://localhost:3000`
- backend: `http://localhost:8000`

## Docker

İmaj oluşturma:

```bash
docker build -t notera .
```

Container çalıştırma:

```bash
docker run --rm -p 3000:3000 -p 8000:8000 -e API_URL=http://localhost:8000 notera
```

Not:

- Browser üzerinden erişilen backend adresi farklıysa `API_URL` buna göre verilmelidir.
- Docker içinde de `app/` ve `bot/` aynı dosya sistemi altında kalmalıdır.
- Docker image `ffmpeg` ile build edilir; container içinde ayrıca sistem kurulumu gerekmez.
- Docker image WhisperX runtime için gerekli cache dizinlerini de hazır oluşturur.
- Varsayılan build, WhisperX ASR modeli, Turkish alignment modeli ve pyannote VAD modelini image içindeki Hugging Face cache'e indirir.
- Bu yüzden Docker build süresi ve image boyutu belirgin şekilde artar.
- İsterseniz bunu kapatmak için `docker build --build-arg PRELOAD_WHISPERX_MODELS=0 -t notera .` kullanabilirsiniz.
- Hugging Face erişimi token gerektirirse `docker build --build-arg HF_TOKEN=... -t notera .` kullanabilirsiniz.
- GitHub Actions workflow'u [docker-image.yml](.github/workflows/docker-image.yml) ile image'ı `ghcr.io/<github-owner>/notera` adresine `main`, `v*` tag push'ları ve manuel tetikleme için build edip GHCR'a gönderir.
- Workflow publish için ekstra Docker kullanıcı/parolası istemez; GitHub Actions içindeki yerleşik `GITHUB_TOKEN` kullanılır. Model preload erişimi gerekiyorsa opsiyonel `HF_TOKEN` secret'ı eklenebilir.
- Anonim `docker pull` istiyorsanız ilk publish'ten sonra GitHub Packages içindeki container package görünürlüğünü bir kez `public` yapmanız gerekir.

## Hibrit transcript akışı

### 1. Teams caption toplama

Bot toplantı sırasında canlı caption event’lerini toplar.

Saklanan bilgiler:

- `speaker_name`
- caption text
- gözlemlenme zamanı
- slot index
- revision no

Bu veriler daha sonra canonical transcript üretmek için kullanılır.

### 2. Ses kaydı

Toplantı oluştururken ses kaydı açık veya kapalı olabilir.

Ses kaydı açıksa:

- bot audio track’leri toplamaya çalışır
- chunk’ları birleştirir
- master dosyayı `webm` olarak saklar
- postprocess sırasında `wav` türevi üretilir

Ses kaydı alınamazsa:

- toplantı yine transcript toplamaya devam eder
- `audio_status=failed` olur
- transcript akışı durmaz

### 3. Canonical Teams transcript

Worker, ham caption event’lerinden daha temiz bir Teams transcript çıkarır.

Bu aşamada:

- kısa ve gürültülü tekrarlar temizlenir
- aynı slot içindeki rephrase’ler birleştirilir
- rotasyon yapan caption parçaları normalize edilir

Çıktı:

- `teams_canonical.json`

### 4. WhisperX transcript

Ses kaydı varsa worker tüm dosyayı WhisperX ile transcript eder.

Mevcut davranış:

- varsayılan dil `tr`
- `WHISPERX_FORCE_LANGUAGE=1` ise dil auto-detect’e bırakılmaz
- model cache’te varsa local cache-only modda yüklenir

Bu özellikle kısa / gürültülü kayıtlarda İngilizceye yanlış kaymayı azaltmak için eklendi.

### 5. Alignment

Teams canonical ile WhisperX token akışları global olarak hizalanır.

Çıktı:

- `alignment_map.json`

Bu dosya review teşhisinde çok önemlidir:

- hangi utterance kaç token eşleşti
- coverage ne oldu
- Whisper önerisi hangi segmentlerden geldi

### 6. Final transcript ve review

Worker her canonical utterance için:

- Teams text
- Whisper suggestion
- coverage
- confidence
- whisper segment count

bilgilerini değerlendirir.

Karar mantığı:

- çok güvenli ise auto-apply
- yeterince kapsıyorsa pending review
- çok düşük coverage ise Teams text korunur

Ek not:

- çok segmentli veya düşük güvenli suggestion’lar artık `Kelime` diye gösterilmez
- bu tip örnekler `Cümle` review olarak sınıflandırılır
- review kartındaki Whisper önerisi artık ham Whisper segment metnini korur

## Export

Transcript sayfasından:

- `TXT`
- `CSV`

çıktısı alınabilir.

State tarafındaki ilgili aksiyonlar:

- `TranscriptPageState.download_txt`
- `TranscriptPageState.download_csv`

## Çalışma mantığı notları

- Speaker kaynağı her zaman Teams tarafıdır.
- WhisperX speaker diarization kullanılmaz.
- Review uygulanırsa `transcript.text` suggestion ile güncellenir.
- `teams_text` alanı her zaman referans olarak saklanır.
- Review reddedilirse mevcut transcript korunur.

## Ortam değişkenleri

Öne çıkan WhisperX ayarları:

- `WHISPERX_MODEL`
  varsayılan: `large-v3`
- `WHISPERX_MODEL_PATH`
  modeli local path’ten yüklemek için
- `WHISPERX_COMPUTE_TYPE`
  varsayılan: `int8`
- `WHISPERX_BATCH_SIZE`
  varsayılan: `8`
- `WHISPERX_LANGUAGE`
  varsayılan: `tr`
- `WHISPERX_FORCE_LANGUAGE`
  varsayılan: `1`

## Sık görülen sorunlar

### WhisperX çok yavaş

İlk nedenler:

- model cache’te değildir
- CPU üzerinde çalışıyordur
- alignment modeli için ağ retry’ı oluyordur

Mevcut kod cache varsa local-only yüklemeyi dener.

### Review kartındaki Whisper önerisi eksik görünüyorsa

Bu genelde segment metni yerine kırpılmış token-range suggestion gösterilmesinden kaynaklanırdı.

Güncel akışta:

- suggestion ham Whisper segment metninden üretilir
- çok segmentli örnekler daha temkinli sınıflandırılır

### `ffmpeg` bulunamadı

Sistemde `ffmpeg` kurulu olmalıdır.

Docker image kullanıyorsanız `ffmpeg` image içine dahil edilir; bu uyarı yerel kurulum için geçerlidir.

### `huggingface.co` erişim hatası

İlk model indirmesi için ağ gerekir.

Sonrasında:

- `bot/runtime_cache/huggingface/` altında cache oluşur
- cache varsa worker local-only modda açabilir

### Bot `msedge` ile açılmıyor

Bot önce Edge channel dener, olmazsa Chromium fallback’i kullanır.

Bu durumda ses yakalama davranışı değişebilir.

## Geliştirme notları

- UI tarafında güncel stil dosyası `app/assets/notera.css` içindedir.
- Eski `premium.css` kaldırılmıştır.
- Runtime artifact’ları git’e alınmaz.
- README ve `.gitignore` dışındaki büyük transcript/review akışı değişiklikleri tek commit halinde tutuldu.

## Hızlı özet

Bu proje artık sadece live caption toplayan bir bot değil:

- Teams caption event’lerini saklıyor
- ses kaydı alıyor
- WhisperX ile ikinci transcript üretiyor
- ikisini hizalıyor
- review kuyruğu üzerinden kullanıcıya kontrollü düzeltme akışı sunuyor

Yeni yapının omurgası:

- `meeting_runtime.py`
- `postprocess_worker.py`
- transcript/review UI ve state katmanı
