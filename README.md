# Notera

Notera, Microsoft Teams toplantılarını bot ile izleyip canlı caption toplamak, mümkünse ses kaydı almak, toplantı bittikten sonra WhisperX ile ikinci bir transcript üretmek ve final metni review akışıyla netleştirmek için hazırlanmış bir uygulamadır.

Uygulama iki ana parçadan oluşur:

- `app/`: Reflex tabanlı web uygulaması, kullanıcı oturumu, dashboard, transcript ekranı ve SQLite veritabanı
- `bot/`: Playwright ile Teams toplantısına katılan bot, canlı caption toplama, ses kaydı ve toplantı sonrası postprocess worker

## Ne Yapar

- Teams toplantı linki üzerinden bot oturumu başlatır
- Canlı caption event’lerini ham haliyle saklar
- Uzak ses track’i alınabiliyorsa master ses kaydı üretir
- Ham caption akışından canonical Teams transcript çıkarır
- Ses kaydı varsa WhisperX ile ikinci transcript üretir
- Teams ve WhisperX çıktısını hizalayıp final transcript oluşturur
- Düşük güvenli farkları review akışına bırakır
- Final transcript’i `TXT` ve `CSV` olarak dışa aktarır

## Kullanıcı Akışı

Bir kullanıcı uygulamayı şu sırayla kullanır:

1. Giriş ekranında e-posta ile kayıt olur veya giriş yapar.
2. Dashboard üzerinden toplantı adı, Teams linki ve isteğe bağlı ses kaydı tercihiyle yeni toplantı oluşturur.
3. Uygulama ilgili toplantı için bot sürecini başlatır.
4. Bot toplantıya katılır, caption toplamaya başlar ve uygunsa ses kaydı alır.
5. Toplantı tamamlandığında postprocess worker otomatik devreye girer.
6. Transcript ekranında final metin, review önerileri, meeting ses kaydı ve bot önizleme görüntüleri görülebilir.
7. Gerekirse review kararları verilir, duplicate transcript satırları birleştirilir ve çıktı dışa aktarılır.

## Sistem Nasıl Çalışır

### 1. Canlı caption toplama

Bot Teams arayüzündeki caption yüzeyini izler ve her caption event’i için şu verileri saklar:

- konuşmacı adı
- caption metni
- gözlemlenme zamanı
- slot index
- revision numarası

Bu ham kayıtlar `teamscaptionevent` tablosunda tutulur.

### 2. Ses kaydı

Toplantı oluştururken ses kaydı açıksa bot uzak ses track’lerini yakalamaya çalışır. Başarılı olursa:

- parça parça chunk toplar
- master ses dosyasını üretir
- 16 kHz mono PCM kopyası oluşturur

Ses yakalama başarısız olsa bile caption toplama akışı devam eder.

### 3. Canonical Teams transcript

Toplantı bittikten sonra worker ham caption event’lerinden daha temiz bir Teams transcript çıkarır. Bu aşamada:

- kısa ve gürültülü tekrarlar ayıklanır
- aynı slot içindeki revizyonlar birleştirilir
- rotasyon yapan parçalı caption akışı normalize edilir

### 4. WhisperX transcript

Ses kaydı varsa worker aynı ses dosyasını WhisperX ile yeniden transcript eder.

Mevcut varsayılanlar:

- model: `large-v3`
- compute type: `int8`
- varsayılan dil: `tr`
- `WHISPERX_FORCE_LANGUAGE=1` ile dil zorlaması açık

Docker image varsayılan olarak açık erişimli ASR ve Turkish alignment modellerini preload eder. Bu projedeki pyannote VAD, ekstra `pyannote/segmentation` repo preload’u istemez; WhisperX paketindeki gömülü asset’i kullanır.

### 5. Alignment ve final transcript

Worker canonical Teams transcript ile WhisperX segmentlerini hizalar. Bu hizalama sonucunda:

- güvenli eşleşmeler otomatik uygulanır
- kararsız eşleşmeler `transcriptreviewitem` olarak review kuyruğuna bırakılır
- final transcript satırları `transcript` tablosuna yazılır

### 6. Review

Transcript ekranında review gereken satırlar vurgulu görünür. Kullanıcı:

- `Uygula` ile WhisperX önerisini final transcript’e yazabilir
- `Koru` ile mevcut Teams caption’ını koruyabilir
- tüm review önerilerini toplu uygulayabilir
- review sonrası kalan duplicate transcript satırlarını tek aksiyonla birleştirebilir

## Veri Modeli

Öne çıkan tablolar:

- `user`: e-posta bazlı kullanıcı kaydı
- `meeting`: toplantı meta verisi, bot durumu, ses ve postprocess durumları
- `teamscaptionevent`: ham canlı caption kayıtları
- `meetingaudioasset`: master ses kaydı ve türevleri
- `transcript`: final transcript satırları
- `transcriptreviewitem`: review gerektiren öneriler

Şema runtime sırasında bootstrap edilir. Uygulama sıfırdan ayağa kalktığında gerekli tablolar ve eksik kolonlar otomatik oluşturulur; ayrıca migration komutu gerekmez.

## Kalıcı Dosyalar

Toplantı sırasında ve sonrasında şu artefact’lar üretilir:

- `app/reflex.db`
- `bot/meeting_audio/meeting_<id>/master.webm`
- `bot/meeting_audio/meeting_<id>/master_16k_mono.wav`
- `bot/meeting_audio/meeting_<id>/teams_canonical.json`
- `bot/meeting_audio/meeting_<id>/whisperx_result.json`
- `bot/meeting_audio/meeting_<id>/alignment_map.json`
- `app/assets/meeting_audio/`
- `app/assets/review_audio_clips/`
- `app/assets/live_meeting_frames/`

Bu dosyalar uygulamanın çalışma verisidir; prod kullanımda volume ile kalıcı tutulmaları gerekir.

## Prod Kurulum

Önerilen dağıtım modeli Docker image kullanmaktır.

### Image

Docker Hub image adı:

- `chosenwar/notera:latest`

Image hem `linux/amd64` hem `linux/arm64` için yayınlanır; Intel/AMD Linux host'larda ve Apple Silicon tabanlı Docker kurulumlarında aynı tag kullanılabilir.

### Gerekli Portlar

- `3000`: web arayüzü
- `8000`: Reflex backend API

### Önerilen Kalıcı Dizin Yapısı

Host üzerinde şu dizinleri oluşturun:

```bash
mkdir -p docker-data/app-assets/live_meeting_frames
mkdir -p docker-data/app-assets/meeting_audio
mkdir -p docker-data/app-assets/review_audio_clips
mkdir -p docker-data/bot/meeting_audio
mkdir -p docker-data/bot/runtime_cache
touch docker-data/reflex.db
```

### Çalıştırma

```bash
docker run -d \
  --name notera \
  -p 3000:3000 \
  -p 8000:8000 \
  -e API_URL=http://localhost:8000 \
  -v "$(pwd)/docker-data/reflex.db:/srv/notera/app/reflex.db" \
  -v "$(pwd)/docker-data/app-assets/live_meeting_frames:/srv/notera/app/assets/live_meeting_frames" \
  -v "$(pwd)/docker-data/app-assets/meeting_audio:/srv/notera/app/assets/meeting_audio" \
  -v "$(pwd)/docker-data/app-assets/review_audio_clips:/srv/notera/app/assets/review_audio_clips" \
  -v "$(pwd)/docker-data/bot/meeting_audio:/srv/notera/bot/meeting_audio" \
  -v "$(pwd)/docker-data/bot/runtime_cache:/srv/notera/bot/runtime_cache" \
  chosenwar/notera:latest
```

Ardından arayüz:

- `http://localhost:3000`

### Reverse Proxy Notu

Frontend ile backend farklı alan adı veya proxy arkasında sunuluyorsa `API_URL` veya `REFLEX_API_URL` doğru backend adresine göre verilmelidir.

## Uygulamada İlk Kullanım

1. Uygulamayı açın.
2. E-posta adresiyle kayıt olun.
3. Dashboard’da yeni toplantı oluşturun.
4. Teams link’ini girin.
5. Ses kaydı gerekiyorsa açık bırakın.
6. Botun toplantıya katılmasını bekleyin.
7. Toplantı tamamlanınca transcript ekranından çıktıları ve review akışını yönetin.

## Sınırlamalar ve Operasyonel Notlar

- Kimlik doğrulama şu an yalnızca e-posta tabanlı hafif bir akıştır; parola, rol yönetimi veya SSO yoktur.
- Veritabanı SQLite’tır; tek node / düşük-orta ölçekli kullanım için uygundur.
- Her canlı toplantı ayrı bot süreci açar; yoğun eşzamanlı kullanımda CPU ve RAM yükünü asıl bot + WhisperX tarafı belirler.
- WhisperX CPU üzerinde çalıştığı için uzun kayıtlar ve eşzamanlı postprocess yükü pahalıdır.
- Speaker kaynağı her zaman Teams caption tarafıdır; WhisperX diarization kullanılmaz.
- Review reddedildiğinde `teams_text` korunur, review uygulandığında `transcript.text` güncellenir.
- Ses yakalama Teams arayüzü ve tarayıcı davranışına bağlıdır; bazı toplantılarda caption varken audio capture başarısız olabilir.

## Repo Yapısı

Uygulamanın ana dosyaları:

- `app/app/app.py`: sayfa bileşenleri ve UI yerleşimleri
- `app/app/state.py`: dashboard, transcript ekranı ve aksiyon state mantığı
- `app/app/models.py`: SQLModel tabloları
- `app/app/meeting_runtime.py`: runtime schema bootstrap ve artifact path helper’ları
- `bot/bot.py`: Teams bot süreci
- `bot/postprocess_worker.py`: canonical transcript, WhisperX, alignment ve review üretimi
- `app/assets/notera.css`: arayüz stilleri

## Özet

Notera bir “caption viewer” değil; toplantıyı izleyen, ham caption ve ses verisini toplayan, toplantı sonrası ikinci transcript üreten, bu iki kaynağı birleştirip review akışıyla final metni netleştiren bir operasyon uygulamasıdır.
