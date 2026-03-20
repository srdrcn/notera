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
