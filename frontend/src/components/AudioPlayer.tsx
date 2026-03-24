import {
  forwardRef,
  useEffect,
  useRef,
  useState,
  type AudioHTMLAttributes,
  type ChangeEvent,
  type CSSProperties,
} from "react";


type AudioPlayerProps = Omit<AudioHTMLAttributes<HTMLAudioElement>, "controls"> & {
  compact?: boolean;
};


const PLAYBACK_RATES = [1, 1.25, 1.5, 2];


function formatTime(seconds: number) {
  if (!Number.isFinite(seconds) || seconds < 0) {
    return "00:00";
  }

  const totalSeconds = Math.floor(seconds);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const remainingSeconds = totalSeconds % 60;

  if (hours > 0) {
    return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${String(remainingSeconds).padStart(2, "0")}`;
  }

  return `${String(minutes).padStart(2, "0")}:${String(remainingSeconds).padStart(2, "0")}`;
}


function formatPlaybackRate(rate: number) {
  return Number.isInteger(rate) ? `${rate.toFixed(0)}x` : `${rate.toFixed(2).replace(/0$/, "")}x`;
}


function PlayIcon() {
  return (
    <svg aria-hidden="true" className="nt-audio-player-icon nt-audio-player-icon-play" viewBox="0 0 24 24">
      <path d="M8 6.5v11l9-5.5z" fill="currentColor" />
    </svg>
  );
}


function PauseIcon() {
  return (
    <svg aria-hidden="true" className="nt-audio-player-icon" viewBox="0 0 24 24">
      <path d="M8 6h3v12H8zM13 6h3v12h-3z" fill="currentColor" />
    </svg>
  );
}


function ReplayIcon() {
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M3 12a9 9 0 1 0 3-6.7" />
      <path d="M3 4v4h4" />
      <path d="M9.2 9.6h-1.9l-.9 1.6h1.5v3.4h1.3z" fill="currentColor" stroke="none" />
    </svg>
  );
}


function SkipBackIcon() {
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M11 19l-7-7 7-7" />
      <path d="M20 19l-7-7 7-7" />
    </svg>
  );
}


function SkipForwardIcon() {
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M13 5l7 7-7 7" />
      <path d="M4 5l7 7-7 7" />
    </svg>
  );
}


export const AudioPlayer = forwardRef<HTMLAudioElement, AudioPlayerProps>(function AudioPlayer(
  {
    compact = false,
    className,
    onDurationChange,
    onEnded,
    onLoadedMetadata,
    onPause,
    onPlay,
    onRateChange,
    onSeeked,
    onTimeUpdate,
    preload = "metadata",
    src,
    ...audioProps
  },
  forwardedRef,
) {
  const audioElementRef = useRef<HTMLAudioElement | null>(null);
  const [isPlaying, setIsPlaying] = useState(false);
  const [currentTime, setCurrentTime] = useState(0);
  const [duration, setDuration] = useState(0);
  const [playbackRate, setPlaybackRate] = useState(1);

  useEffect(() => {
    setIsPlaying(false);
    setCurrentTime(0);
    setDuration(0);
    setPlaybackRate(1);
    if (audioElementRef.current) {
      audioElementRef.current.playbackRate = 1;
    }
  }, [src]);

  const syncFromElement = (element: HTMLAudioElement) => {
    setCurrentTime(element.currentTime);
    setDuration(Number.isFinite(element.duration) && element.duration > 0 ? element.duration : 0);
    setPlaybackRate(element.playbackRate || 1);
    setIsPlaying(!element.paused && !element.ended);
  };

  const setAudioRef = (node: HTMLAudioElement | null) => {
    audioElementRef.current = node;

    if (typeof forwardedRef === "function") {
      forwardedRef(node);
      return;
    }

    if (forwardedRef) {
      forwardedRef.current = node;
    }
  };

  const togglePlayback = () => {
    const audioElement = audioElementRef.current;
    if (!audioElement) {
      return;
    }

    if (audioElement.paused || audioElement.ended) {
      void audioElement.play().catch(() => {
        setIsPlaying(false);
      });
      return;
    }

    audioElement.pause();
  };

  const seekTo = (timeSec: number) => {
    const audioElement = audioElementRef.current;
    if (!audioElement) {
      return;
    }

    const nextTime = Math.max(0, Math.min(duration || audioElement.duration || timeSec, timeSec));
    audioElement.currentTime = nextTime;
    syncFromElement(audioElement);
  };

  const handleScrub = (event: ChangeEvent<HTMLInputElement>) => {
    seekTo(Number(event.target.value));
  };

  const skipBy = (deltaSec: number) => {
    const audioElement = audioElementRef.current;
    if (!audioElement) {
      return;
    }

    seekTo(audioElement.currentTime + deltaSec);
  };

  const cyclePlaybackRate = () => {
    const audioElement = audioElementRef.current;
    if (!audioElement) {
      return;
    }

    const currentIndex = PLAYBACK_RATES.findIndex((rate) => Math.abs(rate - playbackRate) < 0.01);
    const nextRate = PLAYBACK_RATES[(currentIndex + 1) % PLAYBACK_RATES.length];
    audioElement.playbackRate = nextRate;
    setPlaybackRate(nextRate);
  };

  const progressPercent = duration > 0 ? Math.min(100, (currentTime / duration) * 100) : 0;
  const playerStateLabel = isPlaying ? "Şimdi çalıyor" : currentTime > 0 ? "Duraklatıldı" : "Dinlemeye hazır";
  const playerClassName = ["nt-audio-player-shell", compact ? "is-compact" : "", className ?? ""].filter(Boolean).join(" ");
  const mainClassName = ["nt-audio-player-main", compact ? "is-compact" : ""].filter(Boolean).join(" ");

  return (
    <div
      className={playerClassName}
      style={{ "--nt-audio-progress": `${progressPercent}%` } as CSSProperties}
    >
      <audio
        {...audioProps}
        ref={setAudioRef}
        className="nt-audio-native"
        controls={false}
        preload={preload}
        src={src}
        onLoadedMetadata={(event) => {
          syncFromElement(event.currentTarget);
          onLoadedMetadata?.(event);
        }}
        onDurationChange={(event) => {
          syncFromElement(event.currentTarget);
          onDurationChange?.(event);
        }}
        onPlay={(event) => {
          syncFromElement(event.currentTarget);
          onPlay?.(event);
        }}
        onPause={(event) => {
          syncFromElement(event.currentTarget);
          onPause?.(event);
        }}
        onEnded={(event) => {
          syncFromElement(event.currentTarget);
          onEnded?.(event);
        }}
        onSeeked={(event) => {
          syncFromElement(event.currentTarget);
          onSeeked?.(event);
        }}
        onTimeUpdate={(event) => {
          syncFromElement(event.currentTarget);
          onTimeUpdate?.(event);
        }}
        onRateChange={(event) => {
          syncFromElement(event.currentTarget);
          onRateChange?.(event);
        }}
      />

      <div className="nt-audio-player-top">
        <div className="nt-audio-player-status">
          <span className={`nt-audio-player-status-dot ${isPlaying ? "is-playing" : ""}`} />
          <div className="nt-audio-player-status-copy">
            <strong>{playerStateLabel}</strong>
            <span>{duration > 0 ? `${formatTime(currentTime)} / ${formatTime(duration)}` : "Süre hazırlanıyor"}</span>
          </div>
        </div>

        <div className="nt-audio-player-top-actions">
          <button
            className="nt-audio-player-rate"
            onClick={cyclePlaybackRate}
            type="button"
          >
            {formatPlaybackRate(playbackRate)}
          </button>

          <button
            className="nt-audio-player-chip nt-audio-player-chip-replay"
            onClick={() => seekTo(0)}
            type="button"
          >
            <ReplayIcon />
            <span>Başa dön</span>
          </button>
        </div>
      </div>

      <div className={mainClassName}>
        {!compact ? (
          <button
            aria-label="10 saniye geri sar"
            className="nt-audio-player-side-btn"
            onClick={() => skipBy(-10)}
            type="button"
          >
            <SkipBackIcon />
            <span>-10</span>
          </button>
        ) : null}

        <button
          aria-label={isPlaying ? "Duraklat" : "Oynat"}
          className="nt-audio-player-toggle"
          onClick={togglePlayback}
          type="button"
        >
          {isPlaying ? <PauseIcon /> : <PlayIcon />}
        </button>

        {!compact ? (
          <button
            aria-label="10 saniye ileri sar"
            className="nt-audio-player-side-btn"
            onClick={() => skipBy(10)}
            type="button"
          >
            <span>+10</span>
            <SkipForwardIcon />
          </button>
        ) : null}

        {compact ? (
          <>
            <input
              aria-label="Oynatma konumu"
              className="nt-audio-player-scrubber"
              max={duration || 0}
              min={0}
              onChange={handleScrub}
              step={0.1}
              type="range"
              value={duration > 0 ? Math.min(currentTime, duration) : 0}
            />
            <div className="nt-audio-player-times">
              <span>{formatTime(currentTime)}</span>
              <span>{formatTime(duration)}</span>
            </div>
          </>
        ) : (
          <div className="nt-audio-player-progress-block">
            <input
              aria-label="Oynatma konumu"
              className="nt-audio-player-scrubber"
              max={duration || 0}
              min={0}
              onChange={handleScrub}
              step={0.1}
              type="range"
              value={duration > 0 ? Math.min(currentTime, duration) : 0}
            />
            <div className="nt-audio-player-times">
              <span>{formatTime(currentTime)}</span>
              <span>{formatTime(duration)}</span>
            </div>
          </div>
        )}
      </div>

      {!compact ? (
        <div className="nt-audio-player-bottom">
          <div className="nt-audio-player-bars" aria-hidden="true">
            <span />
            <span />
            <span />
            <span />
            <span />
            <span />
            <span />
          </div>
        </div>
      ) : null}
    </div>
  );
});
