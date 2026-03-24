from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import shutil
import subprocess
from datetime import datetime

from backend.runtime.paths import (
    get_meeting_audio_chunks_dir,
    get_meeting_audio_dir,
    get_meeting_master_audio_path,
    get_meeting_pcm_audio_path,
)
from backend.runtime.logging import log_event
from backend.workers.bot_store import register_audio_source


logger = logging.getLogger("notera.worker.bot")
DEBUG_ARTIFACTS_ENABLED = os.getenv("NOTERA_DEBUG_ARTIFACTS", "1").strip().lower() in {"1", "true", "yes", "on"}
REMOTE_AUDIO_ATTRIBUTE = "data-notera-remote-audio"


def probe_audio_duration_ms(audio_path) -> int | None:
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=nk=1:nw=1",
                str(audio_path),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return None
        seconds = float((result.stdout or "").strip())
        if seconds <= 0:
            return None
        return int(seconds * 1000)
    except Exception:
        return None


class MeetingAudioChunkWriter:
    def __init__(self, meeting_id: int):
        self.meeting_id = int(meeting_id)
        self.chunk_index = 0
        self.chunk_paths = []
        self.mime_type = ""
        self.format = "webm"
        self.accept_writes = True
        self.chunk_dir = get_meeting_audio_chunks_dir(self.meeting_id)
        self.aggregate_path = get_meeting_audio_dir(self.meeting_id) / "recording.part"
        for stale_chunk in self.chunk_dir.glob("chunk_*"):
            stale_chunk.unlink(missing_ok=True)
        self.aggregate_path.unlink(missing_ok=True)

    def save_chunk(self, payload: dict[str, str]) -> bool:
        if not self.accept_writes:
            return False

        base64_data = payload.get("base64") or ""
        mime_type = (payload.get("mimeType") or "").lower()
        if not base64_data:
            return False

        raw_bytes = base64.b64decode(base64_data)
        if not raw_bytes:
            return False

        self.chunk_index += 1
        self.mime_type = mime_type or self.mime_type
        self.format = self._format_from_mime(self.mime_type)
        chunk_path = self.chunk_dir / f"chunk_{self.chunk_index:05d}.{self.format}"
        chunk_path.write_bytes(raw_bytes)
        self.chunk_paths.append(chunk_path)
        with self.aggregate_path.open("ab") as aggregate_file:
            aggregate_file.write(raw_bytes)
        logger.info(
            "Saved audio chunk for meeting %s: part #%s (%s bytes)",
            self.meeting_id,
            self.chunk_index,
            len(raw_bytes),
        )
        return True

    def finalize(self):
        master_path = get_meeting_master_audio_path(self.meeting_id, self.format)
        pcm_path = get_meeting_pcm_audio_path(self.meeting_id)
        finalized = False
        used_aggregate_stream = False

        if self._should_prefer_aggregate_stream() and self.aggregate_path.exists() and self.aggregate_path.stat().st_size > 0:
            finalized = self._finalize_from_aggregate_stream(master_path)
            if not finalized:
                shutil.copy2(self.aggregate_path, master_path)
                finalized = True
            used_aggregate_stream = finalized
        elif self.chunk_paths:
            finalized = self._finalize_from_chunk_concat(master_path)
        elif self.aggregate_path.exists() and self.aggregate_path.stat().st_size > 0:
            shutil.copy2(self.aggregate_path, master_path)
            finalized = True

        if not finalized:
            raise RuntimeError("no audio chunks were captured")

        pcm_result = self._build_pcm_copy(master_path, pcm_path)
        if pcm_result.returncode != 0 and used_aggregate_stream and self.aggregate_path.exists():
            logger.warning(
                "Could not decode remuxed aggregate audio stream for meeting %s (ffmpeg_return_code=%s). Retrying from aggregate stream.",
                self.meeting_id,
                pcm_result.returncode,
            )
            pcm_result = self._build_pcm_copy(self.aggregate_path, pcm_path)

        if pcm_result.returncode != 0 and self.chunk_paths and not used_aggregate_stream:
            if self._finalize_from_chunk_concat(master_path):
                pcm_result = self._build_pcm_copy(master_path, pcm_path)

        if pcm_result.returncode != 0:
            logger.warning(
                "Could not create PCM audio copy for meeting %s (ffmpeg_return_code=%s)",
                self.meeting_id,
                pcm_result.returncode,
            )

        duration_ms = probe_audio_duration_ms(pcm_path) if pcm_path.exists() else None
        if duration_ms is None:
            duration_ms = probe_audio_duration_ms(master_path)
        log_event(
            logger,
            logging.INFO,
            "audio.finalized",
            "Meeting audio finalized",
            duration_ms=duration_ms,
            has_pcm_copy=pcm_path.exists(),
            format=self.format,
        )
        if master_path.exists() and (pcm_path.exists() or duration_ms is not None):
            self._cleanup_temporary_audio_parts()
        return master_path, pcm_path if pcm_path.exists() else None, self.format, duration_ms

    def stop_accepting_writes(self) -> None:
        self.accept_writes = False

    def _should_prefer_aggregate_stream(self) -> bool:
        return self.format in {"webm", "ogg"}

    def _finalize_from_chunk_concat(self, master_path) -> bool:
        if not self.chunk_paths:
            return False
        concat_manifest_path = self.chunk_dir / "concat_inputs.txt"
        concat_manifest_path.write_text(
            "".join(f"file '{self._escape_concat_path(path)}'\n" for path in self.chunk_paths),
            encoding="utf-8",
        )
        concat_copy_result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_manifest_path),
                "-c",
                "copy",
                str(master_path),
            ],
            capture_output=True,
            text=True,
        )
        if concat_copy_result.returncode != 0:
            logger.warning(
                "Could not concat audio chunks with stream copy for meeting %s: %s",
                self.meeting_id,
                concat_copy_result.stderr.strip() or "ffmpeg concat copy failed",
            )
            concat_transcode_result = subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-f",
                    "concat",
                    "-safe",
                    "0",
                    "-i",
                    str(concat_manifest_path),
                    *self._master_transcode_args(),
                    str(master_path),
                ],
                capture_output=True,
                text=True,
            )
            if concat_transcode_result.returncode != 0:
                logger.warning(
                    "Could not concat audio chunks with transcode for meeting %s: %s",
                    self.meeting_id,
                    concat_transcode_result.stderr.strip() or "audio chunk concat failed",
                )
                return False
        return True

    def _finalize_from_aggregate_stream(self, master_path) -> bool:
        remux_result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(self.aggregate_path),
                "-c",
                "copy",
                str(master_path),
            ],
            capture_output=True,
            text=True,
        )
        if remux_result.returncode == 0:
            return True
        logger.warning(
            "Could not remux aggregate audio stream for meeting %s: %s",
            self.meeting_id,
            remux_result.stderr.strip() or "aggregate audio remux failed",
        )
        transcode_result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(self.aggregate_path),
                *self._master_transcode_args(),
                str(master_path),
            ],
            capture_output=True,
            text=True,
        )
        if transcode_result.returncode != 0:
            logger.warning(
                "Could not transcode aggregate audio stream for meeting %s: %s",
                self.meeting_id,
                transcode_result.stderr.strip() or "aggregate audio transcode failed",
            )
            return False
        return True

    @staticmethod
    def _build_pcm_copy(master_path, pcm_path):
        return subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(master_path),
                "-ac",
                "1",
                "-ar",
                "16000",
                str(pcm_path),
            ],
            capture_output=True,
            text=True,
        )

    def _cleanup_temporary_audio_parts(self) -> None:
        if DEBUG_ARTIFACTS_ENABLED:
            return
        for stale_chunk in self.chunk_dir.glob("chunk_*"):
            stale_chunk.unlink(missing_ok=True)
        (self.chunk_dir / "concat_inputs.txt").unlink(missing_ok=True)
        self.aggregate_path.unlink(missing_ok=True)

    def _master_transcode_args(self) -> list[str]:
        if self.format == "wav":
            return ["-vn", "-c:a", "pcm_s16le"]
        if self.format == "m4a":
            return ["-vn", "-c:a", "aac", "-b:a", "128k"]
        if self.format == "ogg":
            return ["-vn", "-c:a", "libopus"]
        return ["-vn", "-c:a", "libopus"]

    @staticmethod
    def _escape_concat_path(path) -> str:
        return str(path).replace("'", "'\\''")

    @staticmethod
    def _format_from_mime(mime_type: str) -> str:
        if "wav" in mime_type:
            return "wav"
        if "mp4" in mime_type or "m4a" in mime_type:
            return "m4a"
        if "ogg" in mime_type:
            return "ogg"
        return "webm"


async def install_teams_audio_hook(context) -> None:
    audio_attr = json.dumps(REMOTE_AUDIO_ATTRIBUTE)
    await context.add_init_script(
        f"""
        (() => {{
          if (window.__noteraTeamsAudioHookInstalled) return;
          const audioAttr = {audio_attr};
          const remoteAudioEntries = new Map();
          let hiddenContainer = null;

          const ensureContainer = () => {{
            if (hiddenContainer && document.body?.contains(hiddenContainer)) {{
              return hiddenContainer;
            }}
            hiddenContainer = document.createElement('div');
            hiddenContainer.setAttribute('data-notera-remote-audio-container', 'true');
            hiddenContainer.style.cssText = 'position:fixed;left:-9999px;top:-9999px;width:1px;height:1px;opacity:0;pointer-events:none;';
            (document.body || document.documentElement).appendChild(hiddenContainer);
            return hiddenContainer;
          }};

          const connectToRecorderIfReady = (entry) => {{
            const controller = window.__noteraRecorderController;
            if (controller && typeof controller.connectEntry === 'function') {{
              controller.connectEntry(entry);
            }}
          }};

          const attachAudioTrack = (track) => {{
            try {{
              if (!track || track.kind !== 'audio') return;
              const trackId = track.id || `audio-${{Date.now()}}-${{Math.random().toString(16).slice(2)}}`;
              if (remoteAudioEntries.has(trackId)) return;

              const stream = new MediaStream([track]);
              const audioEl = document.createElement('audio');
              audioEl.autoplay = true;
              audioEl.playsInline = true;
              audioEl.controls = false;
              audioEl.muted = true;
              audioEl.srcObject = stream;
              audioEl.setAttribute(audioAttr, 'true');
              audioEl.dataset.noteraRemoteTrackId = trackId;
              audioEl.style.cssText = 'position:absolute;width:1px;height:1px;opacity:0;pointer-events:none;';
              ensureContainer().appendChild(audioEl);
              const entry = {{ trackId, track, stream, streamId: stream.id || '', audioEl }};
              remoteAudioEntries.set(trackId, entry);
              connectToRecorderIfReady(entry);
              if (typeof window.__noteraRegisterAudioSource === 'function') {{
                Promise.resolve(window.__noteraRegisterAudioSource({{
                  source_key: `webrtc:track:${{trackId}}`,
                  source_kind: 'webrtc_remote_track',
                  track_id: trackId,
                  stream_id: entry.streamId || '',
                  format: 'webm',
                  status: 'recording',
                }})).catch((error) => {{
                  console.warn('[Notera] Failed registering audio source', error);
                }});
              }}

              const cleanup = () => {{
                remoteAudioEntries.delete(trackId);
                const controller = window.__noteraRecorderController;
                if (controller?.sources?.has(trackId)) {{
                  const sourceBundle = controller.sources.get(trackId);
                  try {{
                    sourceBundle?.sourceNode?.disconnect();
                  }} catch (error) {{
                    console.warn('[Notera] Failed disconnecting source node', error);
                  }}
                  try {{
                    sourceBundle?.gainNode?.disconnect();
                  }} catch (error) {{
                    console.warn('[Notera] Failed disconnecting gain node', error);
                  }}
                  controller.sources.delete(trackId);
                  controller.sourceCount = controller.sources.size;
                }}
                audioEl.remove();
              }};
              track.addEventListener('ended', cleanup, {{ once: true }});
            }} catch (error) {{
              console.warn('[Notera] Failed attaching Teams audio track', error);
            }}
          }};

          const NativePC = window.RTCPeerConnection || window.webkitRTCPeerConnection;
          if (!NativePC) return;

          const patchInstance = (pc) => {{
            pc.addEventListener('track', (event) => {{
              try {{
                if (event.track && event.track.kind === 'audio') {{
                  attachAudioTrack(event.track);
                }}
                if (Array.isArray(event.streams)) {{
                  for (const stream of event.streams) {{
                    for (const streamTrack of stream.getAudioTracks()) {{
                      attachAudioTrack(streamTrack);
                    }}
                  }}
                }}
              }} catch (error) {{
                console.warn('[Notera] Failed processing Teams ontrack event', error);
              }}
            }});
          }};

          class PatchedRTCPeerConnection extends NativePC {{
            constructor(...args) {{
              super(...args);
              patchInstance(this);
            }}
          }}

          PatchedRTCPeerConnection.prototype = NativePC.prototype;
          Object.setPrototypeOf(PatchedRTCPeerConnection, NativePC);
          window.RTCPeerConnection = PatchedRTCPeerConnection;
          if (window.webkitRTCPeerConnection) {{
            window.webkitRTCPeerConnection = PatchedRTCPeerConnection;
          }}

          window.__noteraRemoteAudioInfo = () => {{
            return {{
              count: remoteAudioEntries.size,
              activeRecorderSources: window.__noteraRecorderController?.sourceCount || 0,
            }};
          }};

          window.__noteraStartAudioRecorder = async () => {{
            const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
            if (!AudioContextCtor) {{
              return {{ ok: false, error: 'AudioContext unavailable' }};
            }}
            if (!window.MediaRecorder) {{
              return {{ ok: false, error: 'MediaRecorder unavailable' }};
            }}

            const existingController = window.__noteraRecorderController;
            if (existingController?.recorder?.state === 'recording') {{
              return {{
                ok: true,
                mimeType: existingController.mimeType || '',
                sourceCount: existingController.sourceCount || 0,
              }};
            }}

            const audioContext = new AudioContextCtor();
            try {{
              if (audioContext.state === 'suspended') {{
                await audioContext.resume();
              }}
            }} catch (error) {{
              console.warn('[Notera] Failed resuming AudioContext', error);
            }}

            const destination = audioContext.createMediaStreamDestination();
            const controller = {{
              audioContext,
              destination,
              recorder: null,
              mimeType: '',
              sourceCount: 0,
              sources: new Map(),
              pendingUploads: new Set(),
              connectEntry: async (entry) => {{
                if (!entry?.trackId || !entry.stream || controller.sources.has(entry.trackId)) return;
                try {{
                  const sourceNode = audioContext.createMediaStreamSource(entry.stream);
                  const gainNode = audioContext.createGain();
                  gainNode.gain.value = 1.0;
                  sourceNode.connect(gainNode);
                  gainNode.connect(destination);
                  controller.sources.set(entry.trackId, {{ sourceNode, gainNode }});
                  controller.sourceCount = controller.sources.size;
                }} catch (error) {{
                  console.warn('[Notera] Failed connecting remote audio stream', error);
                }}
              }},
            }};

            window.__noteraRecorderController = controller;

            const audioEntries = Array.from(remoteAudioEntries.values());
            for (const entry of audioEntries) {{
              if (typeof window.__noteraRegisterAudioSource === 'function') {{
                try {{
                  await window.__noteraRegisterAudioSource({{
                    source_key: `webrtc:track:${{entry.trackId}}`,
                    source_kind: 'webrtc_remote_track',
                    track_id: entry.trackId,
                    stream_id: entry.streamId || '',
                    format: 'webm',
                    status: 'recording',
                  }});
                }} catch (error) {{
                  console.warn('[Notera] Failed backfilling audio source registration', error);
                }}
              }}
              await controller.connectEntry(entry);
            }}

            if (!controller.sourceCount) {{
              return {{ ok: false, error: 'No remote audio tracks available' }};
            }}

            const mimeCandidates = ['audio/webm;codecs=opus', 'audio/webm', 'audio/mp4'];
            const mimeType = mimeCandidates.find((candidate) => MediaRecorder.isTypeSupported(candidate)) || '';
            let recorder;
            try {{
              recorder = mimeType
                ? new MediaRecorder(destination.stream, {{ mimeType }})
                : new MediaRecorder(destination.stream);
            }} catch (error) {{
              return {{ ok: false, error: String(error) }};
            }}
            controller.recorder = recorder;
            controller.mimeType = recorder.mimeType || mimeType || '';

            recorder.ondataavailable = async (event) => {{
              let uploadPromise = null;
              try {{
                if (!event.data || !event.data.size || typeof window.__noteraSaveAudioChunk !== 'function') return;
                uploadPromise = (async () => {{
                  const arrayBuffer = await event.data.arrayBuffer();
                  const bytes = new Uint8Array(arrayBuffer);
                  let binary = '';
                  const chunkSize = 0x8000;
                  for (let offset = 0; offset < bytes.length; offset += chunkSize) {{
                    const slice = bytes.subarray(offset, offset + chunkSize);
                    binary += String.fromCharCode(...slice);
                  }}
                  await window.__noteraSaveAudioChunk({{
                    base64: btoa(binary),
                    mimeType: event.data.type || controller.mimeType,
                  }});
                }})();
                controller.pendingUploads.add(uploadPromise);
                await uploadPromise;
              }} catch (error) {{
                console.warn('[Notera] Failed persisting recorder chunk', error);
              }} finally {{
                if (uploadPromise) {{
                  controller.pendingUploads.delete(uploadPromise);
                }}
              }}
            }};

            recorder.start(5000);
            return {{
              ok: true,
              mimeType: controller.mimeType,
              sourceCount: controller.sourceCount,
            }};
          }};

          window.__noteraPrepareAudioRecorderShutdown = async () => {{
            return {{ ok: true }};
          }};

          window.__noteraStopAudioRecorder = async () => {{
            const controller = window.__noteraRecorderController;
            if (!controller?.recorder) {{
              return {{ ok: false, error: 'Recorder not initialized' }};
            }}
            if (controller.recorder.state === 'inactive') {{
              return {{ ok: true }};
            }}
            await new Promise((resolve) => {{
              controller.recorder.addEventListener('stop', resolve, {{ once: true }});
              controller.recorder.stop();
            }});
            if (controller.pendingUploads.size) {{
              await Promise.allSettled(Array.from(controller.pendingUploads));
            }}
            return {{ ok: true }};
          }};

          window.__noteraTeamsAudioHookInstalled = true;
        }})();
        """
    )


async def get_remote_audio_info(page) -> dict[str, int]:
    try:
        return await page.evaluate(
            "() => window.__noteraRemoteAudioInfo ? window.__noteraRemoteAudioInfo() : ({count: 0, activeRecorderSources: 0})"
        )
    except Exception as exc:
        logger.debug("Could not read remote audio info: %s", exc)
        return {"count": 0, "activeRecorderSources": 0}


async def start_browser_audio_capture(page, chunk_writer: MeetingAudioChunkWriter, meeting_id: int):
    try:
        await page.expose_function("__noteraSaveAudioChunk", chunk_writer.save_chunk)
    except Exception as exc:
        if "__noteraSaveAudioChunk" not in str(exc):
            raise

    try:
        await page.expose_function(
            "__noteraRegisterAudioSource",
            lambda payload: register_audio_source(
                meeting_id,
                (payload or {}).get("source_key") or f"webrtc:track:{datetime.utcnow().timestamp()}",
                (payload or {}).get("source_kind") or "webrtc_remote_track",
                track_id=(payload or {}).get("track_id"),
                stream_id=(payload or {}).get("stream_id"),
                fmt=(payload or {}).get("format"),
                status=(payload or {}).get("status") or "recording",
            ),
        )
    except Exception as exc:
        if "__noteraRegisterAudioSource" not in str(exc):
            raise

    deadline = asyncio.get_running_loop().time() + 30
    last_error = "Toplantı ses akışı bulunamadı."
    last_info = {"count": 0, "activeRecorderSources": 0}

    while asyncio.get_running_loop().time() < deadline:
        last_info = await get_remote_audio_info(page)
        if last_info.get("count", 0) > 0:
            result = await page.evaluate(
                "() => window.__noteraStartAudioRecorder ? window.__noteraStartAudioRecorder() : ({ok: false, error: 'Audio recorder bridge unavailable'})"
            )
            if result.get("ok"):
                return True, result, None
            last_error = result.get("error") or last_error
        await asyncio.sleep(1)

    return False, last_info, last_error


async def stop_browser_audio_capture(page):
    try:
        await page.evaluate(
            "() => window.__noteraPrepareAudioRecorderShutdown ? window.__noteraPrepareAudioRecorderShutdown() : ({ok: true})"
        )
    except Exception:
        pass

    try:
        return await page.evaluate(
            "() => window.__noteraStopAudioRecorder ? window.__noteraStopAudioRecorder() : ({ok: false, error: 'Audio recorder bridge unavailable'})"
        )
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
