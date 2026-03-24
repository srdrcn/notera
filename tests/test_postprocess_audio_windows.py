from __future__ import annotations

import tempfile
import unittest
import wave
from pathlib import Path

from backend.workers.postprocess_worker import probe_wav_duration_ms, trim_audio_window


def write_silence(path: Path, duration_ms: int, sample_rate: int = 16000) -> None:
    frame_count = int(sample_rate * (duration_ms / 1000))
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(b"\x00\x00" * frame_count)


class PostprocessAudioWindowTests(unittest.TestCase):
    def test_probe_wav_duration_ms_reads_wave_length(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            audio_path = Path(temp_dir) / "sample.wav"
            write_silence(audio_path, duration_ms=1000)
            self.assertEqual(probe_wav_duration_ms(audio_path), 1000)

    def test_trim_audio_window_rejects_offsets_beyond_source_duration(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source_path = Path(temp_dir) / "source.wav"
            output_path = Path(temp_dir) / "clip.wav"
            write_silence(source_path, duration_ms=1000)
            self.assertFalse(trim_audio_window(source_path, output_path, start_offset_ms=1500, end_offset_ms=2500))
            self.assertFalse(output_path.exists())


if __name__ == "__main__":
    unittest.main()
