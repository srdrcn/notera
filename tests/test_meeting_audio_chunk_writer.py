from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.workers.bot import MeetingAudioChunkWriter


class MeetingAudioChunkWriterTests(unittest.TestCase):
    def test_finalize_keeps_aggregate_stream_when_pcm_decode_initially_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            audio_dir = temp_root / "meeting_audio"
            chunk_dir = audio_dir / "chunks"
            audio_dir.mkdir(parents=True, exist_ok=True)
            chunk_dir.mkdir(parents=True, exist_ok=True)
            aggregate_path = audio_dir / "recording.part"
            aggregate_path.write_bytes(b"aggregate-audio")
            master_path = audio_dir / "master.webm"
            pcm_path = audio_dir / "master_16k_mono.wav"

            writer = MeetingAudioChunkWriter.__new__(MeetingAudioChunkWriter)
            writer.meeting_id = 999
            writer.chunk_index = 0
            writer.chunk_paths = [chunk_dir / "chunk_00001.webm"]
            writer.mime_type = "audio/webm"
            writer.format = "webm"
            writer.accept_writes = False
            writer.chunk_dir = chunk_dir
            writer.aggregate_path = aggregate_path

            def fake_remux(target_path: Path) -> bool:
                target_path.write_bytes(aggregate_path.read_bytes())
                return True

            pcm_call_count = {"value": 0}

            def fake_build_pcm_copy(source_path: Path, output_path: Path) -> subprocess.CompletedProcess:
                pcm_call_count["value"] += 1
                if pcm_call_count["value"] == 1:
                    return subprocess.CompletedProcess(args=["ffmpeg"], returncode=1, stdout="", stderr="decode failed")
                output_path.write_bytes(b"pcm-audio")
                return subprocess.CompletedProcess(args=["ffmpeg"], returncode=0, stdout="", stderr="")

            with (
                patch("backend.workers.bot.get_meeting_master_audio_path", return_value=master_path),
                patch("backend.workers.bot.get_meeting_pcm_audio_path", return_value=pcm_path),
                patch.object(writer, "_finalize_from_aggregate_stream", side_effect=fake_remux),
                patch.object(writer, "_build_pcm_copy", side_effect=fake_build_pcm_copy) as build_pcm_copy,
                patch.object(writer, "_finalize_from_chunk_concat") as finalize_from_chunk_concat,
                patch("backend.workers.bot.probe_audio_duration_ms", return_value=1000),
            ):
                resolved_master, resolved_pcm, fmt, duration_ms = writer.finalize()

            self.assertEqual(resolved_master, master_path)
            self.assertEqual(resolved_pcm, pcm_path)
            self.assertEqual(fmt, "webm")
            self.assertEqual(duration_ms, 1000)
            self.assertTrue(master_path.exists())
            self.assertEqual(master_path.read_bytes(), aggregate_path.read_bytes())
            self.assertEqual(build_pcm_copy.call_count, 2)
            self.assertEqual(build_pcm_copy.call_args_list[0].args[0], master_path)
            self.assertEqual(build_pcm_copy.call_args_list[1].args[0], aggregate_path)
            finalize_from_chunk_concat.assert_not_called()


if __name__ == "__main__":
    unittest.main()
