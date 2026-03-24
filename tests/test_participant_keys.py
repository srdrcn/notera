from __future__ import annotations

import unittest

from backend.workers.bot import (
    extract_participant_key,
    is_unstable_stable_key,
    participant_identity_conflicts,
)


class ParticipantKeyTests(unittest.TestCase):
    def test_extract_participant_key_falls_back_to_name_for_positional_stable_key(self) -> None:
        key = extract_participant_key(
            {
                "display_name": "Serdar Can",
                "stable_key": "2",
                "platform_identity": "",
            }
        )
        self.assertTrue(key.startswith("teams-name:serdar can:"))

    def test_extract_participant_key_prefers_platform_identity(self) -> None:
        key = extract_participant_key(
            {
                "display_name": "Serdar Can",
                "stable_key": "participant-2",
                "platform_identity": "8:orgid:12345",
            }
        )
        self.assertEqual(key, "teams-platform:8:orgid:12345")

    def test_unstable_stable_keys_are_rejected(self) -> None:
        self.assertTrue(is_unstable_stable_key("2"))
        self.assertTrue(is_unstable_stable_key("participant-2"))
        self.assertFalse(is_unstable_stable_key("teams-person-abc"))

    def test_identity_conflict_requires_different_names_without_shared_platform_identity(self) -> None:
        self.assertTrue(participant_identity_conflicts("serdar can", "", "güseyin", ""))
        self.assertFalse(participant_identity_conflicts("serdar can", "8:orgid:1", "güseyin", "8:orgid:1"))


if __name__ == "__main__":
    unittest.main()
