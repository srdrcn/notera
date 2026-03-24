from __future__ import annotations

import unittest

from backend.runtime.participant_names import is_roster_heading_name, normalize_participant_name


class ParticipantNameTests(unittest.TestCase):
    def test_normalize_participant_name_collapses_whitespace(self) -> None:
        self.assertEqual(normalize_participant_name("  Serdar   Can  "), "Serdar Can")

    def test_roster_headings_are_filtered(self) -> None:
        self.assertTrue(is_roster_heading_name("In this meeting (2)"))
        self.assertTrue(is_roster_heading_name("Bu toplantıda (3)"))
        self.assertTrue(is_roster_heading_name("2 People"))

    def test_real_participant_names_are_kept(self) -> None:
        self.assertFalse(is_roster_heading_name("Serdar Can"))
        self.assertFalse(is_roster_heading_name("People Operations"))


if __name__ == "__main__":
    unittest.main()
