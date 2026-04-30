"""Testes: normalização de payload com envelope Evolution em /meta-new-lead."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.meta_lead_webhook import normalize_lead_events


class TestEvolutionEnvelope(unittest.TestCase):
    def test_unwraps_evolution_envelope_with_flat_lead_in_data(self) -> None:
        raw = {
            "apikey": "x",
            "event": "webhook",
            "instance": "inst",
            "server_url": "https://evo.example",
            "sender": "s",
            "destination": "d",
            "date_time": "2026-01-01",
            "data": {
                "nome": "Maria",
                "telefone": "5511999999999",
                "codi_id": "12345678901234567890123456789012",
            },
        }
        events = normalize_lead_events(raw)
        self.assertEqual(len(events), 1)
        self.assertIn("body", events[0])
        self.assertEqual(events[0]["body"].get("data", {}).get("nome"), "Maria")

    def test_unwraps_data_as_json_string(self) -> None:
        import json

        inner = {"nome": "João", "telefone": "5511888888888"}
        raw = {
            "event": "x",
            "instance": "y",
            "server_url": "u",
            "data": json.dumps(inner),
        }
        events = normalize_lead_events(raw)
        self.assertEqual(len(events), 1)

    def test_case_insensitive_keys_in_data_subdict(self) -> None:
        raw = {"data": {"NOME": "x", "TELEFONE": "y"}, "leadgenId": None}
        from execution.meta_lead_webhook import _is_meta_lead_body

        self.assertTrue(_is_meta_lead_body(raw))


if __name__ == "__main__":
    unittest.main()
