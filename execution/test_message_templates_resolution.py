"""Testes: resolução de variáveis e variáveis personalizadas de mensagem."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from execution.message_templates import (
    apply_custom_variables,
    custom_variables_storage_channel,
    get_effective_source_keys,
    map_custom_variable_display,
    render_template_text,
    resolution_channel_for_lead,
    upsert_custom_variables_channel,
    upsert_variable_resolution_channel,
)


class TestMapCustomDisplay(unittest.TestCase):
    def test_maps_raw_to_label(self) -> None:
        out = map_custom_variable_display(
            "Ap29092",
            {"Ap29092": "Jardim Prudência"},
            default="",
            normalize={"trim": True, "lower": False},
        )
        self.assertEqual(out, "Jardim Prudência")

    def test_lower_normalize(self) -> None:
        out = map_custom_variable_display(
            "ap29092",
            {"Ap29092": "Jardim Prudência"},
            default="",
            normalize={"trim": True, "lower": True},
        )
        self.assertEqual(out, "Jardim Prudência")

    def test_fallback_raw_when_no_mapping(self) -> None:
        out = map_custom_variable_display(
            "Outro",
            {"Ap29092": "X"},
            default="",
            normalize={"trim": True, "lower": False},
        )
        self.assertEqual(out, "Outro")

    def test_default_when_empty_raw(self) -> None:
        out = map_custom_variable_display(
            "",
            {"a": "b"},
            default="vazio",
            normalize={},
        )
        self.assertEqual(out, "vazio")


class TestResolutionChannel(unittest.TestCase):
    def test_internal_maps_to_meta(self) -> None:
        self.assertEqual(resolution_channel_for_lead("internal_lead"), "meta_lead")

    def test_site_preserved(self) -> None:
        self.assertEqual(resolution_channel_for_lead("site_lead"), "site_lead")


class TestCustomVariablesStorageChannel(unittest.TestCase):
    def test_google_report_own_bucket(self) -> None:
        self.assertEqual(custom_variables_storage_channel("google_report"), "google_report")

    def test_meta_report_own_bucket(self) -> None:
        self.assertEqual(custom_variables_storage_channel("meta_report"), "meta_report")

    def test_internal_lead_shares_meta(self) -> None:
        self.assertEqual(custom_variables_storage_channel("internal_lead"), "meta_lead")

    def test_resolution_differs_from_storage(self) -> None:
        self.assertEqual(resolution_channel_for_lead("google_report"), "meta_lead")
        self.assertNotEqual(
            custom_variables_storage_channel("google_report"),
            resolution_channel_for_lead("google_report"),
        )


class TestEffectiveKeysDefaults(unittest.TestCase):
    def test_nome_default_tuple(self) -> None:
        keys = get_effective_source_keys("meta_lead", "nome")
        self.assertIn("nome_completo", keys)
        self.assertIn("name", keys)


class TestRenderWithCustomVar(unittest.TestCase):
    def test_template_receives_custom_placeholder(self) -> None:
        ctx = {
            "client_name": "C",
            "chegada_em": "hoje",
            "nome": "n",
            "email": "e",
            "whatsapp": "w",
            "respostas": "r",
            "bairro_amigavel": "Jardim Prudência",
        }
        text = render_template_text("Bairro: {{bairro_amigavel}} — {{client_name}}", ctx)
        self.assertIn("Jardim Prudência", text)
        self.assertIn("C", text)


class TestPersistenceRoundTrip(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._path = os.path.join(self._tmpdir.name, "message_templates.json")

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_custom_variables_roundtrip(self) -> None:
        with patch("execution.message_templates.message_templates_json_path", return_value=self._path):
            with patch("execution.message_templates._use_db_templates", return_value=False):
                saved = upsert_custom_variables_channel(
                    "meta_lead",
                    [
                        {
                            "key": "bairro_amigavel",
                            "source_keys": ["referencia"],
                            "mappings": {"Ap29092": "Jardim Prudência"},
                            "default": "",
                            "normalize": {"trim": True, "lower": True},
                        }
                    ],
                )
                self.assertEqual(len(saved), 1)
                self.assertEqual(saved[0]["key"], "bairro_amigavel")
                with open(self._path, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                self.assertIn("custom_variables", doc)
                self.assertEqual(doc["custom_variables"]["meta_lead"][0]["key"], "bairro_amigavel")
                self.assertEqual(doc["custom_variables"]["meta_lead"][0].get("source", "payload"), "payload")

    def test_internal_lead_saves_to_meta_lead_bucket(self) -> None:
        with patch("execution.message_templates.message_templates_json_path", return_value=self._path):
            with patch("execution.message_templates._use_db_templates", return_value=False):
                upsert_custom_variables_channel(
                    "internal_lead",
                    [
                        {
                            "key": "rotulo_x",
                            "source_keys": ["campo_a"],
                            "mappings": {},
                            "default": "d",
                            "normalize": {"trim": True, "lower": False},
                        }
                    ],
                )
                with open(self._path, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                self.assertIn("rotulo_x", [x.get("key") for x in doc["custom_variables"]["meta_lead"]])

    def test_variable_resolution_roundtrip(self) -> None:
        with patch("execution.message_templates.message_templates_json_path", return_value=self._path):
            with patch("execution.message_templates._use_db_templates", return_value=False):
                upsert_variable_resolution_channel(
                    "meta_lead",
                    {"nome": {"source_keys": ["apelido", "nome"]}},
                )
                with open(self._path, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                self.assertEqual(doc["variable_resolution"]["meta_lead"]["nome"]["source_keys"], ["apelido", "nome"])


class TestExtendedOriginField(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._path = os.path.join(self._tmpdir.name, "message_templates.json")

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_get_effective_source_keys_for_custom_origin(self) -> None:
        with patch("execution.message_templates.message_templates_json_path", return_value=self._path):
            with patch("execution.message_templates._use_db_templates", return_value=False):
                upsert_variable_resolution_channel(
                    "meta_lead",
                    {
                        "codigo_bairro": {"source_keys": ["referencia", "ref", "bairro_id"]},
                    },
                )
                keys = get_effective_source_keys("meta_lead", "codigo_bairro")
        self.assertEqual(keys, ("referencia", "ref", "bairro_id"))

    def test_internal_lead_upsert_goes_to_meta_lead(self) -> None:
        with patch("execution.message_templates.message_templates_json_path", return_value=self._path):
            with patch("execution.message_templates._use_db_templates", return_value=False):
                upsert_variable_resolution_channel(
                    "internal_lead",
                    {"x_extra": {"source_keys": ["campo_x"]}},
                )
                with open(self._path, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                self.assertIn("x_extra", doc.get("variable_resolution", {}).get("meta_lead", {}))


class TestApplyCustomVariables(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._path = os.path.join(self._tmpdir.name, "message_templates.json")

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_context_source_reads_nome(self) -> None:
        with patch("execution.message_templates.message_templates_json_path", return_value=self._path):
            with patch("execution.message_templates._use_db_templates", return_value=False):
                upsert_custom_variables_channel(
                    "meta_lead",
                    [
                        {
                            "key": "saudacao",
                            "source": "context",
                            "source_keys": ["nome"],
                            "mappings": {},
                            "default": "visitante",
                            "normalize": {"trim": True, "lower": False},
                        }
                    ],
                )
                ctx: dict = {"nome": "Ana", "client_name": "C"}
                apply_custom_variables(
                    "meta_lead",
                    ctx,
                    resolve_payload=lambda _k: "",
                )
                self.assertEqual(ctx.get("saudacao"), "Ana")

    def test_multi_pass_custom_depends_on_custom(self) -> None:
        with patch("execution.message_templates.message_templates_json_path", return_value=self._path):
            with patch("execution.message_templates._use_db_templates", return_value=False):
                upsert_custom_variables_channel(
                    "meta_lead",
                    [
                        {
                            "key": "a",
                            "source": "context",
                            "source_keys": ["nome"],
                            "mappings": {},
                            "default": "",
                            "normalize": {"trim": True, "lower": False},
                        },
                        {
                            "key": "b",
                            "source": "context",
                            "source_keys": ["a"],
                            "mappings": {},
                            "default": "x",
                            "normalize": {"trim": True, "lower": False},
                        },
                    ],
                )
                ctx = {"nome": "Zé"}
                apply_custom_variables("meta_lead", ctx, resolve_payload=lambda _k: "")
                self.assertEqual(ctx.get("a"), "Zé")
                self.assertEqual(ctx.get("b"), "Zé")


if __name__ == "__main__":
    unittest.main()
