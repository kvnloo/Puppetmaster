"""Provider-qualified agentic identities are not discovery slugs.

Issue #197: #196 can select ``agentic/openai/gpt-5-6-sol`` after a spent
launch adapter, then preflight blocks because ``gpt-5.6-sol`` is absent
from a fresh nonempty agentic snapshot. OpenAI/OpenRouter never publish
that Marionette marketing slug, so ``models discover --probe`` cannot
learn it. The leftover is catalog-vs-identity, not a missing discover
row.

Admit an exact enabled ``adapter/provider/leaf`` registry id when that
provider is ready. Bare and two-segment names still need the snapshot.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

_HERMETIC_DIR = os.path.dirname(os.path.abspath(__file__))
if _HERMETIC_DIR not in sys.path:
    sys.path.insert(0, _HERMETIC_DIR)
import hermetic_env  # noqa: F401

from puppetmaster.model_registry import ModelSpec, save_registry, write_discovery_meta
from puppetmaster.models import Task
from puppetmaster.platform_billing import BillingStatus
from puppetmaster.preflight import preflight_check


def _openai_sol():
    return ModelSpec(
        id="agentic/openai/gpt-5-6-sol",
        adapter="agentic",
        adapter_model_name="gpt-5.6-sol",
        capability_score=92,
        billing="api",
        tags=["agentic"],
        payload_defaults={"provider": "openai"},
    )


def _bare_gpt5():
    return ModelSpec(
        id="agentic/gpt-5",
        adapter="agentic",
        adapter_model_name="gpt-5",
        capability_score=90,
        billing="api",
    )


def _unqualified_sol():
    return ModelSpec(
        id="agentic/gpt-5.6-sol",
        adapter="agentic",
        adapter_model_name="gpt-5.6-sol",
        capability_score=90,
        billing="api",
        payload_defaults={"provider": "openai-codex"},
    )


def _openrouter_sol():
    return ModelSpec(
        id="agentic/openrouter/gpt-5-6-sol",
        adapter="agentic",
        adapter_model_name="gpt-5.6-sol",
        capability_score=91,
        billing="api",
        payload_defaults={"provider": "openrouter"},
    )


def _healthy_agentic():
    return BillingStatus(
        adapter="agentic",
        billing="api",
        healthy=True,
        detail="openai key",
        evidence=["openai"],
    )


class ProviderQualifiedPreflightTests(TestCase):
    def _check(self, tmp, specs, model, *, identities=(), catalog_ids=None):
        registry_path = Path(tmp) / "models.json"
        save_registry(specs, registry_path)
        write_discovery_meta(
            "agentic",
            1,
            registry_path,
            model_ids=list(catalog_ids or ["other-model"]),
        )
        with patch.dict(
            os.environ,
            {
                "PUPPETMASTER_MODELS_PATH": str(registry_path),
                "PUPPETMASTER_CATALOG_CACHE_TTL_SECONDS": "3600",
            },
            clear=False,
        ), patch(
            "puppetmaster.providers.available_providers",
            return_value={"openai"},
        ):
            return preflight_check(
                "agentic",
                model,
                identities=identities,
                billing_status=_healthy_agentic(),
            )

    def test_qualified_openai_sol_admits_when_catalog_lacks_slug(self) -> None:
        with TemporaryDirectory() as tmp:
            result = self._check(
                tmp,
                [_openai_sol()],
                "gpt-5.6-sol",
                identities=("agentic/openai/gpt-5-6-sol",),
            )
        self.assertTrue(result.ok)
        self.assertIn("preflight:provider_qualified_identity", result.evidence)
        self.assertNotIn("preflight:cached_model_not_in_catalog", result.evidence)
        self.assertIn("agentic/openai/gpt-5-6-sol", result.reason)

    def test_unique_qualified_spec_admits_from_wire_name_alone(self) -> None:
        with TemporaryDirectory() as tmp:
            result = self._check(tmp, [_openai_sol()], "gpt-5.6-sol")
        self.assertTrue(result.ok)
        self.assertIn("preflight:provider_qualified_identity", result.evidence)

    def test_bare_name_still_blocks_on_fresh_nonempty_catalog(self) -> None:
        with TemporaryDirectory() as tmp:
            result = self._check(tmp, [_bare_gpt5()], "gpt-5")
        self.assertFalse(result.ok)
        self.assertIn("preflight:cached_model_not_in_catalog", result.evidence)
        self.assertIn("recent agentic catalog", result.reason)
        self.assertNotIn("preflight:provider_qualified_identity", result.evidence)

    def test_unqualified_two_segment_sol_still_needs_catalog(self) -> None:
        with TemporaryDirectory() as tmp:
            result = self._check(tmp, [_unqualified_sol()], "gpt-5.6-sol")
        self.assertFalse(result.ok)
        self.assertIn("preflight:cached_model_not_in_catalog", result.evidence)

    def test_ambiguous_qualified_wire_name_does_not_guess(self) -> None:
        with TemporaryDirectory() as tmp:
            result = self._check(
                tmp,
                [_openai_sol(), _openrouter_sol()],
                "gpt-5.6-sol",
            )
        self.assertFalse(result.ok)
        self.assertIn("preflight:cached_model_not_in_catalog", result.evidence)

    def test_identity_selects_openai_when_wire_name_is_ambiguous(self) -> None:
        with TemporaryDirectory() as tmp:
            result = self._check(
                tmp,
                [_openai_sol(), _openrouter_sol()],
                "gpt-5.6-sol",
                identities=("agentic/openai/gpt-5-6-sol",),
            )
        self.assertTrue(result.ok)
        self.assertIn("preflight:provider_qualified_identity", result.evidence)
        self.assertIn("openai", result.reason)

    def test_qualified_identity_blocks_when_its_provider_is_unready(self) -> None:
        with TemporaryDirectory() as tmp:
            registry_path = Path(tmp) / "models.json"
            save_registry([_openai_sol()], registry_path)
            write_discovery_meta(
                "agentic",
                1,
                registry_path,
                model_ids=["other-model"],
            )
            with patch.dict(
                os.environ,
                {
                    "PUPPETMASTER_MODELS_PATH": str(registry_path),
                    "PUPPETMASTER_CATALOG_CACHE_TTL_SECONDS": "3600",
                },
                clear=False,
            ), patch(
                "puppetmaster.providers.available_providers",
                return_value={"openrouter"},
            ):
                result = preflight_check(
                    "agentic",
                    "gpt-5.6-sol",
                    identities=("agentic/openai/gpt-5-6-sol",),
                    billing_status=_healthy_agentic(),
                )
        self.assertFalse(result.ok)
        self.assertIn("preflight:provider_qualified_unready", result.evidence)
        self.assertNotIn("preflight:cached_model_not_in_catalog", result.evidence)
        self.assertIn("openai", result.reason)

    def test_registry_provider_binding_outranks_id_namespace(self) -> None:
        # Marionette binds OpenRouter models as ``agentic/<namespace>/<leaf>``
        # with ``payload_defaults.provider``. A namespace that is also a direct
        # provider (deepseek, openai) must not reroute the readiness check:
        # a router fallback to this spec was blocked as "provider 'deepseek'
        # is not available" with only OpenRouter keyed.
        spec = ModelSpec(
            id="agentic/deepseek/deepseek-v4-pro",
            adapter="agentic",
            adapter_model_name="deepseek/deepseek-v4-pro",
            capability_score=85,
            billing="api",
            tags=["agentic"],
            payload_defaults={"provider": "openrouter"},
        )
        for ready, ok in (({"openrouter"}, True), ({"deepseek"}, False)):
            with TemporaryDirectory() as tmp:
                registry_path = Path(tmp) / "models.json"
                save_registry([spec], registry_path)
                write_discovery_meta("agentic", 1, registry_path, model_ids=["other-model"])
                with patch.dict(
                    os.environ,
                    {
                        "PUPPETMASTER_MODELS_PATH": str(registry_path),
                        "PUPPETMASTER_CATALOG_CACHE_TTL_SECONDS": "3600",
                    },
                    clear=False,
                ), patch("puppetmaster.providers.available_providers", return_value=ready):
                    result = preflight_check(
                        "agentic",
                        "deepseek/deepseek-v4-pro",
                        identities=("agentic/deepseek/deepseek-v4-pro",),
                        billing_status=_healthy_agentic(),
                    )
            self.assertEqual(result.ok, ok, (ready, result.reason))
            self.assertIn("openrouter", result.reason)

    def test_worker_preflight_forwards_router_model_id(self) -> None:
        from puppetmaster.workers import LocalWorker

        with TemporaryDirectory() as tmp:
            registry_path = Path(tmp) / "models.json"
            save_registry([_openai_sol()], registry_path)
            write_discovery_meta(
                "agentic",
                1,
                registry_path,
                model_ids=["other-model"],
            )
            task = Task(
                job_id="j",
                role="explore",
                instruction="audit",
                adapter="agentic",
                payload={
                    "model": "gpt-5.6-sol",
                    "router_model_id": "agentic/openai/gpt-5-6-sol",
                    "billing": "api",
                },
            )
            worker = LocalWorker("explore")
            with patch.dict(
                os.environ,
                {
                    "PUPPETMASTER_MODELS_PATH": str(registry_path),
                    "PUPPETMASTER_CATALOG_CACHE_TTL_SECONDS": "3600",
                },
                clear=False,
            ), patch(
                "puppetmaster.providers.available_providers",
                return_value={"openai"},
            ), patch(
                "puppetmaster.preflight.detect_adapter_billing",
                return_value=_healthy_agentic(),
            ):
                artifact = worker._preflight(task)
        self.assertIsNone(artifact)

    def test_setup_readiness_admits_qualified_sol_without_catalog_slug(self) -> None:
        from puppetmaster import setup_readiness as readiness

        with TemporaryDirectory() as tmp:
            registry_path = Path(tmp) / "models.json"
            save_registry([_openai_sol()], registry_path)
            write_discovery_meta(
                "agentic",
                1,
                registry_path,
                model_ids=["other-model"],
            )
            with patch.dict(
                os.environ,
                {
                    "PUPPETMASTER_MODELS_PATH": str(registry_path),
                    "PUPPETMASTER_CATALOG_CACHE_TTL_SECONDS": "3600",
                    "OPENAI_API_KEY": "fake-test-key",
                },
                clear=False,
            ), patch(
                "puppetmaster.platform_lock.is_adapter_enabled",
                return_value=True,
            ), patch(
                "puppetmaster.providers.available_providers",
                return_value={"openai"},
            ), patch(
                "puppetmaster.preflight.detect_adapter_billing",
                return_value=_healthy_agentic(),
            ), patch(
                "puppetmaster.platform_billing.detect_adapter_billing",
                return_value=_healthy_agentic(),
            ):
                result = readiness.collect_setup_readiness(
                    {"agentic"},
                    installation_results={"agentic": "installed"},
                )
        row = result["targets"][0]
        self.assertEqual(row["model"], "agentic/openai/gpt-5-6-sol")
        self.assertEqual(row["model_available"]["status"], "pass")
        self.assertEqual(row["preflight_passed"]["status"], "pass")
        self.assertIn("provider-qualified", row["model_available"]["evidence"][0])
