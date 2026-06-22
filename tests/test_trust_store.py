import sys
import types

from stemmata import cli


def _fake_truststore(calls: list[bool]) -> types.ModuleType:
    mod = types.ModuleType("truststore")
    mod.inject_into_ssl = lambda: calls.append(True)
    return mod


def test_injects_system_trust_store_by_default(monkeypatch):
    calls: list[bool] = []
    monkeypatch.setitem(sys.modules, "truststore", _fake_truststore(calls))
    monkeypatch.delenv("STEMMATA_SYSTEM_CA", raising=False)

    cli._inject_system_trust_store()

    assert calls == [True]


def test_opt_out_skips_injection(monkeypatch):
    calls: list[bool] = []
    monkeypatch.setitem(sys.modules, "truststore", _fake_truststore(calls))
    monkeypatch.setenv("STEMMATA_SYSTEM_CA", "0")

    cli._inject_system_trust_store()

    assert calls == []


def test_injection_failure_is_swallowed(monkeypatch):
    mod = types.ModuleType("truststore")

    def _boom() -> None:
        raise RuntimeError("no system trust store")

    mod.inject_into_ssl = _boom
    monkeypatch.setitem(sys.modules, "truststore", mod)
    monkeypatch.delenv("STEMMATA_SYSTEM_CA", raising=False)

    # Must not raise: HTTPS keeps working with the default bundle.
    cli._inject_system_trust_store()
