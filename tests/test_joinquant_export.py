import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
EXPORT_SCRIPT = ROOT / "scripts" / "export_joinquant.py"


def _load_export_module():
    spec = importlib.util.spec_from_file_location("export_joinquant_test", EXPORT_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _run_cleanroom(code, *, isolated=False, no_site=False):
    command = [sys.executable]
    if isolated:
        command.append("-I")
    if no_site:
        command.append("-S")
    command.extend(["-X", "utf8", "-c", code])
    return subprocess.run(
        command,
        cwd=str(ROOT),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def _copy_export_sources(exporter, destination):
    for artifact in exporter.ARTIFACTS:
        target = destination / artifact.source
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / artifact.source, target)


def _repository_identity(exporter):
    return exporter._repository_snapshot(ROOT)[2]


def test_export_is_deterministic_and_preserves_source_bytes(tmp_path):
    exporter = _load_export_module()
    first = tmp_path / "first"
    second = tmp_path / "second"

    first_manifest = exporter.export_joinquant(ROOT, first)
    second_manifest = exporter.export_joinquant(ROOT, second)

    assert first_manifest == second_manifest
    assert (first / "manifest.json").read_bytes() == (
        second / "manifest.json"
    ).read_bytes()
    assert {path.name for path in first.iterdir()} == {
        "good_etf.py",
        "bullet_trade_jq_remote_helper.py",
        "jq_runtime_config.example.py",
        "manifest.json",
    }
    manifest_text = (first / "manifest.json").read_text(encoding="utf-8")
    assert str(ROOT) not in manifest_text
    assert first_manifest["production_ready"] is False

    for spec in exporter.ARTIFACTS:
        source = ROOT / spec.source
        output = first / spec.output
        assert output.read_bytes() == source.read_bytes()


def test_exported_helper_imports_without_dataclasses_module(tmp_path):
    exporter = _load_export_module()
    destination = tmp_path / "export"
    exporter.export_joinquant(ROOT, destination)
    helper_path = destination / "bullet_trade_jq_remote_helper.py"
    code = """
import builtins
import importlib.util

original_import = builtins.__import__
def joinquant_import(name, *args, **kwargs):
    if name == 'dataclasses':
        raise ModuleNotFoundError('JoinQuant has no dataclasses')
    return original_import(name, *args, **kwargs)
builtins.__import__ = joinquant_import

spec = importlib.util.spec_from_file_location('jq_helper_without_dataclasses', {path!r})
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
request = helper.ExecutionRequest(
    style=helper.ConditionalLimitExecution(2000)
)
assert request.style.price_band_ppm == 2000
try:
    request.follow_up = helper.FollowUpPolicy.NONE
except AttributeError:
    pass
else:
    raise AssertionError('execution request is mutable')
print('HELPER_WITHOUT_DATACLASSES_OK')
""".format(path=str(helper_path))

    result = _run_cleanroom(code, isolated=True, no_site=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "HELPER_WITHOUT_DATACLASSES_OK" in result.stdout


@pytest.mark.parametrize("with_content", [False, True])
def test_export_refuses_any_existing_destination(tmp_path, with_content):
    exporter = _load_export_module()
    destination = tmp_path / "existing"
    destination.mkdir()
    if with_content:
        (destination / "keep.txt").write_text("user data", encoding="utf-8")

    with pytest.raises(exporter.ValidationError, match="must not exist"):
        exporter.export_joinquant(ROOT, destination)

    assert destination.is_dir()
    if with_content:
        assert (destination / "keep.txt").read_text(encoding="utf-8") == "user data"


def test_export_replace_failure_preserves_absent_destination(tmp_path, monkeypatch):
    exporter = _load_export_module()
    destination = tmp_path / "export"

    def fail_replace(source, target):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(exporter.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated replace failure"):
        exporter.export_joinquant(ROOT, destination)

    assert not destination.exists()
    assert not list(tmp_path.glob(".joinquant-export-*"))


def test_export_uses_one_immutable_source_snapshot(tmp_path, monkeypatch):
    exporter = _load_export_module()
    repo = tmp_path / "repo"
    _copy_export_sources(exporter, repo)
    strategy = repo / "strategies" / "joinquant" / "good_etf.py"
    original = strategy.read_bytes()
    original_validate = exporter._validate_source_data
    mutated = []

    def mutate_after_read(role, data, source_name):
        result = original_validate(role, data, source_name)
        if role == "strategy" and not mutated:
            strategy.write_bytes(data + b"\n# changed after snapshot\n")
            mutated.append(True)
        return result

    monkeypatch.setattr(exporter, "_validate_source_data", mutate_after_read)
    destination = tmp_path / "export"
    manifest = exporter.export_joinquant(repo, destination)

    assert strategy.read_bytes() != original
    assert (destination / "good_etf.py").read_bytes() == original
    strategy_entry = next(
        item for item in manifest["files"] if item["role"] == "strategy"
    )
    assert strategy_entry["sha256"] == exporter._sha256(original)


def test_export_rejects_reparse_destination_component(tmp_path, monkeypatch):
    exporter = _load_export_module()
    monkeypatch.setattr(exporter, "_has_reparse_component", lambda path: True)

    with pytest.raises(exporter.ValidationError, match="reparse point"):
        exporter.export_joinquant(ROOT, tmp_path / "export")


def test_reparse_check_rejects_dangling_link_before_exists(tmp_path, monkeypatch):
    exporter = _load_export_module()
    destination = tmp_path / "dangling-link"
    original_is_symlink = exporter.Path.is_symlink

    def dangling_link_model(path):
        if path == destination:
            return True
        return original_is_symlink(path)

    monkeypatch.setattr(exporter.Path, "is_symlink", dangling_link_model)

    assert not destination.exists()
    assert exporter._has_reparse_component(destination) is True


def test_manifest_matches_exported_file_hashes(tmp_path):
    exporter = _load_export_module()
    destination = tmp_path / "export"
    exporter.export_joinquant(ROOT, destination)
    manifest = json.loads((destination / "manifest.json").read_text(encoding="utf-8"))

    assert manifest["schema_version"] == 1
    assert manifest["artifact_kind"] == "bullet-trade-joinquant-upload"
    assert manifest["production_ready"] is False
    assert "contracts" not in manifest
    assert [entry["output"] for entry in manifest["files"]] == [
        spec.output for spec in exporter.ARTIFACTS
    ]
    for entry in manifest["files"]:
        data = (destination / entry["output"]).read_bytes()
        assert len(data) == entry["bytes"]
        assert exporter._sha256(data) == entry["sha256"]


@pytest.mark.parametrize(
    ("role", "source", "message"),
    [
        (
            "strategy",
            "url = 'https://open.feishu.cn/open-apis/bot/v2/hook/secret'\n",
            "forbidden webhook or bearer credential",
        ),
        (
            "strategy",
            "header = 'Bearer abcdef0123456789'\n",
            "forbidden webhook or bearer credential",
        ),
        (
            "strategy",
            "credential = 'hard-coded-real-token'\n",
            "literal private value",
        ),
        (
            "strategy",
            "BT_REMOTE_TOKEN = 'hard-coded-real-token'\n",
            "literal private value",
        ),
        (
            "strategy",
            "api_token = 'hard-coded-real-token'\n",
            "literal private value",
        ),
        (
            "strategy",
            "server_host = '10.0.0.8'\n",
            "literal private value",
        ),
        (
            "strategy",
            "configure(token='hard-coded-real-token')\n",
            "literal private value",
        ),
        (
            "strategy",
            "configure('10.0.0.8', 'hard-coded-real-token')\n",
            "literal private value",
        ),
        (
            "helper",
            "BT_REMOTE_TOKEN = 'hard-coded-real-token'\n",
            "literal private value",
        ),
    ],
)
def test_source_validation_rejects_obvious_credentials(
    tmp_path, role, source, message
):
    exporter = _load_export_module()
    path = tmp_path / "candidate.py"
    path.write_text(source, encoding="utf-8")

    with pytest.raises(exporter.ValidationError, match=message):
        exporter.validate_source(role, path)


def test_source_validation_rejects_python_39_syntax(tmp_path):
    exporter = _load_export_module()
    path = tmp_path / "strategy.py"
    path.write_text("match value:\n    case 1:\n        pass\n", encoding="utf-8")

    with pytest.raises(exporter.ValidationError, match="Python 3.8 compatible"):
        exporter.validate_source("strategy", path)


def test_source_validation_rejects_dataclasses_in_joinquant_helper(tmp_path):
    exporter = _load_export_module()
    path = tmp_path / "helper.py"
    path.write_text(
        "from dataclasses import dataclass\n", encoding="utf-8"
    )

    with pytest.raises(
        exporter.ValidationError,
        match="unavailable JoinQuant module dataclasses",
    ):
        exporter.validate_source("helper", path)


def test_source_validation_rejects_unknown_role(tmp_path):
    exporter = _load_export_module()
    path = tmp_path / "candidate.py"
    path.write_text("value = 1\n", encoding="utf-8")

    with pytest.raises(exporter.ValidationError, match="unknown artifact role"):
        exporter.validate_source("unknown", path)


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (
            "PROFILE_SCHEMA_VERSION = 3\nDEFAULT_PROFILE = 'prod'\n"
            "STRATEGIES = {}\n"
            "PROFILES = {'prod': {'host': '10.0.0.8', 'token': 'real-token'}}\n",
            "literal private value",
        ),
        (
            "PROFILE_SCHEMA_VERSION = 3\nDEFAULT_PROFILE = 'prod'\n"
            "STRATEGIES = {}\n"
            "PROFILES = {'prod': {'host': '', 'token': '', "
            "'account_key': 123}}\n",
            "literal private value",
        ),
        (
            "PROFILE_SCHEMA_VERSION = 3\nDEFAULT_PROFILE = 'prod'\n"
            "STRATEGIES = {}\n"
            "PROFILES = {'prod': {'host': '', 'token': ''}}\n"
            "print('side effect')\n",
            "assignments only",
        ),
        (
            "PROFILE_SCHEMA_VERSION = 3\nDEFAULT_PROFILE = 'prod'\n"
            "STRATEGIES = {}\n"
            "PROFILES = {'prod': {'host': '', "
            "'token': 'real-' + 'token'}}\n",
            "literal data",
        ),
        (
            "PROFILE_SCHEMA_VERSION = 3\nDEFAULT_PROFILE = 'prod'\n"
            "STRATEGIES = {}\n"
            "PROFILES = {'prod': {'host': '', 'token': '', "
            "'credential': None}}\n",
            "unknown fields",
        ),
        (
            "PROFILE_SCHEMA_VERSION = 3\nDEFAULT_PROFILE = 'prod'\n"
            "STRATEGIES = {'good_etf_remote': {'qmt_account_enabled': 'yes'}}\n"
            "PROFILES = {'prod': {'host': '', 'token': ''}}\n",
            "account switches must be bool",
        ),
    ],
)
def test_profile_template_rejects_invalid_shapes(tmp_path, body, message):
    exporter = _load_export_module()
    path = tmp_path / "profile.py"
    path.write_text(body, encoding="utf-8")

    with pytest.raises(exporter.ValidationError, match=message):
        exporter.validate_source("profile-template", path)


def test_private_profile_is_validated_without_exposing_or_copying_secrets(tmp_path):
    exporter = _load_export_module()
    identity = _repository_identity(exporter)
    private_profile = tmp_path / "jq_runtime_config.py"
    private_profile.write_text(
        "PROFILE_SCHEMA_VERSION = 3\n"
        "DEFAULT_PROFILE = 'qmt-main'\n"
        "STRATEGIES = {'good_etf_remote': {'profile': 'qmt-main', "
        "'jq_account_enabled': True, 'qmt_account_enabled': True}}\n"
        "PROFILES = {'qmt-main': {"
        "'host': '10.0.0.8', "
        "'token': 'hard-coded-real-token', 'port': 58620, "
        "'account_key': None, 'tls_cert': None, 'rpc_timeout': 60.0}}\n",
        encoding="utf-8",
    )

    result = exporter.validate_private_profile(private_profile, identity)

    assert result == {
        "profile": "qmt-main",
        "profile_schema_version": 3,
        "strategy_id": "good_etf_remote",
        "jq_account_enabled": True,
        "qmt_account_enabled": True,
    }
    assert list(tmp_path.iterdir()) == [private_profile]
    assert "hard-coded-real-token" not in repr(result)


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (
            "PROFILE_SCHEMA_VERSION = 3\nDEFAULT_PROFILE = 'qmt-main'\n"
            "STRATEGIES = {'good_etf_remote': {'profile': 'missing'}}\n"
            "PROFILES = {'qmt-main': {'host': '10.0.0.8', "
            "'token': 'real-token'}}\n",
            "invalid profile",
        ),
        (
            "PROFILE_SCHEMA_VERSION = 3\nDEFAULT_PROFILE = 'qmt-main'\n"
            "STRATEGIES = {}\n"
            "PROFILES = {'qmt-main': {"
            "'host': '10.0.0.8', 'token': 'real-token', "
            "'credential': 'leak'}}\n",
            "unknown fields",
        ),
        (
            "PROFILE_SCHEMA_VERSION = 3\nDEFAULT_PROFILE = 'qmt-main'\n"
            "STRATEGIES = {}\n"
            "PROFILES = make_profile()\n",
            "literal data",
        ),
        (
            "PROFILE_SCHEMA_VERSION = 3\nDEFAULT_PROFILE = 'qmt-main'\n"
            "STRATEGIES = {}\n"
            "PROFILES = {'qmt-main': {"
            "'host': '10.0.0.8', 'token': 'real-token', "
            "'rpc_timeout': None}}\n",
            "invalid rpc_timeout",
        ),
        (
            "PROFILE_SCHEMA_VERSION = 3\nDEFAULT_PROFILE = 'qmt-main'\n"
            "STRATEGIES = {}\n"
            "PROFILES = {'qmt-main': {"
            "'host': 'your-host', 'token': 'your-token'}}\n",
            "invalid host",
        ),
        (
            "PROFILE_SCHEMA_VERSION = 3\nDEFAULT_PROFILE = 'qmt-main'\n"
            "STRATEGIES = {}\n"
            "PROFILES = {'qmt-main': {"
            "'host': '10.0.0.8', 'token': 'replace-me'}}\n",
            "invalid token",
        ),
    ],
)
def test_private_profile_rejects_contract_drift_and_code(tmp_path, body, message):
    exporter = _load_export_module()
    private_profile = tmp_path / "jq_runtime_config.py"
    private_profile.write_text(body, encoding="utf-8")

    with pytest.raises(exporter.ValidationError, match=message):
        exporter.validate_private_profile(
            private_profile, _repository_identity(exporter)
        )


def test_cli_reports_stable_io_error_on_stderr(monkeypatch, capsys):
    exporter = _load_export_module()

    def fail_export(repo, destination, private_profile=None):
        raise OSError("sensitive local path")

    monkeypatch.setattr(exporter, "export_joinquant", fail_export)
    monkeypatch.setattr(sys, "argv", [str(EXPORT_SCRIPT), "--output", "new-output"])

    assert exporter.main() == 3
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "JOINQUANT_EXPORT_IO_ERROR\n"
    assert "sensitive local path" not in captured.err


def test_cleanroom_import_uses_only_exported_runtime_files(tmp_path):
    exporter = _load_export_module()
    destination = tmp_path / "export"
    exporter.export_joinquant(ROOT, destination)
    code = """
import importlib.util
import runpy
import sys
import types
from pathlib import Path

root = Path({root!r})
sys.path.insert(0, str(root))
jqdata = types.ModuleType('jqdata')
jqdata.__all__ = []
sys.modules['jqdata'] = jqdata
spec = importlib.util.spec_from_file_location('clean_good_etf', root / 'good_etf.py')
assert spec is not None and spec.loader is not None
strategy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(strategy)
helper = sys.modules['bullet_trade_jq_remote_helper']
assert helper.STRATEGY_RUNTIME_API_VERSION == 20
profile = runpy.run_path(str(root / 'jq_runtime_config.example.py'))
assert profile['PROFILE_SCHEMA_VERSION'] == 3
assert profile['DEFAULT_PROFILE'] == 'qmt-main'
assert profile['PROFILES']['qmt-main']['host'] == ''
print('CLEANROOM_IMPORT_OK')
""".format(root=str(destination))

    result = _run_cleanroom(code, isolated=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "CLEANROOM_IMPORT_OK" in result.stdout


def test_cleanroom_missing_helper_fails_jq_closed(tmp_path):
    exporter = _load_export_module()
    destination = tmp_path / "export"
    exporter.export_joinquant(ROOT, destination)
    strategy_path = destination / "good_etf.py"
    code = """
import importlib.util
import sys
import types

jqdata = types.ModuleType('jqdata')
jqdata.__all__ = []
sys.modules['jqdata'] = jqdata
spec = importlib.util.spec_from_file_location('missing_helper_strategy', {path!r})
strategy = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(strategy)
except ModuleNotFoundError as exc:
    assert exc.name == 'bullet_trade_jq_remote_helper'
else:
    raise AssertionError('JQ accepted missing helper')
print('MISSING_HELPER_FAIL_CLOSED_OK')
""".format(path=str(strategy_path))

    result = _run_cleanroom(code, isolated=True, no_site=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "MISSING_HELPER_FAIL_CLOSED_OK" in result.stdout


def test_cleanroom_version_mismatch_is_rejected_by_helper_boundary(tmp_path):
    exporter = _load_export_module()
    destination = tmp_path / "export"
    exporter.export_joinquant(ROOT, destination)
    strategy_path = destination / "good_etf.py"
    code = """
import importlib.util
import sys
import types

jqdata = types.ModuleType('jqdata')
jqdata.__all__ = []
sys.modules['jqdata'] = jqdata
called = []
helper = types.ModuleType('bullet_trade_jq_remote_helper')
from enum import Enum
class RuntimeMode(str, Enum):
    BACKTEST = 'BACKTEST'
    JQ = 'JQ'
    QMT_REMOTE = 'QMT_REMOTE'
    JQ_QMT_PARALLEL = 'JQ_QMT_PARALLEL'
helper.RuntimeMode = RuntimeMode
def install(*args, **kwargs):
    called.append(kwargs['expected_api_version'])
    raise RuntimeError('helper运行时API版本不匹配')
helper.install_joinquant_runtime = install
sys.modules['bullet_trade_jq_remote_helper'] = helper
spec = importlib.util.spec_from_file_location('bad_version_strategy', {path!r})
strategy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(strategy)
context = types.SimpleNamespace(run_params=types.SimpleNamespace(type='sim_trade'))
try:
    strategy._install_runtime(context)
except RuntimeError as exc:
    assert 'API' in str(exc)
else:
    raise AssertionError('accepted mismatched helper API')
assert called == [20]
print('VERSION_MISMATCH_FAIL_CLOSED_OK')
""".format(path=str(strategy_path))

    result = _run_cleanroom(code, isolated=True, no_site=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "VERSION_MISMATCH_FAIL_CLOSED_OK" in result.stdout


def test_cleanroom_missing_private_profile_fails_jq_closed(tmp_path):
    exporter = _load_export_module()
    destination = tmp_path / "export"
    exporter.export_joinquant(ROOT, destination)
    code = """
import importlib.util
import sys
import types
from pathlib import Path

root = Path({root!r})
sys.path.insert(0, str(root))
jqdata = types.ModuleType('jqdata')
jqdata.__all__ = []
sys.modules['jqdata'] = jqdata
sys.modules.pop('jq_runtime_config', None)
spec = importlib.util.spec_from_file_location('missing_profile_strategy', root / 'good_etf.py')
strategy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(strategy)
context = types.SimpleNamespace(run_params=types.SimpleNamespace(type='sim_trade'))
try:
    strategy._install_runtime(context)
except RuntimeError as exc:
    assert '运行配置模块' in str(exc)
else:
    raise AssertionError('JQ accepted missing private profile')
print('MISSING_PROFILE_FAIL_CLOSED_OK')
""".format(root=str(destination))

    result = _run_cleanroom(code, isolated=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "MISSING_PROFILE_FAIL_CLOSED_OK" in result.stdout
