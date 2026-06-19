import pytest

from stemmata.errors import EXIT_CONFIG, ConfigError, SchemaError
from stemmata.npmrc import NpmConfig, load_npmrc, parse_npmrc, _canonicalize_url


def test_basic_key_value():
    entries = parse_npmrc("registry=https://registry.example.com/\n", env={})
    assert entries == {"registry": "https://registry.example.com/"}


def test_comments_and_blank_lines():
    text = """
# full line comment
; semicolon comment
registry=https://registry.example.com/
@acme:registry=https://private.example/  # trailing comment
"""
    entries = parse_npmrc(text, env={})
    assert entries["registry"] == "https://registry.example.com/"
    assert entries["@acme:registry"] == "https://private.example/"


def test_whitespace_tolerance():
    entries = parse_npmrc("   registry   =   https://x.y/   \n", env={})
    assert entries["registry"] == "https://x.y/"


def test_var_substitution():
    entries = parse_npmrc("//host/:_authToken=${TOKEN}\n", env={"TOKEN": "abc"})
    assert entries["//host/:_authToken"] == "abc"


def test_var_substitution_undefined_raises():
    with pytest.raises(SchemaError):
        parse_npmrc("//host/:_authToken=${MISSING}\n", env={})


def test_dollar_escape():
    entries = parse_npmrc("foo=$$LITERAL\n", env={})
    assert entries["foo"] == "$LITERAL"


def test_quoted_values():
    entries = parse_npmrc('foo="a b c"\n', env={})
    assert entries["foo"] == "a b c"


def test_last_wins_duplicate_keys():
    entries = parse_npmrc("registry=one\nregistry=two\n", env={})
    assert entries["registry"] == "two"


def test_canonicalize_url_strips_scheme_and_trailing_slash():
    assert _canonicalize_url("https://HOST.Com/path/") == "//host.com/path"
    assert _canonicalize_url("http://host.com:8080/a") == "//host.com:8080/a"


def test_auth_longest_prefix_wins():
    cfg = NpmConfig(entries={
        "//host.com/:_authToken": "short",
        "//host.com/scope/:_authToken": "long",
    })
    auth = cfg.auth_for_url("https://host.com/scope/pkg/-/pkg-1.0.0.tgz")
    assert auth.auth_token == "long"


def test_auth_scoped_registry_resolution():
    cfg = NpmConfig(entries={
        "registry": "https://default.example/",
        "@acme:registry": "https://private.example/repo/",
    })
    assert cfg.registry_for_scope("@acme") == "https://private.example/repo/"
    assert cfg.registry_for_scope("@other") == "https://default.example/"


def test_auth_basic_from_username_password():
    import base64
    encoded = base64.b64encode(b"hunter2").decode()
    cfg = NpmConfig(entries={
        "//host.com/:username": "alice",
        "//host.com/:_password": encoded,
    })
    auth = cfg.auth_for_url("https://host.com/pkg/")
    assert auth.username == "alice"
    assert auth.password_b64 == encoded


def test_unknown_keys_ignored():
    entries = parse_npmrc("strict-ssl=true\nregistry=https://x/\n", env={})
    assert entries["strict-ssl"] == "true"
    assert entries["registry"] == "https://x/"


def test_crlf_tolerated():
    entries = parse_npmrc("registry=https://x/\r\n", env={})
    assert entries["registry"] == "https://x/"


def test_bom_tolerated():
    entries = parse_npmrc("\ufeffregistry=https://x/\n", env={})
    assert entries["registry"] == "https://x/"


# --- file resolution precedence (issue #17) -------------------------------


def test_explicit_path_loaded(tmp_path):
    f = tmp_path / "explicit.npmrc"
    f.write_text("registry=https://explicit/\n")
    cfg = load_npmrc(f, env={})
    assert cfg.default_registry() == "https://explicit/"


def test_explicit_missing_path_raises_config_error(tmp_path):
    missing = tmp_path / "nope.npmrc"
    with pytest.raises(ConfigError) as exc:
        load_npmrc(missing, env={})
    assert exc.value.code == EXIT_CONFIG
    assert str(missing) in exc.value.message


def test_userconfig_env_used_when_file_exists(tmp_path):
    f = tmp_path / "ci.npmrc"
    f.write_text("registry=https://ci/\n")
    cfg = load_npmrc(None, env={"NPM_CONFIG_USERCONFIG": str(f)})
    assert cfg.default_registry() == "https://ci/"


def test_userconfig_env_missing_file_skips_to_home(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    (home / ".npmrc").write_text("registry=https://home/\n")
    monkeypatch.setattr("pathlib.Path.home", lambda: home)
    cfg = load_npmrc(None, env={"NPM_CONFIG_USERCONFIG": str(tmp_path / "ghost.npmrc")})
    assert cfg.default_registry() == "https://home/"


def test_userconfig_env_empty_string_treated_as_unset(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    (home / ".npmrc").write_text("registry=https://home/\n")
    monkeypatch.setattr("pathlib.Path.home", lambda: home)
    for blank in ("", "   "):
        cfg = load_npmrc(None, env={"NPM_CONFIG_USERCONFIG": blank})
        assert cfg.default_registry() == "https://home/"


def test_userconfig_env_tilde_expansion(tmp_path, monkeypatch):
    # '~' must expand via Path.home() so it is correct on every OS
    # (USERPROFILE on Windows, HOME/pwd on POSIX), not via os.path.expanduser.
    home = tmp_path / "home"
    home.mkdir()
    (home / "custom.npmrc").write_text("registry=https://tilde/\n")
    monkeypatch.setattr("pathlib.Path.home", lambda: home)
    cfg = load_npmrc(None, env={"NPM_CONFIG_USERCONFIG": "~/custom.npmrc"})
    assert cfg.default_registry() == "https://tilde/"


def test_explicit_path_wins_over_env(tmp_path):
    explicit = tmp_path / "explicit.npmrc"
    explicit.write_text("registry=https://explicit/\n")
    env_file = tmp_path / "env.npmrc"
    env_file.write_text("registry=https://env/\n")
    cfg = load_npmrc(explicit, env={"NPM_CONFIG_USERCONFIG": str(env_file)})
    assert cfg.default_registry() == "https://explicit/"


def test_no_sources_yields_empty_config(tmp_path, monkeypatch):
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path / "empty-home")
    cfg = load_npmrc(None, env={})
    assert cfg.entries == {}
