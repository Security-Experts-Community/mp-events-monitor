from nomos.redact import MASK, redact, redact_env_text, redact_headers, redact_obj


def test_pat_token_masked():
    assert "pat_" not in redact("token=pat_F8C3E3B22CA556EE8B6034A269AC51AC83D81272")


def test_bearer_masked():
    out = redact("Authorization: Bearer abcdef1234567890XYZ")
    assert "abcdef" not in out and MASK in out


def test_kv_pairs_masked():
    src = '{"password": "SuperSecret1!", "login": "operator"}'
    out = redact(src)
    assert "SuperSecret1!" not in out
    assert "operator" in out  # логин — не секрет


def test_env_redaction_keeps_host():
    env = "HOST=mp.local\nPERSONAL_TOKEN=pat_AAAABBBBCCCCDDDD1111\n# comment"
    out = redact_env_text(env)
    assert "HOST=mp.local" in out
    assert "pat_AAAA" not in out
    assert "# comment" in out


def test_headers_and_obj():
    headers = redact_headers({"Cookie": "session=abc", "Accept": "json"})
    assert headers["Cookie"] == MASK and headers["Accept"] == "json"
    obj = redact_obj({"nested": [{"access_token": "eyJx.eyJy.zzz", "id": 5}]})
    assert obj["nested"][0]["access_token"] == MASK
    assert obj["nested"][0]["id"] == 5


def test_redact_preserves_json_validity():
    """Регрессия инцидента 410608: повторная редакция сериализованного JSON
    (бандл редактирует уже редактированные дампы) не должна ломать синтаксис."""
    import json

    record = {"response": {"body": {"token": "AAAABBBBCCCC", "isPotentiallySlow": False}}}
    serialized = json.dumps(record, ensure_ascii=False, indent=2)
    once = redact(serialized)
    parsed = json.loads(once)  # не должно бросить JSONDecodeError
    assert parsed["response"]["body"]["token"] == MASK
    twice = redact(once)  # идемпотентность: вторая редакция тоже валидна
    assert json.loads(twice) == parsed


def test_redact_env_style_unquoted_still_masked():
    assert redact("password=Qwerty123") == f"password={MASK}"
