from utils.analytics import Analytics


def test_track_without_identify_omits_user_id(monkeypatch):
    captured = {}

    def fake_track(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("utils.analytics.rudder_analytics.track", fake_track)

    analytics = Analytics()
    analytics.track("mcp_tools/call_success", {"k": "v"})

    assert captured.get("anonymous_id") is not None
    assert captured.get("event") == "mcp_tools/call_success"
    assert "user_id" not in captured


def test_track_with_identify_includes_user_id(monkeypatch):
    captured = {}

    def fake_track(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("utils.analytics.rudder_analytics.track", fake_track)

    analytics = Analytics()
    analytics.identify("user-123", {"email": "a@b.com"})
    analytics.track("mcp_tools/call_success", {"k": "v"})

    assert captured.get("user_id") == "user-123"
