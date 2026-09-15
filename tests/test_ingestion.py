import requests

from ingestion import api_ingestion


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, *, headers, params, timeout):
        self.calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        if len(self.calls) == 1:
            return FakeResponse({
                "data": [{"id": "first"}],
                "pagination": {"next_page": "cursor-2"},
            })
        return FakeResponse({
            "data": [{"id": "second"}],
            "pagination": {"next_page": None},
        })


def test_fetch_tech_jobs_follows_cursor_pagination(monkeypatch):
    monkeypatch.setattr(api_ingestion, "CLEANJOBDATA_API_KEY", "secret")
    session = FakeSession()

    jobs = api_ingestion.fetch_tech_jobs("za", session=session)

    assert jobs == [{"id": "first"}, {"id": "second"}]
    assert session.calls[0]["params"]["location"] == "ZA"
    assert "next_page" not in session.calls[0]["params"]
    assert session.calls[1]["params"]["next_page"] == "cursor-2"
    assert session.calls[0]["timeout"] == 30


def test_fetch_tech_jobs_rejects_invalid_data_shape(monkeypatch):
    monkeypatch.setattr(api_ingestion, "CLEANJOBDATA_API_KEY", "secret")

    class InvalidSession:
        def get(self, *args, **kwargs):
            return FakeResponse({"data": {"id": "not-a-list"}})

    try:
        api_ingestion.fetch_tech_jobs("za", session=InvalidSession())
    except ValueError as error:
        assert "must be a list" in str(error)
    else:
        raise AssertionError("Invalid API data shape did not raise ValueError")


def test_create_http_session_configures_retries():
    session = api_ingestion.create_http_session()
    adapter = session.get_adapter("https://")

    assert adapter.max_retries.total == 4
    assert 429 in adapter.max_retries.status_forcelist