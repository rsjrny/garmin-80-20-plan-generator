from pathlib import Path

from garmin_data_hub.ingest import trackpoints


def test_trackpoint_row_parser_delegates_to_givemydata(monkeypatch):
    sentinel = [(1, "ts")]

    def fake_parse(_: bytes):
        return sentinel

    monkeypatch.setattr(trackpoints, "_parse_track_rows_from_fit_bytes", fake_parse)

    assert trackpoints._track_rows_from_fit_bytes(b"fit-bytes") == sentinel


def test_activity_id_extractors_delegate_to_givemydata(monkeypatch):
    def fake_member(_: str):
        return 1234567

    def fake_zip(_: Path):
        return 7654321

    monkeypatch.setattr(trackpoints, "_parse_activity_id_from_member", fake_member)
    monkeypatch.setattr(trackpoints, "_parse_activity_id_from_zip_filename", fake_zip)

    assert trackpoints._extract_activity_id_from_member("foo.fit") == 1234567
    assert trackpoints._activity_id_from_zip_filename(Path("x.zip")) == 7654321
