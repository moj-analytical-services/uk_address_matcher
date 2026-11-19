from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from uk_address_matcher.labelling.generator import generate_labelling_html, main


def test_generated_html_contains_runtime_but_no_user_data(tmp_path: Path) -> None:
    output = generate_labelling_html(tmp_path / "review.html", open_browser=False)

    html = output.read_text(encoding="utf-8")

    assert "window.__UKAM_LABELLING_BOOTSTRAP__" not in html
    assert 'id="bundle-directory"' in html
    assert 'id="canonical-directory"' in html
    assert "data:application/wasm;base64," in html
    assert "data:text/javascript;base64," in html


def test_cli_generates_requested_output_path(tmp_path: Path) -> None:
    output = tmp_path / "cli-review.html"

    with patch(
        "sys.argv",
        ["ukam-labelling", "--output", str(output), "--no-browser"],
    ):
        assert main() == 0

    assert output.is_file()
