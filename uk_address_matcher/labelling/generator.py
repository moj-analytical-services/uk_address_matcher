from __future__ import annotations

import argparse
import base64
import mimetypes
import re
import webbrowser
from importlib.resources import files
from pathlib import Path
from urllib.parse import unquote


def _static_root() -> Path:
    package_root = Path(str(files("uk_address_matcher.labelling.app")))
    static_root = package_root / "static"
    if (static_root / "index.html").is_file():
        return static_root
    raise FileNotFoundError(
        "The labelling app assets are not built. Run `npm ci && npm run build` first."
    )


def _data_url(path: Path) -> str:
    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:{content_type};base64,{encoded}"


def _inline_app(static_root: Path) -> str:
    html = (static_root / "index.html").read_text(encoding="utf-8")
    script_match = re.search(
        r'<script type="module" crossorigin src="([^"]+)"></script>', html
    )
    style_match = re.search(r'<link rel="stylesheet" crossorigin href="([^"]+)">', html)
    if script_match is None or style_match is None:
        raise ValueError("Built labelling app is missing its JavaScript or CSS asset")

    script_path = static_root / unquote(script_match.group(1).removeprefix("./"))
    script = script_path.read_text(encoding="utf-8")
    for asset_path in (static_root / "assets").iterdir():
        if asset_path.suffix in {".js", ".wasm"} and asset_path.name in script:
            script = script.replace(asset_path.name, _data_url(asset_path))
    script = script.replace("</script", "<\\/script")
    style_path = static_root / unquote(style_match.group(1).removeprefix("./"))
    style = style_path.read_text(encoding="utf-8")
    html = html.replace(
        script_match.group(0),
        f'<script type="module">{script}</script>',
        1,
    )
    return html.replace(
        style_match.group(0),
        f"<style>{style}</style>",
        1,
    )


def generate_labelling_html(
    output_path: str | Path = "ukam_labelling.html",
    *,
    open_browser: bool = True,
) -> Path:
    """Render a shareable labelling app without embedding user data."""
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(_inline_app(_static_root()), encoding="utf-8")
    url = output.as_uri()
    print(f"UKAM labelling tool: {url}", flush=True)  # noqa: T201
    if open_browser:
        webbrowser.open(url)
    return output


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a shareable UKAM labelling HTML file"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("ukam_labelling.html"),
        help="Path for the rendered HTML file",
    )
    parser.add_argument("--no-browser", action="store_true")
    arguments = parser.parse_args()
    generate_labelling_html(
        arguments.output,
        open_browser=not arguments.no_browser,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
