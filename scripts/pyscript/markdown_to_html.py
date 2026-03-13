"""Convert Markdown files to HTML."""

from __future__ import annotations

# argparse = a helper to read options like -i and -o from the command line.
import argparse
# re = regular expressions, a tiny pattern language for finding things in text.
import re
# sys = access to Python's runtime info (like exit codes and stderr output).
import sys
# Path = safer, nicer way to work with file paths than raw strings.
from pathlib import Path

# This is a third-party library (installed with pip) that turns Markdown into HTML.
# It's not part of Python itself, so we check for it and give a friendly message.
try:
    import markdown
except ImportError:  # pragma: no cover - runtime dependency check
    print("Missing dependency: pip install markdown", file=sys.stderr)
    sys.exit(1)


def extract_title(markdown_text: str, fallback: str) -> str:
    # Look for the first Markdown heading so we can use it as the HTML title.
    # This regex matches lines like "# Title" or "## Title".
    heading_re = re.compile(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$")
    for line in markdown_text.splitlines():
        match = heading_re.match(line)
        if match:
            return match.group(1).strip()
    return fallback


def render_markdown(markdown_text: str) -> str:
    # Turn Markdown text into HTML.
    # Extensions:
    # - fenced_code: lets you use triple backticks ``` for code blocks.
    # - tables: supports Markdown tables.
    # - toc: builds anchors for headings (useful for links and a table of contents).
    return markdown.markdown(
        markdown_text,
        extensions=[
            "fenced_code",
            "tables",
            "toc",
        ],
    )


def wrap_html(body_html: str, title: str, css_href: str | None) -> str:
    # If a CSS file was provided, include a <link> tag in the HTML head.
    # If not, leave it out so the HTML is simple.
    css_tag = f'<link rel="stylesheet" href="{css_href}">' if css_href else ""
    # Build a basic HTML page with a title and a <main> area.
    # This wraps the Markdown HTML so browsers can show it properly.
    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '  <meta charset="utf-8">',
            '  <meta name="viewport" content="width=device-width, initial-scale=1">',
            f"  <title>{title}</title>",
            f"  {css_tag}".rstrip(),
            "</head>",
            "<body>",
            "  <main>",
            body_html,
            "  </main>",
            "</body>",
            "</html>",
            "",
        ]
    )


def convert_file(input_path: Path, output_path: Path, css_href: str | None) -> None:
    # Read the Markdown file as text (UTF-8 is standard for Markdown).
    markdown_text = input_path.read_text(encoding="utf-8")
    # Use the first heading as the HTML title, or the filename if there isn't one.
    title = extract_title(markdown_text, input_path.stem)
    # Convert the Markdown into HTML (just the body content).
    body_html = render_markdown(markdown_text)
    # Wrap the body in a full HTML page.
    html = wrap_html(body_html, title, css_href)
    # Make sure the output folder exists.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Write the final HTML to disk.
    output_path.write_text(html, encoding="utf-8")


def convert_tree(input_root: Path, output_root: Path, css_href: str | None) -> None:
    # Find all Markdown files under the input folder, including subfolders.
    for md_path in input_root.rglob("*.md"):
        # Keep the same folder structure in the output.
        rel = md_path.relative_to(input_root)
        out_path = output_root / rel.with_suffix(".html")
        # Convert each file one by one.
        convert_file(md_path, out_path, css_href)


def resolve_output_file(input_path: Path, output_path: Path) -> Path:
    # If the output is a folder (or not ending in .html),
    # create a file inside that folder with the same name as the input.
    if output_path.is_dir() or output_path.suffix.lower() != ".html":
        return output_path / f"{input_path.stem}.html"
    return output_path


def parse_args() -> argparse.Namespace:
    # Build the command-line interface for this script.
    parser = argparse.ArgumentParser(description="Convert Markdown to HTML.")
    parser.add_argument(
        "-i",
        "--input",
        default="Documentation",
        help="Markdown file or folder to convert (default: Documentation).",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="site",
        help="Output folder or file (default: site).",
    )
    parser.add_argument(
        "--css",
        default="",
        help="Optional CSS file path or URL to link in output.",
    )
    return parser.parse_args()


def main() -> int:
    # Read options from the command line.
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    # Empty string means "no CSS".
    css_href = args.css.strip() or None

    # If the input path doesn't exist, stop and show an error.
    if not input_path.exists():
        print(f"Input path not found: {input_path}", file=sys.stderr)
        return 1

    # If the input is a single file, convert just that file.
    if input_path.is_file():
        out_file = resolve_output_file(input_path, output_path)
        convert_file(input_path, out_file, css_href)
        return 0

    # Otherwise, treat the input as a folder and convert all .md files inside it.
    convert_tree(input_path, output_path, css_href)
    return 0


if __name__ == "__main__":
    # This lets you run the script directly: python markdown_to_html.py
    raise SystemExit(main())
