#!/usr/bin/env python3
"""
tools/convertdbtsql.py

Copy all .sql files from SOURCE_DIR to DEST_DIR, rewriting any
table name in a FROM or JOIN clause into a dbt source() call,
with the replacement format easily configurable in `main()`.
Debug logging is off by default; pass the word "debug" as a third
argument to enable verbose output.

Usage:
    uv run tools/convertdbtsql.py /path/to/dbt/models /path/to/original/sql/ [debug]
"""
import argparse
import re
import sys
from pathlib import Path

_block_pattern = re.compile(r'/\*.*?\*/', flags=re.DOTALL)
_cte_pattern   = re.compile(r'(?i)\b([A-Za-z0-9_]+)\s+AS\s*\(', flags=re.IGNORECASE)
_inline_pattern = re.compile(
    r'(?P<kw>\b(?:FROM|JOIN))'
    r'(?P<space>\s+)'
    r'(?P<ident>(?:`[^`]+`|"[^"]+"|[A-Za-z0-9_]+)'
    r'(?:\.(?:`[^`]+`|"[^"]+"|[A-Za-z0-9_]+))*)',
    flags=re.IGNORECASE
)
_standalone_kw_pattern = re.compile(r'^(?P<indent>\s*)(?P<kw>FROM|JOIN)\s*$', flags=re.IGNORECASE)
_ident_line_pattern = re.compile(
    r'^(?P<indent>\s*)'
    r'(?P<ident>(?:`[^`]+`|"[^"]+"|[A-Za-z0-9_]+)'
    r'(?:\.(?:`[^`]+`|"[^"]+"|[A-Za-z0-9_]+))*)'
    r'(?P<rest>.*)$'
)
_comma_pattern = re.compile(
    r',\s*(?P<ident>(?:`[^`]+`|"[^"]+"|[A-Za-z0-9_]+)'
    r'(?:\.(?:`[^`]+`|"[^"]+"|[A-Za-z0-9_]+))*)'
)

def rewrite_sql(text: str, fmt_template: str, debug: bool=False) -> str:
    # 1) Mask block comments
    blocks = []
    def mask_block(m):
        blocks.append(m.group(0))
        return f"__BLOCK_COMMENT_{len(blocks)-1}__"
    masked = _block_pattern.sub(mask_block, text)

    # 2) Collect CTE names to skip
    cte_names = {m.group(1).lower() for m in _cte_pattern.finditer(masked)}

    lines = masked.splitlines(keepends=True)
    out = []
    prev_kw = None
    inside_from = False

    for line in lines:
        # Preserve newline
        if line.endswith("\r\n"):
            nl, body = "\r\n", line[:-2]
        elif line.endswith("\n"):
            nl, body = "\n",  line[:-1]
        else:
            nl, body = "",   line

        # Split off any "--" comment
        if "--" in body:
            code, comment = body.split("--", 1)
            comment = "--" + comment
        else:
            code, comment = body, ""

        # Handle standalone FROM/JOIN
        mkw = _standalone_kw_pattern.match(code)
        if mkw:
            kw = mkw.group("kw").upper()
            prev_kw = kw
            inside_from = (kw == "FROM")
            if debug:
                print(f"[DEBUG] Detected standalone keyword: {kw}")
            out.append(code + comment + nl)
            continue

        # Handle continued indent under a FROM block
        if inside_from and not prev_kw:
            m = _ident_line_pattern.match(code)
            if m:
                ident = m.group("ident")
                segments = [g1 or g2 or g3 for g1,g2,g3 in
                            re.findall(r'`([^`]+)`|"([^"]+)"|([A-Za-z0-9_]+)', ident)]
                table_full = ".".join(segments)
                if table_full.lower() not in cte_names:
                    j = fmt_template.format(table=table_full)
                    if debug:
                        print(f"[DEBUG] Continued FROM → '{ident}'")
                    out.append(m.group("indent") + j + m.group("rest") + comment + nl)
                    continue
            # fall through if not matched

        # Handle the first line after standalone
        if prev_kw:
            m = _ident_line_pattern.match(code)
            if m:
                ident = m.group("ident")
                segments = [g1 or g2 or g3 for g1,g2,g3 in
                            re.findall(r'`([^`]+)`|"([^"]+)"|([A-Za-z0-9_]+)', ident)]
                table_full = ".".join(segments)
                if table_full.lower() not in cte_names:
                    j = fmt_template.format(table=table_full)
                    if debug:
                        print(f"[DEBUG] Multi-line {prev_kw} → '{ident}'")
                    out.append(m.group("indent") + j + m.group("rest") + comment + nl)
                else:
                    out.append(code + comment + nl)
            else:
                out.append(code + comment + nl)
            prev_kw = None
            continue

        # Inline FROM/JOIN replacements
        def inline_repl(m):
            kw, space, ident = m.group("kw"), m.group("space"), m.group("ident")
            segments = [g1 or g2 or g3 for g1,g2,g3 in
                        re.findall(r'`([^`]+)`|"([^"]+)"|([A-Za-z0-9_]+)', ident)]
            table_full = ".".join(segments)
            if table_full.lower() in cte_names:
                return m.group(0)
            if debug:
                print(f"[DEBUG] Inline {kw.upper()} → '{ident}'")
            return f"{kw}{space}{fmt_template.format(table=table_full)}"
        code2 = _inline_pattern.sub(inline_repl, code)

        # Comma-separated tables in same FROM line
        if code.lstrip().lower().startswith("from") and "," in code2:
            def comma_repl(m):
                ident = m.group("ident")
                segments = [g1 or g2 or g3 for g1,g2,g3 in
                            re.findall(r'`([^`]+)`|"([^"]+)"|([A-Za-z0-9_]+)', ident)]
                table_full = ".".join(segments)
                if debug:
                    print(f"[DEBUG] Comma-list → '{ident}'")
                return f", {fmt_template.format(table=table_full)}"
            code2 = _comma_pattern.sub(comma_repl, code2)

        # If we hit a JOIN, exit FROM context
        if code.lstrip().upper().startswith("JOIN"):
            inside_from = False

        out.append(code2 + comment + nl)

    combined = "".join(out)

    # Restore block comments
    for i, blk in enumerate(blocks):
        combined = combined.replace(f"__BLOCK_COMMENT_{i}__", blk)

    # Remove final semicolon, then preserve newline if original ended with one
    no_semi = re.sub(r";(?=\s*\Z)", "", combined)
    if text.endswith("\n"):
        return no_semi.rstrip("\r\n") + "\n"
    else:
        return no_semi.rstrip()

def main():
    parser = argparse.ArgumentParser(
        description="convertdbtsql.py: rewrite SQL to use dbt source(), configurable format"
    )
    parser.add_argument("dest_dir",   type=Path,
                        help="dbt models folder (output)")
    parser.add_argument("source_dir", type=Path,
                        help="original SQL folder (input)")
    parser.add_argument("mode", nargs="?", default="normal",
                        choices=["normal", "debug"],
                        help="Use 'debug' to enable verbose logs")
    args = parser.parse_args()
    debug = (args.mode == "debug")

    # === FORMAT CONFIGURATION ===
    fmt_template = "{{{{ source('main', '{table}') }}}}"
    # ============================

    src, dst = args.source_dir, args.dest_dir
    if not src.is_dir():
        print(f"Error: {src} is not a directory", file=sys.stderr)
        sys.exit(1)
    dst.mkdir(parents=True, exist_ok=True)

    for sql_file in sorted(src.glob("*.sql")):
        text = sql_file.read_text(encoding="utf-8")
        new_text = rewrite_sql(text, fmt_template, debug=debug)
        (dst / sql_file.name).write_text(new_text, encoding="utf-8")
        print(f"Rewritten → {sql_file.name}")

    print("Done.")

if __name__ == "__main__":
    main()
