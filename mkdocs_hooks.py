"""MkDocs build hook: tidy pages for the website without changing the Markdown sources.

On GitHub each page starts with a "← Previous · User guide · Next" line and a
numbered title ("# 12. Explain this Map"). The website has its own navigation
and previous/next footer, so the hook drops that line and the number there.
"""
import re

_NAV_LINE = re.compile(r"^\[←[^\n]*\]\(README\.md\)[^\n]*\n+", re.MULTILINE)
_NUMBERED_TITLE = re.compile(r"^# \d+\.\s+", re.MULTILINE)


def on_page_markdown(markdown, page, config, files):
    markdown = _NAV_LINE.sub("", markdown, count=1)
    return _NUMBERED_TITLE.sub("# ", markdown, count=1)
