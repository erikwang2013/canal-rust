#!/usr/bin/env python3
"""Generate site/search-index.json from the static HTML pages."""
from html.parser import HTMLParser
from pathlib import Path
import html
import json
import re

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / 'site'


class MainExtractor(HTMLParser):
    SKIP = {'script', 'style', 'noscript', 'svg', 'button', 'input', 'select', 'textarea'}
    HEADINGS = {'h1', 'h2', 'h3'}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.in_main = 0
        self.skip = 0
        self.heading = 0
        self.heading_parts = []
        self.headings = []
        self.text = []

    def handle_starttag(self, tag, attrs):
        if tag == 'main':
            self.in_main += 1
            return
        if not self.in_main:
            return
        if tag in self.SKIP:
            self.skip += 1
        elif not self.skip and tag in self.HEADINGS:
            self.heading += 1
            self.heading_parts = []

    def handle_endtag(self, tag):
        if tag == 'main':
            self.in_main = max(0, self.in_main - 1)
            return
        if not self.in_main:
            return
        if tag in self.SKIP:
            self.skip = max(0, self.skip - 1)
        elif not self.skip and tag in self.HEADINGS and self.heading:
            self.heading -= 1
            text = ' '.join(''.join(self.heading_parts).split())
            if text:
                self.headings.append(text)
            self.heading_parts = []

    def handle_data(self, data):
        if not self.in_main or self.skip:
            return
        self.text.append(data)
        if self.heading:
            self.heading_parts.append(data)


def build_doc(path):
    html_text = path.read_text(encoding='utf-8')
    lang_match = re.search(r'<html[^>]*\blang="([^"]+)"', html_text)
    title_match = re.search(r'<title>([^<]*)</title>', html_text)
    desc_match = re.search(r'<meta name="description" content="([^"]*)"', html_text)
    parser = MainExtractor()
    parser.feed(html_text)
    text = ' '.join(''.join(parser.text).split())
    if len(text) > 12000:
        text = text[:12000]
    return {
        'url': path.name,
        'lang': 'en' if (lang_match and lang_match.group(1).lower().startswith('en')) else 'zh',
        'title': html.unescape(title_match.group(1)) if title_match else path.stem,
        'description': html.unescape(desc_match.group(1)) if desc_match else '',
        'headings': parser.headings[:30],
        'text': text,
    }


def main():
    docs = []
    for path in sorted(SITE.glob('*.html')):
        if path.name in ('404.html', '404.en.html'):
            continue
        docs.append(build_doc(path))
    # Home pages first, then Chinese/English pages.
    order = {'index.html': 0, 'index.en.html': 1}
    docs.sort(key=lambda d: (order.get(d['url'], 2), d['url']))
    payload = {'generated': '2026-10-28', 'docs': docs}
    (SITE / 'search-index.json').write_text(
        json.dumps(payload, ensure_ascii=False, indent=1) + '\n',
        encoding='utf-8',
    )
    print(f'wrote {SITE / "search-index.json"} ({len(docs)} docs)')


if __name__ == '__main__':
    main()
