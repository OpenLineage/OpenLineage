# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import pathlib

from bs4 import BeautifulSoup

SPHINX_ROOT_DIR = pathlib.Path(__file__).parent.resolve()
SPHINX_BUILD_DIR = SPHINX_ROOT_DIR / pathlib.Path("_build/html/source")
DOCS_OUTPUT_DIR = SPHINX_ROOT_DIR / "converted"


def html_to_mdx(html: str) -> str:
    # Because the HTML uses `class` and has `{}` in it, it isn't valid
    # MDX. As such, we use React's dangerouslySetInnerHTML.
    return f"""

<div dangerouslySetInnerHTML={{{{__html: {json.dumps(html)}}}}}></div>

"""


def bs4_to_mdx(soup: BeautifulSoup, sidebar: BeautifulSoup) -> str:
    return html_to_mdx(str(sidebar) + str(soup))


def convert_html_to_md(html_file: pathlib.Path) -> str:
    html = html_file.read_text()
    soup = BeautifulSoup(html, "html.parser")

    body = soup.find("main").find("div", {"class": "bd-article-container"})
    sidebar = soup.find("main").find("div", {"class": "bd-sidebar-secondary"})
    article = body.find("article")

    # Remove all the "permalink to this heading" links.
    for link in article.find_all("a", {"class": "headerlink"}):
        link.decompose()

    # Remove the trailing " – " from arguments that are missing  # noqa: RUF003
    # a description.
    for item in article.select("dl.field-list dd p"):
        # Note - that's U+2013, not a normal hyphen.
        if str(item).endswith(" – </p>"):  # noqa: RUF001
            parent = item.parent
            new_item = BeautifulSoup(str(item)[:-7] + "</p>", "html.parser")
            parent.p.replace_with(new_item)

    # Extract title from the h1.
    title_element = article.find("h1")
    title = title_element.text
    print(title)
    title_element.decompose()

    md_meta = f"""---
title: {title}
---\n\n"""

    return md_meta + bs4_to_mdx(article, sidebar)


def main():
    DOCS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for doc in SPHINX_BUILD_DIR.glob("**/*.html"):
        md = convert_html_to_md(doc)

        outfile = DOCS_OUTPUT_DIR / doc.relative_to(SPHINX_BUILD_DIR).with_suffix(".md")
        outfile.parent.mkdir(parents=True, exist_ok=True)
        outfile.write_text(md)

        print(f"Generated {outfile}")


if __name__ == "__main__":
    main()
