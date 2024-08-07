import ast
import gzip
import pathlib
import re
from abc import ABC, abstractmethod
from typing import Generator, NamedTuple, Generator, Optional

import requests
from tqdm import tqdm

_STRING_VALUE = r"'[^'\\]*(?:\\.[^'\\]*)*'"
_INTEGER_VALUE = r"\d+"
_FLOAT_VALUE = r"\d+\.\d+"


class CategoryLinksEntry(NamedTuple):
    cl_from: int
    cl_to: str
    is_article: bool


class PageTableEntry(NamedTuple):
    page_id: int
    page_title: Optional[str]
    is_article: bool


def _stream_remote_lines_gzipped(
    url: str, progress: bool, description: str | None
) -> Generator[str, None, None]:
    with requests.get(url, stream=True, timeout=1000) as r:
        raw = r.raw

        p_bar = None

        if progress:
            total = int(r.headers.get("content-length", 0))
            p_bar = tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=description,
            )

        last_position = 0

        with gzip.open(raw, mode="rt", encoding="utf-8", errors="ignore") as f:
            for line in f:
                yield line # type: ignore

                if p_bar is not None:
                    p_bar.update(raw.tell() - last_position)
                    last_position = raw.tell()

        if p_bar is not None:
            p_bar.close()


def _stream_lines_gzipped(
    path: pathlib.Path, progress: bool, description: str | None) -> Generator[str, None, None]:

    p_bar: Optional[tqdm] = None
    last_position = 0

    if progress:
        total = path.stat().st_size
        p_bar = tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=description,
        )

    with path.open("rb") as raw_file_obj:
        with gzip.open(raw_file_obj, mode="rt", encoding="utf-8", errors="ignore") as f:
            line = f.readline()

            while line:
                yield line # type: ignore

                if p_bar is not None:
                    current_position = raw_file_obj.tell()
                    p_bar.update(current_position - last_position)
                    last_position = current_position
                
                line = f.readline()
            
            if p_bar is not None:
                p_bar.close()


class Asset(ABC):

    @abstractmethod
    def stream_lines(self) -> Generator[str, None, None]:
        pass


class RemoteAsset(Asset):
    url: str
    progress: bool
    description: Optional[str]

    def __init__(self, url: str, progress: bool = True, description: Optional[str] = None) -> None:
        super().__init__()

        self.url = url
        self.progress = progress
        self.description = description
    
    def updated(self) -> str:
        return requests.head(self.url, timeout=1000).headers["last-modified"]

    def stream_lines(self) -> Generator[str, None, None]:
        return _stream_remote_lines_gzipped(self.url, self.progress, self.description)
    


class LocalAsset(Asset):
    path: pathlib.Path
    progress: bool
    description: Optional[str]

    def __init__(self, path: pathlib.Path, progress: bool = True, description: Optional[str] = None) -> None:
        super().__init__()

        self.path = path
        self.progress = progress
        self.description = description
    
    def stream_lines(self) -> Generator[str, None, None]:
        return _stream_lines_gzipped(self.path, self.progress, self.description)
    
class WikiTable(ABC):

    asset: Asset

    def __init__(self, asset: Asset) -> None:
        self.asset = asset

    @abstractmethod
    def entries(self) -> Generator[NamedTuple, None, None]:
        pass


class CategoryLinksTable(WikiTable):

    pattern: re.Pattern = re.compile(
        rf"\(({_INTEGER_VALUE}),({_STRING_VALUE}),(?:{_STRING_VALUE},){{4}}'((?:page)|(?:subcat))'\)"
    )
    
    def entries(
        self
    ) -> Generator[CategoryLinksEntry, None, None]:
        for line in self.asset.stream_lines():
            for hit in self.pattern.findall(
                line,
            ):
                cl_from, cl_to, article_or_subcat = hit

                is_article: bool = article_or_subcat == "page"

                cl_from = int(cl_from)
                cl_to = ast.literal_eval(cl_to)

                yield CategoryLinksEntry(cl_from=cl_from, cl_to=cl_to, is_article=is_article)


class PageTable(WikiTable):

    pattern: re.Pattern = re.compile(
        rf"\(({_INTEGER_VALUE}),((?:14)|(?:0)),"
        rf"({_STRING_VALUE}),0,"
        rf"{_INTEGER_VALUE},{_FLOAT_VALUE},"
        rf"{_STRING_VALUE},{_STRING_VALUE},"
        rf"{_INTEGER_VALUE},{_INTEGER_VALUE},"
        rf"{_STRING_VALUE},(?:{_STRING_VALUE}|NULL)\)"
    )

    def entries(self) -> Generator[PageTableEntry, None, None]:
        for line in self.asset.stream_lines():
            for hit in self.pattern.findall(line):
                page_id, namespace, name = hit

                page_id = int(page_id)
                is_article = namespace == "0"
                page_title = None if is_article else ast.literal_eval(name)

                yield PageTableEntry(page_id=page_id, page_title=page_title, is_article=is_article)


class WikiTables(NamedTuple):
    category_links: CategoryLinksTable
    pages: PageTable
    
    @classmethod
    def from_paths(cls, category_links_path: pathlib.Path, pages_path: pathlib.Path, progress: bool = True) -> "WikiTables":
        return cls(
            category_links=CategoryLinksTable(LocalAsset(category_links_path, progress=progress, description="Category Links")),
            pages=PageTable(LocalAsset(pages_path, progress=progress, description="Pages")),
        )
    
    @classmethod
    def from_urls(cls, category_links_url: str, pages_url: str, progress: bool = True) -> "WikiTables":
        return cls(
            category_links=CategoryLinksTable(RemoteAsset(category_links_url, progress=progress, description="Category Links")),
            pages=PageTable(RemoteAsset(pages_url, progress=progress, description="Page Table")),
        )
    
    @classmethod
    def from_latest(cls, language: str, progress: bool = True) -> "WikiTables":
        base_url = (
            f"https://dumps.wikimedia.org/{language}wiki/latest/{language}wiki-latest-"
        )

        category_links_url = f"{base_url}categorylinks.sql.gz"
        page_table_url = f"{base_url}page.sql.gz"

        return cls(
            category_links=CategoryLinksTable(RemoteAsset(category_links_url, progress=progress, description="Category Links")),
            pages=PageTable(RemoteAsset(page_table_url, progress=progress, description="Page Table")),
        )
