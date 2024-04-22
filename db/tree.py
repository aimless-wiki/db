import ast
import gzip
import logging
import pickle as pkl
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Generator, NamedTuple, TypedDict

import networkx as nx
import numpy as np
import requests
from tqdm import tqdm

_STRING_VALUE = r"'[^'\\]*(?:\\.[^'\\]*)*'"
_INTEGER_VALUE = r"\d+"
_FLOAT_VALUE = r"\d+\.\d+"


class CategoryLinksEntry(NamedTuple):
    child_id: int
    parent_name: str


class PageTableEntry(NamedTuple):
    page_id: int
    name: str


class CategoryEntry(NamedTuple):
    name: str
    pages: int
    subcategories: int


def _stream_lines_gzipped(
    url: str, progress: bool, description: str | None
) -> Generator[str, None, None]:
    with requests.get(url, stream=True) as r:
        raw = r.raw

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
                yield line

                if progress:
                    p_bar.update(raw.tell() - last_position)
                    last_position = raw.tell()

        if progress:
            p_bar.close()


class Asset(ABC):
    url: str

    def __init__(self, url: str) -> None:
        self.url = url

    def updated(self) -> str:
        return requests.head(self.url).headers["last-modified"]

    @abstractmethod
    def entries(self, progress: bool) -> Generator[NamedTuple, None, None]:
        pass


class CategoryLinksTable(Asset):

    pattern: re.Pattern = re.compile(
        rf"\(({_INTEGER_VALUE}),({_STRING_VALUE}),(?:{_STRING_VALUE},){{4}}'subcat'\)"
    )

    def entries(
        self, progress: bool = False
    ) -> Generator[CategoryLinksEntry, None, None]:
        for line in _stream_lines_gzipped(self.url, progress, "Category links"):
            for hit in self.pattern.findall(
                line,
            ):
                child_id, parent_name = hit

                child_id = int(child_id)
                parent_name = ast.literal_eval(parent_name)

                yield CategoryLinksEntry(child_id, parent_name)


class PageTable(Asset):

    pattern: re.Pattern = re.compile(
        rf"\(({_INTEGER_VALUE}),14,"
        rf"({_STRING_VALUE}),{_INTEGER_VALUE},"
        rf"{_INTEGER_VALUE},{_FLOAT_VALUE},"
        rf"{_STRING_VALUE},{_STRING_VALUE},"
        rf"{_INTEGER_VALUE},{_INTEGER_VALUE},"
        rf"{_STRING_VALUE},(?:{_STRING_VALUE}|NULL)\)"
    )

    def entries(self, progress: bool = False) -> Generator[PageTableEntry, None, None]:
        for line in _stream_lines_gzipped(self.url, progress, "Pages"):
            for hit in self.pattern.findall(line):
                page_id, name = hit

                page_id = int(page_id)
                name = ast.literal_eval(name)

                yield PageTableEntry(page_id, name)


class CategoryTable(Asset):

    pattern: re.Pattern = re.compile(
        rf"\({_INTEGER_VALUE},({_STRING_VALUE}),"
        rf"({_INTEGER_VALUE}),({_INTEGER_VALUE}),{_INTEGER_VALUE}\)"
    )

    def entries(self, progress: bool = False) -> Generator[CategoryEntry, None, None]:
        for line in _stream_lines_gzipped(self.url, progress, "Categories"):
            for hit in self.pattern.findall(line):
                name, pages, subcategories = hit

                name = ast.literal_eval(name)
                pages = int(pages)
                subcategories = int(subcategories)

                yield CategoryEntry(name, pages, subcategories)


class Assets(NamedTuple):
    category_links: CategoryLinksTable
    pages: PageTable
    categories: CategoryTable


def latest_assets(language: str) -> Assets:
    base_url = (
        f"https://dumps.wikimedia.org/{language}wiki/latest/{language}wiki-latest-"
    )

    return Assets(
        category_links=CategoryLinksTable(f"{base_url}categorylinks.sql.gz"),
        pages=PageTable(f"{base_url}page.sql.gz"),
        categories=CategoryTable(f"{base_url}category.sql.gz"),
    )


class CategoryAttributes(TypedDict):
    name: str
    page_count: int


class CategoryTree(nx.DiGraph):

    def __init__(
        self,
        incoming_graph_data: Any = None,
        assets: Assets | None = None,
        progress: bool = False,  # Unused if assets is not provided
        **attr,
    ) -> None:
        super().__init__(incoming_graph_data=incoming_graph_data, **attr)

        if assets is not None:
            self.add_assets(assets, progress=progress)

    def serialize(self, path: Path) -> None:
        with path.open("wb") as f:
            pkl.dump(self, f)

    @classmethod
    def deserialize(cls, path: Path) -> "CategoryTree":
        with path.open("rb") as f:
            return cls(incoming_graph_data=pkl.load(f))

    def add_assets(self, assets: Assets, progress: bool = False) -> None:
        id_to_name: dict[int, str] = {
            item.page_id: item.name for item in assets.pages.entries(progress)
        }

        name_to_id: dict[str, int] = {v: k for k, v in id_to_name.items()}

        edges: list[tuple[int, int]] = []

        for category_link in assets.category_links.entries(progress):
            linked_int_parent = name_to_id.get(category_link.parent_name, None)

            if category_link.child_id in id_to_name and linked_int_parent is not None:
                edges.append((linked_int_parent, category_link.child_id))

        id_to_page_count: dict[int, int] = {}

        for category in assets.categories.entries(progress):
            if category.name in name_to_id:
                #  Each subcategory is counted as a page.

                id_to_page_count[name_to_id[category.name]] = (
                    category.pages - category.subcategories
                )

        self.add_edges_from(edges)

        for category_id, page_count in id_to_page_count.items():
            try:
                name = id_to_name[category_id]
                self.nodes[category_id]["name"] = name
                self.nodes[category_id]["page_count"] = page_count
            except KeyError:
                logging.warning(
                    f"Category {category_id} not found in category tree, skipping."
                )

        for n in self.nodes:
            if not self.nodes[n]:
                logging.warning(f"Category {n} does not have attributes, removing.")
                self.remove_node_reconstruct(n)

    def remove_node_reconstruct(self, category_id: int):
        successors = self.successors(category_id)
        predecessors = self.predecessors(category_id)

        new_edges = tuple((p, s) for p in predecessors for s in successors)

        self.add_edges_from(new_edges)
        self.remove_node(category_id)

    def page_count_percentile(self, percentile: float) -> int:
        return np.percentile(
            [self.nodes[n]["page_count"] for n in self.nodes], percentile
        )

    def remove_by_condition(self, condition: Callable[[int, CategoryAttributes], bool]):
        to_remove = list(n for n in self.nodes if condition(n, self.nodes[n]))

        for n in to_remove:  # 427258
            self.remove_node_reconstruct(n)

    def remove_past_depth(self, root: int, depth: int):
        oriented_tree = nx.bfs_tree(self, root, depth_limit=depth)
        removed = [n for n in self.nodes if n not in oriented_tree]
        self.remove_nodes_from(removed)

    def components(self) -> Generator[set[int], None, None]:
        return nx.weakly_connected_components(self)

    def keep_only_nodes(self, nodes: set[int]) -> None:
        to_remove = [n for n in self.nodes if n not in nodes]
        self.remove_nodes_from(to_remove)

    def keep_largest_component(self) -> None:
        components = self.components()
        largest = max(components, key=len)
        self.keep_only_nodes(largest)

    def summary(self) -> dict:
        return {
            "nodes": len(self.nodes),
            "edges": len(self.edges),
            "median_page_count": self.page_count_percentile(50),
        }

    def to_dicts(self) -> Generator[dict, None, None]:

        for n in self.nodes:
            yield {
                "_id": n,
                "name": self.nodes[n]["name"],
                "successors": list(self.successors(n)),
                "predecessors": list(self.predecessors(n)),
            }
