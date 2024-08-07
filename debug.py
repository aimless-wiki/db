from itertools import chain
from pathlib import Path
import typing

import networkx as nx

from db.assets import WikiTables
from db.utils import id_for_category_str_by_lang
from db.database import WikiDatabaseOperations, db_connect
from db.tree import Tree

import logging


def excluded_categories() -> typing.Generator[int, None, None]:
    with open("./excluded_names.txt", 'r') as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            yield id_for_category_str_by_lang("en", line, "en")


if __name__ == "__main__":
    # Redirect logging to log.txt, overwriting it if it already exists.
    logging.basicConfig(filename="log.txt", level=logging.INFO, filemode="w")

    wiki_tables = WikiTables.from_paths(
        Path("store/enwiki-latest-categorylinks.sql.gz"),
        Path("store/enwiki-latest-page.sql.gz"),
    )

    tree = Tree.deserialize(Path("tree.json"))

    