import argparse
import os
import re
from pathlib import Path
from pprint import pprint
from typing import Optional
from urllib.parse import quote_plus

import dotenv
from pymongo.collection import Collection

from db.mongo import get_mongo_client
from db.tree import CategoryAttributes, CategoryTree, latest_assets
from db.utils import CategoryNotFound, id_for_category_str_by_lang

_graph_path = Path("graph.bytes")


def main(
    language: str,
    page_percentile: int,
    mongodb_username: str,
    mongodb_password: str,
    dry_run: bool,
    progress: bool,
    debug: bool,
) -> None:

    edges_collection: Optional[Collection] = None

    if not dry_run:
        mongodb_client = get_mongo_client(mongodb_username, mongodb_password)
        edges_collection = mongodb_client.get_database("categories").get_collection(
            "edges"
        )

    if _graph_path.exists():
        tree = CategoryTree.deserialize(_graph_path)
    else:
        tree = CategoryTree(assets=latest_assets(language), progress=progress)
        tree.serialize(_graph_path)

    excluded_ids = set()

    with open("excluded_names.txt", "r") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            try:
                excluded_ids.add(id_for_category_str_by_lang(language, line, "en"))
            except CategoryNotFound:
                continue

    print(f"Excluding category ids: {excluded_ids}")

    def remove_no_reconstruct(_: int, x: CategoryAttributes) -> bool:
        name: str = x["name"]
        name = name.lower()

        exclude_pattern = re.compile(
            r"^[a-z]+-(?:class|importance)|^redirect|(?:stub|template)s?$|wikiproject|^wikipedia")

        return exclude_pattern.search(name) is not None

    tree.remove_by_condition(remove_no_reconstruct, reconstruct=False)

    percentile_value = tree.page_count_percentile(page_percentile)
    print(f"{percentile_value} is page count percentile {page_percentile}")

    def remove_reconstruct(n: int, x: CategoryAttributes) -> bool:
        return x["page_count"] < percentile_value or n in excluded_ids

    tree.remove_by_condition(remove_reconstruct, reconstruct=True)

    tree.keep_largest_component()

    for node in tree.nodes:
        tree.nodes[node]["name"] = tree.nodes[node]["name"].replace("_", " ")

    pprint(tree.summary())

    if not dry_run and edges_collection is not None:
        edges_collection.delete_many({})
        edges_collection.insert_many(tree.to_dicts())

    print("Done")

    if debug:
        try:
            from IPython import embed

            def write_names() -> None:
                names = []

                for _n in tree.nodes:
                    names.append(tree.nodes[_n]["name"])

                names.sort()

                with open("names.txt", "w") as names_f:
                    for name in names:
                        names_f.write(f"{name}\n")

            embed()
        except ImportError:
            print("IPython not installed, not entering debug session.")


if __name__ == "__main__":

    dotenv.load_dotenv(dotenv.find_dotenv())

    env_username = os.environ.get("MONGODB_USERNAME", "")
    env_password = os.environ.get("MONGODB_PASSWORD", "")

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("language", type=str, help="Language code")

    arg_parser.add_argument(
        "--page_percentile",
        type=int,
        default=75,
        help="Page count percentile cutoff",
    )

    arg_parser.add_argument(
        "--mongodb_password",
        type=str,
        default="",
        help="MongoDB password",
    )

    arg_parser.add_argument(
        "--mongodb_username",
        type=str,
        default="",
        help="MongoDB username",
    )

    arg_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run, do not write to database",
        default=False,
    )

    arg_parser.add_argument(
        "--progress",
        action="store_true",
        help="Show progress",
        default=False,
    )

    arg_parser.add_argument(
        "--debug",
        action="store_true",
        help="Open IPython shell on completion.",
        default=False,
    )

    args = arg_parser.parse_args()

    _language = args.language
    _page_percentile = args.page_percentile
    _username = quote_plus(args.mongodb_username or env_username)
    _password = quote_plus(args.mongodb_password or env_password)
    _dry_run = args.dry_run
    _progress = args.progress
    _debug = args.debug

    main(_language, _page_percentile, _username, _password, _dry_run, _progress, _debug)
