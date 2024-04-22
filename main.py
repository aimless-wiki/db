import argparse
import os
from pathlib import Path
from pprint import pprint
from urllib.parse import quote_plus

import dotenv

from db.mongo import get_mongo_client
from db.tree import CategoryAttributes, CategoryTree, latest_assets
from db.utils import CategoryNotFound, id_for_category_str_by_lang

_graph_path = Path("graph.bytes")


def main(
    language: str, page_percentile: int, mongodb_username: str, mongodb_password: str
) -> None:

    mongodb_client = get_mongo_client(mongodb_username, mongodb_password)
    edges_collection = mongodb_client.get_database("categories").get_collection("edges")

    if _graph_path.exists():
        tree = CategoryTree.deserialize(_graph_path)
    else:
        tree = CategoryTree(assets=latest_assets(language), progress=True)
        tree.serialize(_graph_path)

    percentile_value = tree.page_count_percentile(page_percentile)

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

    print(f"{percentile_value} is page count percentile {page_percentile}")
    print(f"Excluding category ids: {excluded_ids}")

    def remove_condition(n: int, x: CategoryAttributes) -> bool:
        return x["page_count"] < percentile_value or n in excluded_ids

    tree.remove_by_condition(remove_condition)
    tree.keep_largest_component()

    pprint(tree.summary())

    edges_collection.delete_many({})
    edges_collection.insert_many(tree.to_dicts())

    print("Done")


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

    args = arg_parser.parse_args()

    language = args.language
    page_percentile = args.page_percentile
    username = quote_plus(args.mongodb_username or env_username)
    password = quote_plus(args.mongodb_password or env_password)

    main(language, page_percentile, username, password)
