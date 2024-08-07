from dataclasses import dataclass
import json
import pathlib
import typing


import networkx as nx

from db.assets import WikiTables
from db.articles import ArticlesDb, db_connect


@dataclass
class Tree:
    articles_db: ArticlesDb
    id_to_name: typing.Dict[int, str]
    graph: nx.DiGraph 

    @classmethod
    def deserialize(cls, path: pathlib.Path):
        with open(path, "r") as f:
            in_dict = json.load(f)

        id_to_name = in_dict["id_to_name"]
        edges = in_dict["edges"]

        graph = nx.DiGraph()

        graph.add_edges_from(edges)

        return cls(
            articles_db=ArticlesDb(db_connect()),
            id_to_name=id_to_name,
            graph=graph,
        )
    
    def serialize(self, path: pathlib.Path):
        out_dict = {
            "id_to_name": self.id_to_name,
            "edges": [e for e in self.graph.edges],
        }

        with open(path, "w") as f:
            json.dump(out_dict, f, ensure_ascii=False, indent=1)
    
    @classmethod
    def from_tables(cls, tables: WikiTables):
        articles_db = ArticlesDb(db_connect())

        cursor = articles_db.connection.cursor()

        def insert_article(article_id: int):
            cursor.execute(
                "INSERT INTO articles (id) VALUES (?)",
                (article_id,)
            )
        
        def contains_article(article_id: int):
            cursor.execute(
                "SELECT 1 FROM articles WHERE id =?",
                (article_id,)
            )

            return cursor.fetchone() is not None

        def insert_article_edge(article_id: int, category_id: int):
            cursor.execute(
                "INSERT INTO article_edges (article_id, category_id) VALUES (?,?)",
                (article_id, category_id)
            )
        
        cat_id_to_name: typing.Dict[int, str] = {}

        cursor.execute("BEGIN")

        for entry in tables.pages.entries():
            if entry.is_article:
                insert_article(entry.page_id)
                continue

            cat_id_to_name[entry.page_id] = entry.page_title  # type: ignore
        
        cursor.execute("COMMIT")
        
        cat_name_to_id = {
            name: id_
            for id_, name in cat_id_to_name.items()
        }

        cursor.execute("BEGIN")

        edges: typing.List[typing.Tuple[int, int]] = []

        for entry in tables.category_links.entries():
            child_id = entry.cl_from

            try:
                parent_id = cat_name_to_id[entry.cl_to]
            except KeyError:
                continue

            if entry.is_article and contains_article(child_id):
                insert_article_edge(child_id, parent_id)
                continue

            if not entry.is_article:
                edges.append((parent_id, child_id))
        
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS article_id_idx ON article_edges(article_id)"
        )

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS category_id_idx ON article_edges(category_id)"
        )

        cursor.execute("COMMIT")
        cursor.close()

        graph = nx.DiGraph()
        graph.add_edges_from(edges)

        return cls(articles_db, cat_id_to_name, graph)

    def remove_categories_under_article_count(self, article_count: int):
        to_remove = []

        for category_id in self.graph.nodes:
            if self.articles_db.article_count(category_id) < article_count:
                to_remove.append(category_id)
        
        self.graph.remove_nodes_from(to_remove)
    
    def keep_largest_component(self):
        largest_component = max(nx.weakly_connected_components(self.graph), key=len)
        self.graph.remove_nodes_from(set(self.graph.nodes) - set(largest_component))
    
    def remove_subcategories(self, category_id: int):
        to_remove = []

        for child_id in self.graph.successors(category_id):
            to_remove.append(child_id)
        
        self.graph.remove_nodes_from(to_remove)