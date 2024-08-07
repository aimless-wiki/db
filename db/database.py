import os
import sqlite3
import typing

from db.assets import WikiTables, PageTable, CategoryLinksTable


_SCHEMA = """
CREATE TABLE IF NOT EXISTS category_edges (
    child_id INTEGER NOT NULL,
    parent_id INTEGER NOT NULL,
    PRIMARY KEY (child_id, parent_id)
);

CREATE TABLE IF NOT EXISTS categories (
    id INTEGER NOT NULL,
    title TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS article_edges (
    article_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    PRIMARY KEY (article_id, category_id)
);

CREATE TABLE IF NOT EXISTS articles (
    id INTEGER NOT NULL,
    PRIMARY KEY (id)
);
"""

_DATABASE_PATH = "wiki_database.db"


def db_connect(path: str = _DATABASE_PATH, schema: str = _SCHEMA) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript(schema)
    return connection


class WikiDatabaseOperations:
    connection: sqlite3.Connection

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection
    
    def insert_wiki_tables(self, tables: WikiTables) -> None:
        
        prev_isolation_level = self.connection.isolation_level
        self.connection.isolation_level = None
        # Managing transactions manually is faster than 
        # using implicit transactions.

        self.insert_pages(tables.pages)
        self.create_pages_indices()

        self.insert_category_links(tables.category_links)
        self.insert_category_self_articles()
        self.create_category_edges_indices()
        self.create_article_edges_indices()

        self.connection.isolation_level = prev_isolation_level
    
    def insert_pages(self, page_table: PageTable) -> int:
        cursor = self.connection.cursor()

        cursor.execute("BEGIN")

        for page in page_table.entries():
            if page.is_article:
                cursor.execute(
                    "INSERT INTO articles (id) VALUES (?)",
                    (page.page_id,)
                )
                continue

            cursor.execute(
                "INSERT INTO categories (id, title) VALUES (?,?)",
                (page.page_id, page.page_title)
            )
        
        cursor.execute("COMMIT")

        row_count = cursor.rowcount
        cursor.close()

        return row_count
    
    def create_pages_indices(self) -> None:
        cursor = self.connection.cursor()

        cursor.execute("BEGIN")
        cursor.execute("CREATE INDEX IF NOT EXISTS article_id_index ON articles (id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS category_id_index ON categories (id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS category_name_index ON categories (title)")
        cursor.execute("COMMIT")

        cursor.close()
    
    def insert_category_links(self, category_links_table: CategoryLinksTable) -> int:
        cursor = self.connection.cursor()

        cursor.execute("BEGIN")

        for category_link in category_links_table.entries():
            if category_link.is_article:
                cursor.execute(
                    "INSERT INTO article_edges SELECT ?, c.id FROM categories c WHERE "
                    "EXISTS (SELECT 1 FROM articles a WHERE a.id = ?) AND c.title = ?",
                    (category_link.cl_from, category_link.cl_from, category_link.cl_to)
                )
                continue

            cursor.execute(
                "INSERT INTO category_edges SELECT ?, c.id FROM categories c WHERE "
                "c.title = ?",
                (category_link.cl_from, category_link.cl_to) 
            )

        cursor.execute("COMMIT")

        row_count = cursor.rowcount
        cursor.close()

        return row_count
    
    def create_category_edges_indices(self) -> None:
        cursor = self.connection.cursor()

        cursor.execute("BEGIN")
        cursor.execute("CREATE INDEX IF NOT EXISTS category_edges_child_index ON category_edges (child_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS category_edges_parent_index ON category_edges (parent_id)")
        cursor.execute("COMMIT")

        cursor.close()
    
    def create_article_edges_indices(self) -> None:
        cursor = self.connection.cursor()

        cursor.execute("BEGIN")
        cursor.execute("CREATE INDEX IF NOT EXISTS article_edges_article_index ON article_edges (article_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS article_edges_category_index ON article_edges (category_id)")
        cursor.execute("COMMIT")

        cursor.close()
    
    def insert_category_self_articles(self) -> None:
        cursor = self.connection.cursor()

        cursor.execute("BEGIN")
        cursor.execute("INSERT INTO article_edges SELECT c.id, c.id FROM categories c")
        cursor.execute("COMMIT")

        cursor.close()
    
    def category_count(self) -> int:
        cursor = self.connection.cursor()

        cursor.execute("SELECT COUNT(1) FROM categories")
        category_count = cursor.fetchone()[0]

        cursor.close()

        return category_count
    
    def article_count(self) -> int:
        cursor = self.connection.cursor()

        cursor.execute("SELECT COUNT(1) FROM articles")
        article_count = cursor.fetchone()[0]

        cursor.close()

        return article_count

    def category_edge_count(self) -> int:
        cursor = self.connection.cursor()

        cursor.execute("SELECT COUNT(1) FROM category_edges")
        edge_count = cursor.fetchone()[0]

        cursor.close()

        return edge_count

    def article_edge_count(self) -> int:
        cursor = self.connection.cursor()

        cursor.execute("SELECT COUNT(1) FROM article_edges")
        edge_count = cursor.fetchone()[0]

        cursor.close()

        return edge_count

    def summary(self) -> str:
        return (
            f"Article count:        {self.article_count()}\n"
            f"Category count:       {self.category_count()}\n"
            f"Article edge count:   {self.article_edge_count()}\n"
            f"Category edge count:  {self.category_edge_count()}"
        )
    
    def remove_categories(self, category_ids: typing.Iterable[int]) -> None:
        """
        Remove categories from the database.

        :param category_ids: The ids of the categories to remove.
        """

        cursor = self.connection.cursor()

        for category_id in category_ids:
            cursor.execute(
                "DELETE FROM categories WHERE id = ?", (category_id,)
            )

        self.connection.commit()
        cursor.close()

        self.remove_orphaned_category_edges()
    
    def remove_articles(self, article_ids: typing.Iterable[int]) -> None:
        """
        Remove articles from the database.

        :param article_ids: The ids of the articles to remove.
        """

        cursor = self.connection.cursor()

        for article_id in article_ids:
            cursor.execute(
                "DELETE FROM articles WHERE id = ?", (article_id,)
            )

        self.connection.commit()
        cursor.close()

        self.remove_orphaned_article_edges()
    
    def remove_orphaned_category_edges(self):
        """
        Remove orphaned category edges from the database.
        """

        cursor = self.connection.cursor()

        cursor.execute(
            "DELETE FROM category_edges WHERE child_id NOT IN (SELECT id FROM categories) OR parent_id NOT IN (SELECT id FROM categories)"
        )

        self.connection.commit()
        cursor.close()

    def remove_orphaned_article_edges(self):
        """
        Remove orphaned article edges from the database.
        """

        cursor = self.connection.cursor()

        cursor.execute(
            "DELETE FROM article_edges WHERE article_id NOT IN (SELECT id FROM articles) OR category_id NOT IN (SELECT id FROM categories)"
        )

        self.connection.commit()
        cursor.close()
    
    def article_count_by_percentile(self, percentile: float) -> int:
        """
        Get the article count of a category by a percentile such that a percentile of 0.5 would be the category with a median article count.

        :param percentile: The percentile of the article count to return. Is a float in the range [0, 1).
        """

        assert 0.0 <= percentile  < 1.0, "Percentile must be in the range [0, 1)."

        cursor = self.connection.cursor()

        cursor.execute(
            "SELECT c.id, (SELECT COUNT(1) - 1 FROM article_edges a WHERE a.category_id = c.id) AS article_count FROM categories c ORDER BY article_count ASC LIMIT 1 OFFSET ?",
            (int(percentile * self.category_count()),)
        )

        page_count = cursor.fetchone()[1]

        cursor.close()

        return page_count

    def categories_under_article_count(self, article_count: int) -> typing.Generator[int, None, None]:
        """
        Get the ids of all categories under a certain article count.

        :param article_count: The minimum article count of categories to return.
        :returns: A generator of (category_id, article_count) tuples.
        """

        cursor = self.connection.cursor()

        cursor.execute(
            "SELECT c.id, (SELECT COUNT(1) - 1 FROM article_edges a WHERE a.category_id = c.id) AS article_count FROM categories c WHERE article_count < ?",
            (article_count,)
        )

        while row := cursor.fetchone():
            yield row[0]

        cursor.close()
    
    def articles_in_category(self, category_id: int) -> typing.Generator[int, None, None]:
        """
        Get the ids of all articles in a category.

        :param category_id: The id of the category.
        :returns: A generator of article ids.
        """

        cursor = self.connection.cursor()

        cursor.execute(
            "SELECT article_id FROM article_edges WHERE category_id =?",
            (category_id,)
        )

        while row := cursor.fetchone():
            yield row[0]

        cursor.close()
    
    def subcategories(self, category_id: int) -> typing.Generator[int, None, None]:
        """
        Get the ids of all subcategories of a category.

        :param category_id: The id of the category.
        :returns: A generator of category ids.
        """

        cursor = self.connection.cursor()

        cursor.execute(
            "SELECT child_id FROM category_edges WHERE parent_id =?",
            (category_id,)
        )

        while row := cursor.fetchone():
            yield row[0]

        cursor.close()

    def supercategories(self, category_id: int) -> typing.Generator[int, None, None]:
        """
        Get the ids of all supercategories of a category.

        :param category_id: The id of the category.
        :returns: A generator of category ids.
        """

        cursor = self.connection.cursor()

        cursor.execute(
            "SELECT parent_id FROM category_edges WHERE child_id =?",
            (category_id,)
        )

        while row := cursor.fetchone():
            yield row[0]

        cursor.close()
    
    def category_edge_list(self) -> typing.Generator[typing.Tuple[int, int], None, None]:
        """
        Get a list of all category edges.

        :returns: A generator of (parent_id, child_id) tuples.
        """

        cursor = self.connection.cursor()

        cursor.execute(
            "SELECT parent_id, child_id FROM category_edges"
        )

        while row := cursor.fetchone():
            yield row[0], row[1]

        cursor.close()
    
    def random_category(self) -> int:
        """
        Get a random category.
        """

        cursor = self.connection.cursor()

        cursor.execute(
            "SELECT id FROM categories ORDER BY RANDOM() LIMIT 1"
        )

        category_id = cursor.fetchone()[0]

        cursor.close()

        return category_id
    
    def title(self, category_id: int) -> str:
        """
        Get the title of a category.

        :param category_id: The id of the category.
        :returns: The title of the category.
        """

        cursor = self.connection.cursor()

        cursor.execute(
            "SELECT title FROM categories WHERE id =?",
            (category_id,)
        )

        title = cursor.fetchone()[0]

        cursor.close()

        return title
    


        
    
    # TODO: Add remove category + propogate page removal methods. 
    # TODO: Perform trimming to meet storage requirements.
    # TODO: Automate trimming and upload.
    # TODO: Add testing.
