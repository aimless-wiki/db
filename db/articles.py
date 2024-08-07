import sqlite3
import typing


_SCHEMA = """
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

_DATABASE_PATH = "articles.db"


def db_connect(path: str = _DATABASE_PATH, schema: str = _SCHEMA) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript(schema)
    return connection


class ArticlesDb:
    connection: sqlite3.Connection

    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.connection.isolation_level = None
    
    def remove_articles_in_category(self, category_id: int):
        cursor = self.connection.cursor()

        cursor.execute("BEGIN")
        
        cursor.execute(
            "DELETE FROM article_edges WHERE category_id = ?",
            (category_id,)
        )

        cursor.execute("COMMIT")
        cursor.close()
    
    def article_count(self, category_id: int) -> int:
        cursor = self.connection.cursor()

        cursor.execute(
            "SELECT COUNT(1) FROM article_edges WHERE category_id =?",
            (category_id,)
        )

        
        result = cursor.fetchone()

        if not result:
            return 0

        return result[0]
    
    def articles(self, category_id: int) -> typing.Generator[int, None, None]:
        cursor = self.connection.cursor()

        cursor.execute(
            "SELECT article_id FROM article_edges WHERE category_id =?",
            (category_id,)
        )

        while row := cursor.fetchone():
            yield row[0]

        cursor.close()
