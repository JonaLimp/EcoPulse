from sqlalchemy import create_engine
import mysql


class DatabaseManager:
    def __init__(self, db_url, host, user, password, database):
        self.engine = create_engine(db_url)

        self.connection = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        self.cursor = self.connection.cursor()


class CommentDataBaseManager(DatabaseManager):
    def run(self, comments: list[dict[str, any]]):
        """Insert Reddit comments into the database."""
        query = """
        INSERT INTO comments (id, post_id, author,
        subreddit, body, score, created_utc, created_datetime, sentiment_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE score = VALUES(score);
        """

        comments_list = comments.to_dict(orient="records")

        data = [
            (
                comment["id"],
                comment["post_id"],
                comment["author"],
                comment["subreddit"],
                comment["body"],
                comment["score"],
                comment.get("created_utc", ""),
                comment.get("created_datetime", ""),
                comment.get("sentiment_score", ""),
            )
            for comment in comments_list
        ]

        self.cursor.executemany(query, data)
        self.connection.commit()

    def close_connection(self):
        """Close database connection."""
        self.cursor.close()
        self.connection.close()


class PostDataBaseManager(DatabaseManager):
    def run(self, posts: list[dict[str, any]]):
        """Insert Reddit posts into the database."""
        query = """
        INSERT INTO posts (id, title, author, subreddit, content,
        created_datetime, score, num_comments, url, category,
        keyword, sentiment_score)
        VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE score = VALUES(score),
        num_comments = VALUES(num_comments);
        """

        posts_list = posts.to_dict(orient="records")

        data = [
            (
                post["id"],
                post["title"],
                post["author"],
                post["subreddit"],
                post["content"],
                post["created_datetime"],
                post["score"],
                post["num_comments"],
                post["url"],
                post.get("category", ""),
                post.get("keyword", ""),
                post.get("sentiment_score", ""),
            )
            for post in posts_list
        ]

        self.cursor.executemany(query, data)
        self.connection.commit()
