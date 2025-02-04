from sqlalchemy import create_engine
import mysql
class DatabaseManager:
    def __init__(self, db_url, host, user, password, database):
        self.engine = create_engine(
            db_url
            )
        
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def store_posts(self, posts):
        """Insert Reddit posts into the database."""
        query = """
        INSERT INTO posts (id, title, author, subreddit, content, created_datetime, score, num_comments, url, category, matched_keywords, sentiment_score)
        VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE score = VALUES(score), num_comments = VALUES(num_comments);
        """
        # data = [(post["id"], post["title"], post["author"], post["subreddit"], post["created_datetime"], post["score"], post["num_comments"], post["url"]) for post in posts]
        
        posts_list = posts.to_dict(orient="records")  
    
        data = [(post["id"], post["title"], post["author"], post["subreddit"],
            post['content'], post["created_datetime"], post["score"], post["num_comments"], 
            post["url"], post.get("category", ""), post.get("matched_keywords", ""), post.get("sentiment_score", "")) for post in posts_list]
    
        self.cursor.executemany(query, data)
        self.connection.commit()

    def store_comments(self, comments):
        """Insert Reddit comments into the database."""
        query = """
        INSERT INTO comments (id, post_id, author, subreddit, body, score, created_datetime)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE score = VALUES(score);
        """
        data = [(comment["id"], comment["post_id"], comment["author"], comment["subreddit"], comment["body"], comment["score"], comment["created_datetime"]) for comment in comments]

        self.cursor.executemany(query, data)
        self.connection.commit()

    def close_connection(self):
        """Close database connection."""
        self.cursor.close()
        self.connection.close()