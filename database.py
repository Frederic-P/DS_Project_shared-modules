"""
    UTILITY CLASS TO INTERACT WITH THE DATABASE; DEALS WITH CONNECTION AND CRUD OPERATIONS
"""
import pymysql
import pymysql.cursors
import pandas as pd

class Database:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        """Establish a connection to the database."""
        if self.connection is None:
            try:
                self.connection = pymysql.connect(
                    host=self.host,
                    port = self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    cursorclass=pymysql.cursors.DictCursor
                )
                print("Connection established")
            except pymysql.MySQLError as e:
                print(f"Error connecting to MySQL: {e}")
                self.connection = None

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            print("Connection closed")

    def start_transaction(self):
        """Start a new transaction."""
        self.connect()
        if self.connection:
            try:
                self.connection.begin()
                print("Transaction started")
            except pymysql.MySQLError as e:
                print(f"Error starting transaction: {e}")

    def commit_transaction(self):
        """Commit the current transaction."""
        if self.connection:
            try:
                self.connection.commit()
                print("Transaction committed")
            except pymysql.MySQLError as e:
                print(f"Error committing transaction: {e}")
                self.connection.rollback()

    def rollback_transaction(self):
        """Rollback the current transaction."""
        if self.connection:
            try:
                self.connection.rollback()
                print("Transaction rolled back")
            except pymysql.MySQLError as e:
                print(f"Error rolling back transaction: {e}")

    def store_sitemap_scrape(self, data):
        """Store the sitemap scrape data in the database."""
        if self.connection:
            try:
                query = "INSERT INTO `sitemaps` (`date`, `day_map`, `scrape_dt`, `size`, `selection_size`) VALUES (%s, %s, current_timestamp(), %s, %s)"
                with self.connection.cursor() as cursor:
                    cursor.execute(query, data)
                    return cursor.lastrowid 
            except pymysql.MySQLError as e:
                print(f"Error executing query: {e}")
                return None

    def store_sitemap_content(self, sitemap_pk_id, data):
        """Store the sitemap content in the database."""
        if self.connection:
            try:
                query = "INSERT INTO `image_entries` (`image_id`, `user_id`, `title`, `image_url`, `status`, `sitemap_source`) VALUES (%s, %s, %s, %s, 'Unprocessed', %s)"
                with self.connection.cursor() as cursor:
                    for _, row in data.iterrows():
                        data = [row['imid'], row['user'], row['title'], row['image_loc'], sitemap_pk_id]
                        cursor.execute(query, data)
                return True
            except pymysql.MySQLError as e:
                print(f"Error executing query: {e}")
                return None
    
    def get_last_completed_map(self): 
        """Get the last completed sitemap from the database."""
        if self.connection: 
                with self.connection.cursor() as cursor:
                    query = "SELECT date, day_map FROM `sitemaps` ORDER BY pk_id DESC LIMIT 1"
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return results

    def execute_query(self, query, params=None):
        """Execute a query and return the results."""
        self.connect()
        if self.connection:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(query, params)
                    results = cursor.fetchall()
                return results
            except pymysql.MySQLError as e:
                print(f"Error executing query: {e}")
                return None

    def execute_update(self, query, params=None):
        """Execute an update/insert/delete query."""
        self.connect()
        if self.connection:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(query, params)
                print("Query executed successfully")
            except pymysql.MySQLError as e:
                print(f"Error executing update: {e}")
                self.connection.rollback()

    def get_random_n_images(self, n=100):
        """Get a random set of n image ids from the database.
        Since the volume is too big to randomly select on millions of
        records there first happens a random selection of the sitemap
        pk_id to randomly select within a randomly chosen subset."""
        random_subset_query = "SELECT max(pk_id) AS upperlimit FROM sitemaps"
        random_images_in_subset = "SELECT * FROM image_entries WHERE sitemap_source = %s AND status = %s ORDER BY rand() LIMIT %s"
        if self.connection:
            with self.connection.cursor() as cursor: 
                cursor.execute(random_subset_query)
                maxid = cursor.fetchall()
                maxid = maxid[0]['upperlimit']
                img_query_data = [maxid, 'Unprocessed', int(n)]
                cursor.execute(random_images_in_subset, img_query_data)
                results = cursor.fetchall()
                return results


    def get_single_record(self, query, params=None):
        """Get a single record from the database."""
        results = self.execute_query(query, params)
        if results:
            return results[0]
        return None

    def get_multiple_records(self, query, params=None):
        """Get multiple records from the database."""
        return self.execute_query(query, params)

    def insert_record(self, query, params):
        """Insert a record into the database."""
        self.execute_update(query, params)

    def update_record(self, query, params):
        """Update a record in the database."""
        self.execute_update(query, params)

    def delete_record(self, query, params):
        """Delete a record from the database."""
        self.execute_update(query, params)

# Example usage:
# db = MySQLDatabase(host='localhost', user='root', password='password', database='mydatabase')
# db.connect()
# db.start_transaction()
# try:
#     db.insert_record("INSERT INTO mytable (name, value) VALUES (%s, %s)", ('name', 'value'))
#     db.update_record("UPDATE mytable SET value = %s WHERE name = %s", ('new_value', 'name'))
#     db.commit_transaction()
# except Exception as e:
#     db.rollback_transaction()
#     print(f"Transaction failed: {e}")
# db.close()
