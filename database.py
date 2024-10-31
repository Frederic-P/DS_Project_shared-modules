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

    def update_processed_entry(self, unix_upload_time, days_online, views, tagcount, groupcount, statusflag, pkid, stored_at):
        query = """UPDATE image_entries 
        SET state = %s, 
        groupcount = %s, 
        tagcount = %s, 
        uploaddate = %s, 
        days_online_at_scrape = %s, 
        viewcount = %s, 
        storage_location = %s
        WHERE pk_id = %s"""
        args = [statusflag, groupcount, tagcount, unix_upload_time, days_online, views, stored_at, pkid]
        res = self.execute_query(query, args)
        
    

    def save_groups(self, groups, pkid):
        for group in groups: 
            title = group['title']
            id = group['id']
            exists_query = "SELECT id FROM `flickr_groups` WHERE flickr_group_id = %s"
            exists_param = [id]
            exists_result = self.execute_query(exists_query, exists_param)
            if len(exists_result) == 0:
                #group does not exist yet; create it!
                create_query = "INSERT INTO `flickr_groups` (`group_name`, `flickr_group_id`) VALUES (%s, %s)"
                create_params = [title, id]
                groupid_pk = self.execute_query(create_query, create_params, True)
            else:
                groupid_pk = exists_result[0]['id']
            relation_query = "INSERT INTO `grouprelations` (`pk_of_group`,`pk_of_img_entry`) VALUES (%s, %s)"
            relation_params = [groupid_pk, pkid]
            self.execute_query(relation_query, relation_params)

    def save_tags(self, tags, pkid):
        for tag in tags['tags']:
            raw_tag = tag['raw']
            norm_tag = tag['_content']
            exists_query = "SELECT id FROM `normalized_tags` WHERE normalized = %s"
            exists_param = [norm_tag]
            exists_result = self.execute_query(exists_query, exists_param)
            if len(exists_result) == 0:
                #tag does not exist yet; create it!
                create_query = "INSERT INTO `normalized_tags` (`normalized`, `bad_tag`) VALUES (%s, %s)"
                create_params = [norm_tag, 0]
                tag_pk = self.execute_query(create_query, create_params, True)
            else:
                tag_pk = exists_result[0]['id']
            relation_query = "INSERT INTO `tagrelations` (`pk_of_norm_tag`,`pk_of_img_entry`) VALUES (%s, %s)"
            relation_params = [tag_pk, pkid]
            self.execute_query(relation_query, relation_params)
            #with the normalized PK known: also store the spelling variant: 
            #   variants are ai_ci collated so there's some wiggleroom.
            variant_exists_query = "SELECT count(*) as f FROM tag_variants WHERE normalized_tag_id = %s AND tag_as_written = %s"
            variant_exists_param = [tag_pk, raw_tag]
            variant_exists_result = self.execute_query(variant_exists_query, variant_exists_param)
            if variant_exists_result[0]['f'] == 0:
                create_variant_query = "INSERT INTO `tag_variants` (`normalized_tag_id`,`tag_as_written`) VALUES (%s, %s)"
                self.execute_query(create_variant_query, variant_exists_param)

    def start_transaction(self):
        """Start a new transaction."""
        self.connect()
        if self.connection:
            try:
                self.connection.begin()
            except pymysql.MySQLError as e:
                print(f"Error starting transaction: {e}")

    def commit_transaction(self):
        """Commit the current transaction."""
        if self.connection:
            try:
                self.connection.commit()
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
                query = "INSERT INTO `image_entries` (`image_id`, `user_id`, `title`, `image_url`, `state`, `sitemap_source`) VALUES (%s, %s, %s, %s, 'unprocessed', %s)"
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

    def execute_query(self, query, params=None, return_rowid = False):
        """Execute a query and return the results."""
        self.connect()
        if self.connection:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(query, params)
                    results = cursor.fetchall()
                if return_rowid:
                    return cursor.lastrowid
                else:
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
            except pymysql.MySQLError as e:
                print(f"Error executing update: {e}")
                self.connection.rollback()

    def get_random_n_images(self, sitemap_id, n=100):
        """Get a random set of n image ids from the database.
        Since the volume is too big to randomly select on millions of
        records there first happens a random selection of the sitemap
        pk_id to randomly select within a randomly chosen subset."""
        random_images_in_subset = "SELECT pk_id, image_id, image_url FROM image_entries WHERE sitemap_source = %s AND state = %s AND is_duplicate = 0 ORDER BY rand() LIMIT %s"
        if self.connection:
            with self.connection.cursor() as cursor: 
                img_query_data = [sitemap_id, 'unprocessed', int(n)]
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
