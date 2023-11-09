import psycopg2
import mysql.connector
import json
import pandas as pd
import os
# from azure.storage.blob import BlobClient

class MindTickle:
    def __init__(self, input_config):
        self.mysql_db = input_config['mysql']['db']
        self.mysql_user = input_config['mysql']['user']
        self.mysql_host = input_config['mysql']['host']
        self.mysql_pass = input_config['mysql']['pass']
        self.pgs_db = input_config['pgs']['db']
        self.pgs_user = input_config['pgs']['user']
        self.pgs_host = input_config['pgs']['host']
        self.pgs_pass = input_config['pgs']['pass']
        self.pgs_port = input_config['pgs']['port']

    # def blob_connection(self):
    #     blob = BlobClient.from_connection_string(
    #         conn_str='my_conn_str',
    #         container_name='my_container_name',
    #         blob_name='my_blob_name')
    # return blob

    def pgs_connection(self):
        try:
            # establishing the connection
            conn = psycopg2.connect(database=self.pgs_db,
                                    user=self.pgs_user,
                                    host=self.pgs_host,
                                    password=self.pgs_pass,
                                    port=self.pgs_port)
        except:
            print('Incorrect user/pass')
            return
        return conn

    def mysql_connection(self):
        try:
            # establishing the connection
            conn = mysql.connector.connect(user=self.mysql_user,
                                           password=self.mysql_pass,
                                           host=self.mysql_host,
                                           database=self.mysql_db)
        except:
            print('Incorrect user/pass')
            return
        return conn

    def query_exec(self, conn, sql):
        # Creating a cursor object using the cursor() method
        cur = conn.cursor()
        try:
            # Execute a command: create datacamp_courses table
            cur.execute(sql)
        except:
            print('Check sql query!!')
            return
        return cur

    def main(self):
        mysql_conn = self.mysql_connection()
        pgs_conn = self.pgs_connection()

        sql1 = '''select user_id, user_name from mindtickle_users where active_status = 'active';'''
        sql2 = '''select user_id, completion_date, count(1) from lesson_completion where DATEDIFF(CURRENT_TIMESTAMP, completion_date) <= 50 group by user_id, completion_date;'''

        #Creating df of our query results
        lesson_data = pd.DataFrame(self.query_exec(mysql_conn, sql2).fetchall())
        lesson_data.columns = ['user_id', 'completion_date', 'count']

        user_data = pd.DataFrame(self.query_exec(pgs_conn, sql1).fetchall())
        user_data.columns = ['user_id', 'user_name']

        # Joining two df
        data = pd.merge(user_data, lesson_data, left_on='user_id', right_on='user_id', how='inner')

        #saving output file
        data.to_csv('output_fn.csv')

        #save file to azure blob storage
        # blob = self.blob_connection()
        # blob.upload_blob(data.to_csv('output_fn.csv'))
        print('File generated successfully!!!')

        # closing open connection
        mysql_conn.close()
        pgs_conn.close()

if __name__ == '__main__':
    input = json.load(open('config.json'))
    data = MindTickle(input)
    data.main()