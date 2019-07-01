import psycopg2
import sys
import logging
import json
import boto3
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)
lambda_client = boto3.client('lambda')


class DataPrep:
    def __init__(self, event):
        self.event = event
        self.reference = self.event["reference"]
        self.period = self.event["period"]
        self.survey = self.event["survey"]
        self.instance = self.event["instance"]
        self.questionCode = "questionCode"
        self.response = "response"
        self.db_user = os.getenv("DATABASE_USER")
        self.db_pw = os.getenv("DATABASE_PASSWORD")
        self.db_host = os.getenv("DATABASE_HOST")
        self.db_name = os.getenv("DATABASE_NAME")
        self.wrangler_lambda = os.getenv("WRANGLER_NAME")

    def get_qcode_resp_from_db(self):

        # print(db_user)
        # print(db_pw)
        # print(db_host)
        try:
            conn = psycopg2.connect(database=self.db_name,
                                    user=self.db_user,
                                    password=self.db_pw,
                                    host=self.db_host,
                                    port='5432')
            cur = conn.cursor()
            # Print PostgreSQL Connection properties
            # print(conn.get_dsn_parameters(), "\n")
            cur.execute("""SELECT trim(questioncode), trim(response)
                          FROM dev01.response 
                          WHERE reference = %s AND period = %s AND survey = %s """,
                        (self.reference, self.period, self.survey))
            record = cur.fetchall()
            # print("You are connected to - ", record, "\n")
            qcode_resps = []
            for row in record:
                qcode_resps.append({self.questionCode: row[0], self.response: row[1]})
            self.event.update({'responses': qcode_resps})

        except (Exception, psycopg2.Error) as error:
            logger.error("ERROR:{}".format(error))
            sys.exit()
        finally:
            # closing database connection.
            if (conn):
                cur.close()
                conn.close()
                logger.info("DataPrep PostgreSQL connection is closed")

    def send_data_to_wrangler(self):
        try:

            # print(wrangler_lambda)
            invoke_response = lambda_client.invoke(FunctionName="function:" + self.wrangler_lambda,
                                                   InvocationType='RequestResponse',
                                                   Payload=json.dumps(self.event))
            print(invoke_response)
        except:
            logger.error("Error while calling wrangler-lambda")

