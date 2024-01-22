import csv
import psycopg2
import psycopg2.extras

DB_USER = "postgres"
DB_PASS = "postgres"
DB_DATABASE = "postgres"
DB_HOST = "localhost"

conn_string = "host={0} user={1} dbname={2} password={3}".format(DB_HOST, DB_USER,
                                                                 DB_DATABASE, DB_PASS)
connection = psycopg2.connect(conn_string)
cursor = connection.cursor()

with open("roc_stations_20240112.tsv") as file:
    tsv_file = csv.reader(file, delimiter="\t")

    # printing data line by line
    for line in tsv_file:
        for i in range(len(line)):
            line[i].replace("\t", "")
            if line[i] == '(null)' or line[i] == '':
                line[i] = None
        try:
            cursor.execute(
                "INSERT INTO podcast.station VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                line)
            connection.commit()
        except Exception as e:
            print(e)
            print(line)
            pass
