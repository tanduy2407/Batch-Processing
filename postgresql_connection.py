import psycopg2
import pyspark

def connect_db(host, port, database, user, password):
    db_params = {
    'host': host,
    'port': port,
    'database': database,
    'user': user,
    'password': password}

    conn = psycopg2.connect(**db_params)
    return conn


def create_table(connection: psycopg2.connection, table: str):
    # Create a cursor object to execute SQL commands
    cursor = connection.cursor()
    sql = '''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER
        );
        '''
    # Execute the SQL command to create the table
    cursor.execute(sql)

    # Commit the changes and close the connection
    connection.commit()
    connection.close()

def insert_data(df, table: str):
    url = "jdbc:postgresql://your_host:your_port/your_database"
    properties = {
        "user": "your_username",
        "password": "your_password",
        "driver": "org.postgresql.Driver"}
    df.write.jdbc(url=url, table=table, mode="append", properties=properties)


