from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table="yt_api"

def get_conn_cursor():
    hook=PostgresHook(postgres_conn_id="postgres_db_yt_elt",database='elt_db') #postgres_db_yt_elt yaml定义，elt_db为env里定义的
    conn =hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor) #hook  → Airflow 的数据库连接工具
    return conn, cur                                 # conn  → 真正连上 PostgreSQL 的连接对象
                                                     #cur   → 用来执行 SQL 这里RealDictCursor让 cursor 查询出来的结果变成“字典格式”。
    # cur.execute("SELECT * FROM table")

def close_conn_cursor(conn,cur):
    cur.close()
    conn.close()

def create_schema(schema):
    conn,cur=get_conn_cursor()
    schema_sql=f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn,cur)


def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
                );
            """
    else:
        table_sql = f"""
                  CREATE TABLE IF NOT EXISTS {schema}.{table} (
                      "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                      "Video_Title" TEXT NOT NULL,
                      "Upload_Date" TIMESTAMP NOT NULL,
                      "Duration" TIME NOT NULL,
                      "Video_Type" VARCHAR(10) NOT NULL,
                      "Video_Views" INT,
                      "Likes_Count" INT,
                      "Comments_Count" INT    
                  ); 
              """

    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def get_video_ids(cur, schema):
    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()
    video_ids = [row["Video_ID"] for row in ids]
    return video_ids

 



