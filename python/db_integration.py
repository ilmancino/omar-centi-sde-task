from dwh_orm import Base
from my_utils import get_my_logger
from sqlalchemy import create_engine, text

import os, pandas as pd


DB_PASSWORD = os.environ['POSTGRES_PASSWORD']
DB_USER = os.environ['POSTGRES_USER']

conn_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@db:5432/{DB_USER}'

_logger = get_my_logger(__name__)

engine = create_engine(conn_string, echo=False)


PUBLICATIONS_COLUMNS = [
    'published_date_id',
    'published_date',
    'bestsellers_date_id',
    'bestsellers_date',
    'published_date_description',
    'list_id',
    'book_sk',
    'publisher',
    'rank',
    'rank_last_week',
    'weeks_on_list',
    ]

LISTS_COLUMNS = [
    'list_id',
    'list_name',
    'list_name_encoded',
    'display_name',
    'updated',
    'list_image',
    'list_image_width',
    'list_image_height',
    ]

BOOKS_COLUMNS = [
    'book_sk',
    'title',
    'author',
    'primary_isbn10',
    'primary_isbn13',
    'description',
    'contributor',
    'contributor_note',
    'created_date',
    'updated_date',
    'price',
    'age_group',
    'book_image',
    'book_image_width',
    'book_image_height',
    ]


def create_tables():
    #Base = declarative_base()
    Base.metadata.create_all(engine)


def insert_into_table(df: pd.DataFrame, table: str, if_exists='append'):
    """
    A wrapper to the pandas function which inserts a Dataframes's
    content into a table.

    :param df: Name of the target table
    :type df: pandas.Dataframe
    :param table: Name of the target table
    :type table: str
    :param if_exists: Method to use if the table already exists
    :type if_exists: Literal['fail', 'replace', 'append']
    """

    # insert data into the DB
    conn = engine.connect()

    try:
        # write publications to DB 
        df.to_sql(table, con=conn, if_exists=if_exists, 
                index=False)
        conn.commit()
    finally:
        conn.close()


def merge_tables(into_table, from_table, fields, replace_index):
    """
    A PostgreSQL way to merge a records from one table into another,

    :param into_table: Name of the target table
    :type into_table: str
    :param from_table: Name of the table where data comes from
    :type from_table: str
    :param fields: The names of the columns to fill in the table
    :type fields: iterable of strings
    :param replace_index: the column to act as identifier of the records
        during the MERGE operation
    :type replace_index: strings
    """

    aliased_fields_fragment = ", ".join([f'f.{i}' for i in fields])
    fields_fragment = ", ".join(fields)
    update_fields_fragment = ", ".join([f'{i} = f.{i}' for i in fields
                                        if i != replace_index])

    try:
        sql_flagged = f'''
            WITH flagged AS
                (
                SELECT
                    {aliased_fields_fragment},
                    CASE
                        WHEN i.{replace_index} IS NOT NULL THEN TRUE
                        ELSE FALSE
                    END AS found_flag
                FROM {from_table} f LEFT JOIN {into_table} i
                    ON f.{replace_index} = i.{replace_index}
                ),'''

        sql_update = f'''
            upd AS (
                UPDATE {into_table} i SET
                        {update_fields_fragment}
                FROM flagged f
                WHERE f.found_flag = TRUE AND f.{replace_index} = i.{replace_index}
                )
            '''

        sql_insert = f'''
            INSERT INTO {into_table}({fields_fragment})
            SELECT  {fields_fragment}
            FROM flagged
            WHERE found_flag = FALSE;
            '''

        sql = f'''
            {sql_flagged}
            {sql_update}
            {sql_insert}
            '''

        conn = engine.connect() 

        conn.execute(text(sql))
        conn.commit()
    finally:
        conn.close()


def sql_to_csv(query_resource: str, params: dict = None):
    """
    Provides an easy way of dumping the results of an SQL
    query into a csv file.

    :param query_resource: The local sql file containing
        the query
    :type query_resource: str
    :param params: Name of the target table
    :type params: Literal['1-top_3_presence', '1-top_3_presence_v2',
                          '2-list_unique_books', '3-publishers_ranking',
                          '4-teams_buying_books']
    """

    df = pd.DataFrame()
    work_path = os.environ['WORK_PATH']

    sql_file = f'{work_path}/sql_scripts/{query_resource}.sql'
    csv_file = f'{work_path}/{query_resource}.csv'
    
    with open(sql_file, 'r') as file:
        query = file.read()

    conn = engine.connect()
    try:
        df = pd.read_sql_query(sql=query, con=conn, params=params)
    finally:
        conn.close()

    with open(csv_file, 'w') as output:
        _logger.info(f"Writing report into: {output.name}")
        df.to_csv(path_or_buf=output.name, header=True, index=False, encoding=None,
                chunksize=None, date_format=None)
