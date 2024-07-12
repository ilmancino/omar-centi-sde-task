from api_integration import fetch_data
from db_integration import (PUBLICATIONS_COLUMNS, LISTS_COLUMNS, BOOKS_COLUMNS,
                            create_tables, insert_into_table, merge_tables, sql_to_csv)
from my_utils import get_my_logger
from time import sleep

import pandas as pd

_logger = get_my_logger(__name__)


START_DATE = '2021-01-01'
END_DATE = '2023-12-31'


# TODO add documentation for the functions in here
def generate_reports():
    _logger.info(f"Generating report 1-top_3_presence")
    sql_to_csv('1-top_3_presence',
               params={'start_date': '2022-01-01', 'end_date': '2022-12-31'}
               )
    _logger.info(f"Generating report 1-top_3_presence_v2")
    sql_to_csv('1-top_3_presence_v2',
               params={'start_date': '2022-01-01', 'end_date': '2022-12-31'}
               )

    _logger.info(f"Generating report 2-list_unique_books")
    sql_to_csv('2-list_unique_books')

    _logger.info(f"Generating report 3-publishers_ranking")
    sql_to_csv('3-publishers_ranking',
               params={'start_date': '2021-01-01', 'end_date': '2023-12-31'}
               )
 
    _logger.info(f"Generating report 4-teams_buying_books")
    sql_to_csv('4-teams_buying_books',
               params={'start_date': '2023-01-01', 'end_date': '2023-12-31'}
               )


def insert_into_db(
        lists_df: pd.DataFrame,
        books_df: pd.DataFrame,
        publications_df: pd.DataFrame
        ):

    create_tables()

    _logger.info("Inserting data into DB...")
    insert_into_table(df=books_df, table='stg_books', if_exists='replace')
    merge_tables(
        into_table='dim_books',
        from_table='stg_books',
        fields=BOOKS_COLUMNS,
        replace_index='book_sk'
        )

    insert_into_table(df=lists_df, table='stg_lists', if_exists='replace')
    merge_tables(
        into_table='dim_lists',
        from_table='stg_lists',
        fields=LISTS_COLUMNS,
        replace_index='list_id'
        )

    insert_into_table(df=publications_df, table='fact_publications',
                    if_exists='append')


def ingest_data():
    _logger.info(f"Fetching data from NYT API from {START_DATE} to {END_DATE}")

    # Fetch data day by day
    published_date = START_DATE
    while published_date is not None and published_date <= END_DATE:
        _logger.info(f"Searching by published date: {published_date}...")
        results = fetch_data('overview', params={'published_date': published_date})

        # Extract data required for each table

        # get publications data
        lists = results.pop('lists')
        _logger.info(f"Obtained {len(lists)} published")
        publications_df = pd.DataFrame.from_dict(
            pd.json_normalize(results),
            orient='columns'
            )

        lists_df = pd.DataFrame()
        books_df = pd.DataFrame()
        for list in lists:
            #get books data
            books = list.pop('books')

            # get lists data
            delta_lists_df = pd.DataFrame.from_dict(
                pd.json_normalize(list),
                orient='columns'
                )
            lists_df = pd.concat([lists_df, delta_lists_df], ignore_index=True)

            delta_books_df = pd.DataFrame.from_dict(
                    pd.json_normalize(books),
                    orient='columns'
                    )
            delta_books_df['list_id'] = list['list_id']
            books_df = pd.concat([books_df, delta_books_df], ignore_index=True)


        # Prepare data for loading

        # books
        # preparing columns
        books_df['isbn'] = books_df.primary_isbn13.combine_first(
            books_df.primary_isbn10)
        books_df['book_sk'] = books_df['isbn'].apply(hash)
        books_df['created_date'] = pd.to_datetime(books_df['created_date'])
        books_df['updated_date'] = pd.to_datetime(books_df['updated_date'])
        books_df['price'] = books_df['price'].astype(float)

        # publications
        # get date ids
        publications_df['published_date_id'] = publications_df['published_date']\
            .str.replace('-', '')
        publications_df['bestsellers_date_id'] = publications_df['bestsellers_date']\
            .str.replace('-', '')


        # to grain: publication by list and by book
        publications_df = publications_df.join(
            books_df,
            #on='list_id',
            how='cross',
            rsuffix='_bo'
            )

        # removing dups books
        books_df.sort_values('updated_date', ascending=False, inplace=True)
        books_df.drop_duplicates(subset='book_sk', inplace=True)

        # clean up columns
        publications_df = publications_df.reindex(columns=PUBLICATIONS_COLUMNS)
        books_df = books_df.reindex(columns=BOOKS_COLUMNS)
        lists_df = lists_df.reindex(columns=LISTS_COLUMNS)

        _logger.info(f"{len(lists_df)} lists and {len(books_df)} books were found")

        # send data to DB
        insert_into_db(
            lists_df=lists_df,
            books_df=books_df,
            publications_df=publications_df,
            )

        # obtain next_published_date 
        published_date = results['next_published_date']

        # the API gives me a rate of 5 requests per minute
        _logger.info("**")
        _logger.info("**")
        _logger.info("Waiting 13 seconds before next request to avoid exceeding API's rate limit ...")
        sleep(13)


def main():
    ingest_data()
    generate_reports()

    input("\nPress Enter to finish, or check the output files in the container before :)")


if __name__ == '__main__':
    main()
