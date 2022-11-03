from src.database.db import create_tables
from src.pipelines.load import insert_data_to_db
from src.pipelines.extract import extract_to_new_table


def etl():
    # Create tables in database
    create_tables()
    print('All tables were created successfully')
    print('---------------')

    # Insert sample data into database tables
    insert_data_to_db(30)
    print('Sample data has been inserted into the database')
    print('---------------')

    # Extract data from database tables and create 'movimento_flat' table
    extract_to_new_table('data/')
    print('csv file was created in the specified path')
    print('---------------')


if __name__ == '__main__':
    etl()
