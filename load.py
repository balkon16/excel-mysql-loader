import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

def import_excel_to_mysql(excel_file_path, mysql_password):
    """
    Imports data from an Excel file to a MySQL database, filtering out older events.

    Args:
        excel_file_path (str): Path to the Excel file.
        mysql_password (str): Password for the MySQL 'data_importer' user.
    """

    try:
        # 1. Read Excel file into a Pandas DataFrame
        df = pd.read_excel(excel_file_path)

        # Rename columns to be more database-friendly and consistent
        df = df.rename(columns={
            'Data': 'event_date',
            'Opis': 'description',
            'Objętość [ml]': 'volume'
        })

        columns_to_keep = ['event_date', 'description', 'volume']
        df = df[columns_to_keep]

        # Convert 'event_date' to datetime objects
        df['event_date'] = pd.to_datetime(df['event_date'])

        # 3. Connect to MySQL database
        mysql_user = 'data_importer'
        mysql_host = 'localhost'
        mysql_database = 'superset'

        # Using SQLAlchemy for easier database interaction
        engine = create_engine(f'mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}')

        # 4. Get MAX event_date from MySQL table
        try:
            max_date_query = "SELECT MAX(event_date) FROM consumption"
            max_date_df = pd.read_sql(max_date_query, engine)
            max_event_date = max_date_df.iloc[0, 0]  # Extract the value from the DataFrame
            max_event_date = pd.to_datetime(max_event_date)  # Convert to datetime64[ns]
            print("Max event_date detected: " + max_event_date.strftime("%Y-%m-%d"))
        except Exception as e:
            print(f"Error getting max date from MySQL: {e}")
            max_event_date = None  # Handle the case where the table is empty or doesn't exist

        # 5. Filter DataFrame
        if max_event_date:
            df_filtered = df[df['event_date'] > max_event_date]
        else:
            df_filtered = df  # If no max_event_date, import all data

        # 6. Load filtered data to MySQL
        df_filtered = df_filtered[['event_date', 'description', 'volume']] # Select only the columns that match the target table
        df_filtered['volume'] = df_filtered['volume'].astype(float) # Ensure volume is float for DECIMAL type
        df_filtered.to_sql('consumption', con=engine, if_exists='append', index=False)
        num_rows_after = len(df_filtered)  # Get the number of rows after loading

        print(f"Data imported successfully! {num_rows_after} rows loaded to MySQL.")

    except FileNotFoundError:
        print(f"Error: Excel file not found at {excel_file_path}")
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    excel_file = input("Enter the path to the Excel file: ")
    password = input("Enter the MySQL password for the 'data_importer' user: ")
    import_excel_to_mysql(excel_file, password)
