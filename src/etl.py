from create_resources import config_file, create_resources
from sql_objects import create_connection, drop_tables, create_tables, load_staging_tables, insert_tables
from delete_resources import delete_resources
from validation import validation_queries
import configparser
import sys

def etl():

    print('Creating Resources...')
    create_resources()
    print('AWS Resources have been created.')

    # Connecting to Redshift Cluster
    print('Initiate ETL...')
    print('Connecting to Redshift Cluster...')
    cur, conn = create_connection()

    print('Dropping Tables...')
    drop_tables(cur, conn)

    print('Creating Tables...')
    create_tables(cur, conn)

    print('Loading Staging Tables...')
    load_staging_tables(cur, conn)

    print('Loading Fact & Dimension Tables...')
    insert_tables(cur, conn)

    print('Closing Cluster Connection...')
    conn.close()

    # Creates an empty list to validate inputs by user
    answer_list = ['Y','N']

    # Calls for an infinite loop that keeps executing until user enter a valid question number
    while True:
        try:
            answer = str(input("Would you like to run some validation queries? Please enter [y] or [n]: ")).upper()
            if answer == 'N':
                answer = str(input("This will delete all AWS Resources. Do you want to proceed? Please enter [y] or [n]: ")).upper()
                if answer == 'Y':

                    # Delete Resources before exit program.
                    print('Deleting Resources...')
                    delete_resources()

                    # Exit Program
                    print('Exiting Script... Goodbye! \n')
                    sys.exit()

                else:
                    continue
            elif answer not in answer_list:
                 print("Error! This is not a answer. These are valids ANSWERS {} ".format(answer_list))
            else:
                validation_queries()

        # This is the exception called the attempt to convert the input to integer
        except ValueError:
        # The cycle will go on until validation
            print("Error! This is not a letter. Try again.")

if __name__ == "__main__":
    etl()