import configparser
import psycopg2
from sql_objects import *
from create_resources import config_file
from delete_resources import delete_resources
import sys

# Creates Dictionary with all available questions
questions = {
              1: "1. Give me the top 10 most popular artists in the music app"
             ,2: "2. Give me the top 10 most popular songs in the music app"
             ,3: "3. Give me the total of songs played in the music app by hour"
             ,4: "4. Custom Query"
            }

def get_question(question_number):
    """Function to return the question from a dictionary

    Args:
        question_number (int): The question number you want to answer
    """
    print('\n' + questions.get(question_number,"Invalid question number!") + '\n')

def validation_queries():
    """"
    Script entry point, following these steps:
        1. Requests user to input a value according to the question number
        2. Valids user input
        3. Creates connection to Redshift Cluster
        4. Executes SQL Query according to input by user
        5. Prints the results
    The script will run until the user requests to exit, by pressing 0.
    All resources will be deleted once the user exits the program.
    Args:
        None
    Returns:
        None
    """

    # Prints the questions available
    print("""
    These are the available questions you can answer:

    1. Give me the top 10 most popular artists in the music app
    2. Give me the top 10 most popular songs in the music app
    3. Give me the total of songs played in the music app by hour '
    4. Custom Query (Check the Table Schema)

    """)

    # Creates an empty list to validate inputs by user
    question_list = list(questions.keys())

    # Calls for an infinite loop that keeps executing until user enter a valid question number
    while True:

            # Try to convert the input to a integer, If something else that is not a number is introduced the ValueError exception will be called.
            try:
                question_number = int(input("Please enter the QUESTION NUMBER you want to answer or [0] to exit: "))
                if question_number == 0:
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
                elif question_number not in question_list:
                    print("Error! This is not a valid question number. These are valids QUESTION NUMBER {} ".format(question_list))
                else:

                    # Connecting to Redshift Cluster
                    cur, conn = create_connection()

                    #Get Query By Question Number
                    query = get_query(question_number)

                    #Print Question
                    get_question(question_number)

                    #Execute Query
                    execute_query(cur, conn, query)

            # This is the exception called the attempt to convert the input to integer
            except ValueError:
                # The cycle will go on until validation
                print("Error! This is not a number. Try again.")

if __name__ == '__main__':
    validation_queries()