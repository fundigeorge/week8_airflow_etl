from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from airflow.decorators import dag, task

#file path
customer_data_path = '/home/fundi/moringaschool/week8/airflow/airflow/customer_data.csv'
order_data_path = '/home/fundi/moringaschool/week8/airflow/airflow/order_data.csv'
payment_data_path = '/home/fundi/moringaschool/week8/airflow/airflow/payment_data.csv'

#postgres db connection
host = 'localhost'
port = 5433
db = 'customers'
user = 'postgres'
password =  'fundi'

#default arg for dag task
default_args = {
    'owner': 'XYZ Telecoms',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#use the dag decorator to create dag context
@dag('mtn_rwanda_customers', default_args=default_args, schedule_interval='@daily')
def taskflow_api():
    #use task decorator to create dag task
    """
    ### Taskflow API for extracting, transforming and loading customer data

    """
    @task()    
    def extract_data(data_file):
        """
        ### extract data task
        extract data from CSV files
        load the CSV data into Pandas dataframes for later transformation
        """
        data =  pd.read_csv(data_file).to_dict(orient='records')

        #return serialized data
        return data
    
    @task()
    def transform_data(data_customer:dict, data_order:dict, data_payment:dict): 
        """
        ### Transform task
        merge the order and payment details to the customer data
        drop unnecessary columns
        """ 
        #convert the dict to pandas dataframe
        customer_data = pd.DataFrame(data_customer)
        order_data = pd.DataFrame(data_order)
        payment_data = pd.DataFrame(data_payment)
        
        # merge customer and order dataframes on the customer_id column
        customer = customer_data.merge(order_data, how='left', on =['customer_id'])

        # merge payment dataframe with the merged dataframe on the order_id and customer_id columns
        customer = customer.merge(payment_data, how='left',  on =['customer_id', 'order_id'])

        # drop unnecessary columns like customer_id and order_id
        customer = customer.drop(columns=['customer_id', 'order_id', 'payment_id','order_date'])
      
        # group the data by customer and aggregate the amount paid using sum
        customer = customer.groupby(by=['first_name', 'last_name', 'email', 'country', 'gender', 'product',
                                        'date_of_birth', 'payment_date']).agg({'amount':'sum'}).reset_index()
        
        #the total value and customer lifetime is ambigous, grouping by customer sums up customer order
        # create a new column to calculate the total value of orders made by each customer
        # calculate the customer lifetime value using the formula CLV = (average order value) x (number of orders made per year) x (average customer lifespan) 
        #customer['data_of_birth'] = customer['data_of_birth'].to_string()
        #customer['payment_date'] = customer['payment_date'].to_string()
        
        #return serialized data
        return customer.to_dict(orient='records')
    
    @task()
    def load_data(data_customer:dict):
        """
        ### load task
        load the transformed data into Postgres database
        """
        
        #convert dict to pandas dataframe
        customer_data = pd.DataFrame(data_customer)

        # load the transformed data into Postgres database
        conn = psycopg2.connect(database=db, user=user, password=password, host=host, port=port)
        cur = conn.cursor()
        cur.execute(
            'drop table if exists customers_data;'
            """create table if not exists customers_data 
            (first_name text, last_name text, email text, country text, gender text, product text,
            date_of_birth text, payment_date text, amount integer);"""
            )
        #for i, row in customer_data.iterrows(): 
        for row in customer_data.itertuples(index=False, name=None):
            #row = tuple(row)
            print(row)
            cur.execute("""
                insert into customers_data 
                (first_name, last_name, email, country, gender, product, date_of_birth, payment_date, amount)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s) """, row )  
             
        #commit the data and close db connection        
        conn.commit()
        cur.close()
        conn.close()
    
    #extract data
    customer_data= extract_data(customer_data_path)
    order_data = extract_data(order_data_path)
    payment_data = extract_data(payment_data_path)    
    
    #transform the data
    customer_data = transform_data(customer_data, order_data, payment_data)

    #load the data
    load_data(customer_data)
    print(customer_data)

taskflow_api()

print("airflow task created for mtn rwanda customer data")


    