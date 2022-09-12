from airflow.decorators import dag, task
from datetime import datetime

default_args = {
    'start_date': datetime(2022, 9, 1)
}

FILE_PATH = "~/airflow/dags/"

@dag('process_dataset', schedule_interval='5 1 * * *', default_args=default_args, catchup=False)
def taskflow():

    import pandas as pd
    
    @task
    def extract_raw_data():
        """
        Read datasets from input folder
        """
        df1 = pd.read_csv(FILE_PATH+"input/dataset1.csv")
        df2 = pd.read_csv(FILE_PATH+"input/dataset2.csv")
        df_merged = pd.concat([df1,df2], ignore_index=True)
        raw_json = df_merged.to_json()
        
        return raw_json

    @task
    def transform_raw_data(raw_json):
        """
        Transform the data based on the rules provided
        """
        transformed_json = transform_df(pd.read_json(raw_json)).to_json()
        return transformed_json
        
    @task
    def load_transformed_data(transformed_json):
        """
        Write processed json to output folder as a csv
        """        
        pd.read_json(transformed_json).to_csv(FILE_PATH+"output/processed_dataset.csv")
        return

    load_transformed_data(transform_raw_data(extract_raw_data()))

dag = taskflow()

def transform_df(df):
    stopwords = ['Mr.', 'Mrs.']

    # name split
    name_split_list = df['name'].str.split(" ")
    name_split_list = name_split_list.apply(lambda x: [i for i in x if i not in stopwords])
    df['first_name'] = name_split_list.str[0]
    df['last_name'] = name_split_list.str[1:].apply(lambda x: " ".join(x))

    # drop blank name columns
    df=df.dropna(subset=['name'])

    # remove 0 
    df['price'] = df['price'].apply(lambda x: float(str(x).rstrip("0")))
                                                                
    # new column
    df['above_100'] = df['price'].apply(lambda x: True if x > 100 else False)  