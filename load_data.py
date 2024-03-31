from datetime import datetime
import pandas as pd

def load_data(task_instance):
    transformed_data = task_instance.xcom_pull(task_ids="transform_weather_data")
    
    aws_credentials = {"key": "", "secret": "", "token": ""}
    s3_bucket = ""
    
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_romaiville_' + dt_string
    df_data.to_csv(f"s3://{bucket}/{dt_string}.csv", index=False, storage_options=aws_credentials)
