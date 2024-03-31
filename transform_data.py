from datetime import datetime

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit


def transform_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")

    transformed_data = {
        "City": data["name"],
        "Description": data["weather"][0]['description'],
        "Temperature (F)": kelvin_to_fahrenheit(data["main"]["temp"]),
        "Feels Like (F)": kelvin_to_fahrenheit(data["main"]["feels_like"]),
        "Minimun Temp (F)": kelvin_to_fahrenheit(data["main"]["temp_min"]),
        "Maximum Temp (F)": kelvin_to_fahrenheit(data["main"]["temp_max"]),
        "Pressure": data["main"]["pressure"],
        "Humidty": data["main"]["humidity"],
        "Wind Speed": data["wind"]["speed"],
        "Time of Record": datetime.utcfromtimestamp(data['dt'] + data['timezone']),
        "Sunrise (Local Time)":datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone']),
        "Sunset (Local Time)": datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])                        
        }
    
    return transformed_data
    