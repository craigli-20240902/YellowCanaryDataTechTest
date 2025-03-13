from datetime import datetime
date_str = "2023-01-26T00:00:00"
date_time = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
print(date_time)
print(date_time.strftime("%Y-%m-%d"))
print(datetime.strptime(date_time.strftime("%Y-%m-%d"), "%Y-%m-%d"))
