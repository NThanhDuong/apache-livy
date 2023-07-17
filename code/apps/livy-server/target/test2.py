import requests
import json
import time

livy_url = "http://localhost:8998"
headers = {'Content-Type': 'application/json'}

data = {'kind': 'pyspark'}
response = requests.post(f"{livy_url}/sessions", json=data, headers=headers)
session_id = response.json()['id']

time.sleep(7)
url = "/opt/bitnami/test.csv"
data = {'code': f"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Tạo một SparkSession
spark = SparkSession.builder.appName("PersonalInfo").getOrCreate()

# Định nghĩa schema cho DataFrame
schema = StructType([
    StructField("Name", StringType(), nullable=False),
    StructField("Age", IntegerType(), nullable=False),
    StructField("Address", StringType(), nullable=True),
    StructField("Email", StringType(), nullable=True)
])

# Tạo dữ liệu cho DataFrame
data = [
    ("John Doe", 25, "123 Main St", "john@example.com"),
    ("Jane Smith", 30, "456 Elm St", "jane@example.com"),
    ("Bob Johnson", 35, "789 Oak St", "bob@example.com")
]

# Tạo DataFrame từ dữ liệu và schema
df = spark.createDataFrame(data, schema)

# In ra DataFrame
df.show()
"""}
response = requests.post(f"{livy_url}/sessions/0/statements", json=data, headers=headers)
#statement_id = response.json()['id']
time.sleep(7)
response = requests.get(f"{livy_url}/sessions/0/statements/0", headers=headers)
result = response.json()
print(result)