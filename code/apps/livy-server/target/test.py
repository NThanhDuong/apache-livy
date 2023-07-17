import requests

livy_url = "http://localhost:8998"
headers = {'Content-Type': 'application/json'}

# Đoạn code Spark
code = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Postgres Example') \
    .config('spark.jars', '/opt/bitnami/livy/postgresql-42.3.7.jar') \
    .getOrCreate()

url = 'jdbc:postgresql://localhost:5432/db'
properties = {
    'user': 'postgres',
    'password': '12345',
    'driver': 'org.postgresql.Driver'
}

df = spark.read.jdbc(url=url, table='public.test1', properties=properties)
print(df.collect())
df.show()
"""

# Tạo một session mới
data = {'kind': 'pyspark'}
response = requests.post(f"{livy_url}/sessions", json=data, headers=headers)
session_id = response.json()['id']

# Gửi mã code để thực thi
data = {'code': code}
response = requests.post(f"{livy_url}/sessions/1/statements", json=data, headers=headers)
#statement_id = response.json()['id']

# Lấy kết quả
response = requests.get(f"{livy_url}/sessions/1/statements/0", headers=headers)
result = response.json()
print(result)
