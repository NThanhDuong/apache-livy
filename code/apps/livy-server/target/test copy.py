from pyspark.sql import SparkSession
from livy.client import HttpClient

# Tạo Spark session
spark = SparkSession.builder.appName('Spark SQL Example').getOrCreate()

# Lấy thông tin từ 1 đến 5
data = list(range(1, 6))
df = spark.createDataFrame(data, "integer").toDF("number")

# Lưu DataFrame vào bảng tạm
df.createOrReplaceTempView("my_table")

# Tạo kết nối Livy
livy_url = 'http://localhost:8998'
client = HttpClient(livy_url)

# Truy vấn SQL bằng Livy
sql = "SELECT * FROM my_table"
response = client.post_session_statement(0, {"code": sql})
statement_id = response.json()['id']

# Lấy kết quả truy vấn
response = client.get_session_statement_output(0, statement_id)
result = response.json()['data']['text/plain']
print(result)
