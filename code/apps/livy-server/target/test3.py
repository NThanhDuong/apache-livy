import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Thông tin kết nối PostgreSQL
postgres_host = "localhost"
postgres_port = 5432
postgres_database = "db"
postgres_table = "test1"
postgres_username = "postgres"
postgres_password = "12345"

# Đường dẫn kết nối PostgreSQL
postgres_url = f"postgresql://{postgres_username}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}"

# Đường dẫn tới tệp CSV
csv_file_path = "/home/duongngt/docker-livy/code/apps/livy-server/target/test.csv"

# Đọc tệp CSV bằng pandas
df = pd.read_csv(csv_file_path)

# Tạo kết nối đến cơ sở dữ liệu PostgreSQL
conn = psycopg2.connect(host=postgres_host, port=postgres_port, database=postgres_database, user=postgres_username, password=postgres_password)
cursor = conn.cursor()

# Tạo schema (nếu chưa tồn tại)
schema_name = "public"
create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
cursor.execute(create_schema_query)
conn.commit()

# Tạo bảng trong schema đã chỉ định
table_name = f"{schema_name}.{postgres_table}"
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (name VARCHAR(255), age INT)"
cursor.execute(create_table_query)
conn.commit()

# Ghi dữ liệu vào PostgreSQL
engine = create_engine(postgres_url)
df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

# Đóng kết nối
cursor.close()
conn.close()
