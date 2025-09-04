import re
import pandas as pd
from clickhouse_driver import Client

def sanitize_table_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r'[^a-z0-9_]+', '_', name)
    return re.sub(r'_+', '_', name).strip('_') or 'uploaded_table'

def connect_client():
    # ใส่ค่าจริงของคุณ—หรือใช้ .env ก็ได้
    return Client(
        host='127.0.0.1',
        port=9000,
        user='default',
        password='',
        database='default',
        settings={}  # ลบ 'use_numpy': True
    )

def pandas_dtype_to_ch(dtype):
    # dtype เป็น string เช่น 'int64','float64','object','datetime64[ns]' เป็นต้น
    s = str(dtype)
    if 'int' in s: return 'Nullable(Int64)'
    if 'float' in s: return 'Nullable(Float64)'
    if 'bool' in s: return 'Nullable(UInt8)'
    if 'datetime' in s or 'date' in s: return 'Nullable(DateTime)'
    # object, string อื่นๆ
    return 'Nullable(String)'

def ensure_table(client: Client, table: str, df: pd.DataFrame):
    cols = []
    for col, dtype in df.dtypes.items():
        ch_type = pandas_dtype_to_ch(dtype)
        col_safe = sanitize_table_name(col)
        cols.append(f'`{col_safe}` {ch_type}')
    cols_sql = ', '.join(cols) if cols else '`_dummy` Nullable(String)'
    sql = f'CREATE TABLE IF NOT EXISTS `{table}` ({cols_sql}) ENGINE = MergeTree ORDER BY tuple()'
    client.execute(sql)

def insert_dataframe(client: Client, table: str, df: pd.DataFrame):
    if df.empty:
        return 0

    # แปลง NaN → None (ให้เข้ากับ Nullable)
    df = df.where(pd.notnull(df), None)

    # ตรวจสอบและแปลงประเภทข้อมูลให้ตรงกับ ClickHouse
    for col in df.columns:
        if df[col].dtype in ['float64', 'float32']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float)
        elif df[col].dtype in ['int64', 'int32']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        elif df[col].dtype == 'bool':
            df[col] = df[col].astype(int)  # แปลง True/False → 1/0
        elif df[col].dtype == 'object' or df[col].dtype.name == 'category':
            df[col] = df[col].astype(str)  # แปลงเป็น string
        elif 'datetime' in str(df[col].dtype):
            df[col] = pd.to_datetime(df[col], errors='coerce')  # แปลงเป็น datetime
        else:
            df[col] = df[col].astype(str)  # fallback เป็น string

    # แปลง DataFrame เป็น list of tuples
    data_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]

    # สร้าง SQL สำหรับ INSERT
    cols = [sanitize_table_name(c) for c in df.columns.tolist()]
    placeholders = ', '.join([f'`{c}`' for c in cols])
    insert_sql = f'INSERT INTO `{table}` ({placeholders}) VALUES'

    # Insert ข้อมูลเข้า ClickHouse
    client.execute(insert_sql, data_to_insert)
    return len(data_to_insert)
