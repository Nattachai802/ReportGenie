import pandas as pd
from clickhouse_driver import Client
from datetime import datetime

def csv_to_clickhouse():
    # 1. อ่าน CSV เป็น DataFrame
    print("กำลังอ่าน CSV file...")
    df = pd.read_csv('multilingual_mobile_app_reviews_2025.csv')
    print(f"อ่านข้อมูลได้ {len(df)} แถว")
    
    # 2. เชื่อมต่อ ClickHouse
    print("กำลังเชื่อมต่อ ClickHouse...")
    client = Client(host='localhost')  # เปลี่ยน host ตามต้องการ
    
    # 3. สร้างตาราง (ถ้ายังไม่มี)
    print("กำลังสร้างตาราง...")
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test (
        review_id UInt32,
        user_id UInt64,
        app_name String,
        app_category String,
        review_text String,
        review_language String,
        rating Float32,
        review_date DateTime,
        verified_purchase UInt8,
        device_type String,
        num_helpful_votes UInt32,
        user_age Float32,
        user_country String,
        user_gender String,
        app_version String
    ) ENGINE = MergeTree()
    ORDER BY review_id
    """
    
    client.execute(create_table_sql)
    print("สร้างตารางเรียบร้อย")
    
    # 4. แปลงข้อมูลให้เหมาะสมกับ ClickHouse
    print("กำลังเตรียมข้อมูล...")
    
    # แปลง boolean เป็น 0/1
    df['verified_purchase'] = df['verified_purchase'].astype(int)
    
    # แปลง review_date เป็น datetime
    df['review_date'] = pd.to_datetime(df['review_date'])
    
    # จัดการค่า null และ NaN ในทุกคอลัมน์
    df = df.fillna({
        'user_age': 0.0,
        'app_name': '',
        'app_category': '',
        'review_text': '',
        'review_language': '',
        'device_type': '',
        'user_country': '',
        'user_gender': '',
        'app_version': '',
        'rating': 0.0
    })
    
    # แปลงข้อมูลให้เป็นประเภทที่ถูกต้อง
    df['user_age'] = df['user_age'].astype(float)
    df['rating'] = df['rating'].astype(float)
    df['review_id'] = df['review_id'].astype(int)
    df['user_id'] = df['user_id'].astype(int)
    df['num_helpful_votes'] = df['num_helpful_votes'].astype(int)
    
    # 5. แปลง DataFrame เป็น list of tuples และแปลง NaN เป็นค่าที่เหมาะสม
    data_to_insert = []
    for _, row in df.iterrows():
        row_data = []
        for value in row:
            if pd.isna(value) or value is None:
                if isinstance(value, (int, float)):
                    row_data.append(0)
                else:
                    row_data.append('')
            else:
                row_data.append(value)
        data_to_insert.append(tuple(row_data))
    
    # 6. Insert ข้อมูลเข้า ClickHouse
    print("กำลัง insert ข้อมูล...")
    insert_sql = """
    INSERT INTO app_reviews (
        review_id, user_id, app_name, app_category, review_text, 
        review_language, rating, review_date, verified_purchase, 
        device_type, num_helpful_votes, user_age, user_country, 
        user_gender, app_version
    ) VALUES
    """
    
    client.execute(insert_sql, data_to_insert)
    print(f"Insert ข้อมูลเรียบร้อย {len(data_to_insert)} แถว")
    
    # 7. ตรวจสอบข้อมูลที่ insert
    result = client.execute("SELECT COUNT(*) FROM app_reviews")
    print(f"จำนวนข้อมูลในตาราง: {result[0][0]} แถว")
    
    # แสดงตัวอย่างข้อมูล 5 แถวแรก
    sample_data = client.execute("SELECT * FROM app_reviews LIMIT 5")
    print("\nตัวอย่างข้อมูล 5 แถวแรก:")
    for row in sample_data:
        print(row)

if __name__ == "__main__":
    try:
        csv_to_clickhouse()
        print("\nเสร็จสิ้น! ข้อมูลถูก import เข้า ClickHouse เรียบร้อยแล้ว")
    except Exception as e:
        print(f"เกิดข้อผิดพลาด: {e}")
