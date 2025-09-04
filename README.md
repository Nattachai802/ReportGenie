# ReportGenie  
AI Assistant for Report Creation in Datawarehouse Project  

---

## 📌 Overview
**ReportGenie** เป็นระบบช่วยเหลือในการสร้างรายงาน (AI Assistant) สำหรับโปรเจค Data Warehouse โดยรองรับการอัปโหลดไฟล์จากผู้ใช้งาน และบันทึกข้อมูลลงใน **ClickHouse** เพื่อใช้งานต่อยอดด้านการวิเคราะห์ข้อมูลและการสร้างรายงานอัตโนมัติ  

---

## 🚀 Features  (ในปัจจุบัน)
- อัปโหลดไฟล์จากผู้ใช้งาน (CSV, TSV, XLSX, Parquet, JSON)  
- แปลงไฟล์ที่อัปโหลดเป็น **Pandas DataFrame**  
- สร้างตารางใหม่ใน ClickHouse อัตโนมัติตามโครงสร้างของข้อมูล  
- Insert ข้อมูลลง ClickHouse รองรับทั้ง **row-based** และ **batch insert** สำหรับไฟล์ขนาดใหญ่  

---

## 🛠 Requirements
- **Python** 3.11  
- **ClickHouse Server** (ใช้ native driver port 9000)  
- Libraries:
  - `django`  
  - `pandas`  
  - `numpy`  
  - `clickhouse-driver`  
  - `openpyxl` (สำหรับไฟล์ Excel)  
  - `pyarrow` (สำหรับ Parquet)  
  - `chardet` (detect encoding CSV/TSV)  

---

## ⚙️ Installation
1. ทำการเปิด server clickhouse (./clickhouse server)
2. git clone https://github.com/Nattachai802/ReportGenie/
3. pip install -r requirements.txt
