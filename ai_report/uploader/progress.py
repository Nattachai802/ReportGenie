import logging
from datetime import datetime

# ตั้งค่า logging สำหรับติดตามการอัพโหลด
logger = logging.getLogger(__name__)

def log_upload_progress(message: str):
    """Log progress message for upload operations"""
    logger.info(f"Upload Progress: {message}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
