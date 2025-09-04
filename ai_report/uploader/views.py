import os
import pandas as pd
from django.shortcuts import render
from django.conf import settings
from .forms import UploadForm
from .utils import sanitize_table_name, connect_client, ensure_table, insert_dataframe

def read_to_dataframe(filepath: str) -> pd.DataFrame:
    name = filepath.lower()
    if name.endswith('.csv'):
        return pd.read_csv(filepath)
    if name.endswith('.xlsx') or name.endswith('.xls'):
        return pd.read_excel(filepath)
    # fallback: พยายามอ่านเป็น CSV
    return pd.read_csv(filepath)

def upload_view(request):
    ctx = {'inserted': None, 'table': None, 'error': None}
    if request.method == 'POST':
        form = UploadForm(request.POST, request.FILES)
        if form.is_valid():
            f = form.cleaned_data['file']
            table_name = form.cleaned_data.get('table_name') or os.path.splitext(f.name)[0]
            table = sanitize_table_name(table_name)

            # save ไฟล์ชั่วคราวลง MEDIA
            save_path = os.path.join(settings.MEDIA_ROOT, f.name)
            os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
            with open(save_path, 'wb+') as dest:
                for chunk in f.chunks():
                    dest.write(chunk)
            try:
                df = read_to_dataframe(save_path)

                # จัดการข้อมูลก่อน insert
                df = df.fillna('')  # เติมค่าเริ่มต้นสำหรับ NaN
                for col in df.select_dtypes(include=['datetime64[ns]', 'datetime']):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                for col in df.select_dtypes(include=['float', 'int']):
                    df[col] = df[col].fillna(0)

                client = connect_client()
                ensure_table(client, table, df)
                inserted = insert_dataframe(client, table, df)
                ctx.update({'inserted': inserted, 'table': table})
            except Exception as e:
                ctx['error'] = str(e)
            finally:
                try: os.remove(save_path)
                except Exception: pass
        else:
            ctx['error'] = 'Invalid form'
    else:
        form = UploadForm()

    ctx['form'] = form
    return render(request, 'uploader/upload.html', ctx)
