from django import forms

class UploadForm(forms.Form):
    file = forms.FileField(label="CSV/Excel")
    table_name = forms.CharField(required=False, help_text="ว่างได้: จะใช้ชื่อไฟล์แทน")