from django.urls import  path

from .views import upload_view

urlpatterns = [
    path("", upload_view, name="upload"),

]
