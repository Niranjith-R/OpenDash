from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from rest_framework import routers
from . import views


router = routers.DefaultRouter()
router.register(r'video', views.VideoViewSet)

urlpatterns = [
    path('', views.recordings, name="recordings"),
    path('login', views.login_user, name="login"), 
    path('settings/', views.settings, name="settings"),
    # path('add/', views.add, name="add"),
    path('api/', include(router.urls))
    ]

urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)