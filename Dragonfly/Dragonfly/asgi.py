# project_name/asgi.py
import os
import django
from django.core.asgi import get_asgi_application

# Set environment before importing anything else from your app
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Dragonfly.settings')
django.setup()

# Import routing AFTER django.setup()
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
from main.routing import websocket_urlpatterns

application = ProtocolTypeRouter({
    "http": get_asgi_application(),
    "websocket": AuthMiddlewareStack(
        URLRouter(
            websocket_urlpatterns
        )
    ),
})