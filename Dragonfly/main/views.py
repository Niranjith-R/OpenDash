from django.shortcuts import render, redirect
from rest_framework import permissions, viewsets
from .serializers import VideoSerializer
from .models import Video
from django.contrib.auth.models import User
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.decorators import login_required
from confluent_kafka import Producer
from django.core.paginator import Paginator
from os import getenv


kafka_host = getenv("kafka_server")

conf = {
     'bootstrap.servers': kafka_host,
     'client.id' : 'Dragonfly_Producer'
}

P = Producer(conf)
topic = "Dragonfly"

# Create your views here.

def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message : {msg} : {err}")





class VideoViewSet(viewsets.ModelViewSet):
    permission_classes = [permissions.AllowAny,]
    queryset = Video.objects.all()
    serializer_class = VideoSerializer


def home (request):
        return render(request, 'main/index.html')

        

def live_feed(request):
        return render(request, 'main/live.html')

@login_required
def recordings(request):

    p = Paginator(Video.objects.filter(is_deleted=False).order_by('-created_date'), 8)
    page_number = request.GET.get('page')
    page_obj = p.get_page(page_number)

    return render(request, 'main/recordings.html', context={"page_objs" : page_obj,'nginx' : getenv('nginx_server'), 'nginx_port': getenv('nginx_port')})


@login_required
def settings(request):
    if request.POST.get("action") == "send":
         P.produce(topic, value = request.POST.get('prod_msg'), on_delivery = acked)
         P.flush(1.0)
    return render(request, 'main/settings.html')

def login_user(request):

    if request.method == 'POST':
        if request.POST.get("action") == 'signup':
            username = request.POST.get("username")
            password = request.POST.get("password")
            user = User.objects.create_user(username=username, password=password)
            user.save()
            auth_user = authenticate(username=username, password=password)
            if auth_user is not None:
                login(request, auth_user)
                return redirect(recordings)
        if request.POST.get("action") == 'login':
            username = request.POST.get("username")
            password = request.POST.get("password")
            auth_user = authenticate(username=username, password=password)
            if auth_user is not None:
                login(request, auth_user)
                return redirect(recordings)
    
    if User.objects.count()>=1:
        return render(request, 'main/login.html')
    else:
         return render(request, 'main/register.html')
    

# def add(request):
#      return render(request, 'main/add.html')