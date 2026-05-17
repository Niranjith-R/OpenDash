from .models import Video
from rest_framework import serializers

 
class VideoSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Video
        fields = [
            'id', 'video_name', 'path', 'created_date','camera_name', 'is_deleted',
        ]