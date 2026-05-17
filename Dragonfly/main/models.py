from django.db import models

# Create your models here.

class Video(models.Model):
    video_name = models.CharField(max_length=50, verbose_name="Video file name")
    path = models.CharField(max_length=500, verbose_name="Video File Path")
    camera_name = models.CharField(max_length=255, verbose_name="Camera Name")
    created_date = models.DateTimeField(verbose_name="Date")
    is_deleted = models.BooleanField(default=False)

    # def __str__(self):
    #     return [ self.video_name, self.path, self.camera_name, self.created_date, self.is_deleted]
