from django.db import models
from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField #,JSONField
from django.urls import reverse
from datasets.models import Dataset
from places.models import Place

def coll_image_path(instance, filename):
  # upload to MEDIA_ROOT/collections/<id>_<filename>
  return 'collections/{0}_{1}'.format(instance.id, filename)

class Collection(models.Model):
  owner = models.ForeignKey(User,related_name='collections', on_delete=models.CASCADE)
  title = models.CharField(null=False, max_length=255)
  description = models.CharField( null=False, max_length=2044)
  tags = ArrayField(models.CharField(max_length=50))
  image_file = models.FileField(upload_to=coll_image_path, blank=True, null=True)
  
  create_date = models.DateTimeField(null=True, auto_now_add=True)
  public = models.BooleanField(default=False)
  featured = models.IntegerField(null=True, blank=True)
  
  datasets = models.ManyToManyField("datasets.Dataset")

  def get_absolute_url(self):
    #return reverse('datasets:dashboard', kwargs={'id': self.id})
    return reverse('datasets:dashboard')  

  @property
  def places(self):
    # TODO: gang up indiv & ds places
    dses = self.dataset_set.all()
    return Place.objects.filter(dataset__in=dses).distinct()

  def __str__(self):
    return '%s:%s' % (self.id, self.title)

  class Meta:
    db_table = 'collections'

#class CollectionPlace(models.Model):
  #collection = models.ForeignKey(Collection, related_name='coll_places',
                                   #default=-1, on_delete=models.CASCADE)
  #place = models.ForeignKey(Place, related_name='places',
                              #default=-1, on_delete=models.CASCADE)

  #def __str__(self):
    #return self.collection + '<>' + self.place

  #class Meta:
    #managed = True
    #db_table = 'collection_place'

#class CollectionDataset(models.Model):
  #collection = models.ForeignKey(Collection, related_name='collection_datasets',default=-1, on_delete=models.CASCADE)
  #dataset = models.ForeignKey(Dataset, related_name='dataset_collections',default=-1, on_delete=models.CASCADE)

  #def __str__(self):
    #return '%s:%s' % (self.collection, self.dataset)

  #class Meta:
    #managed = True
    #db_table = 'collection_dataset'





