from django.conf import settings
from django.shortcuts import get_object_or_404
import re
import pandas as pd
from places.models import *
from datasets.models import Dataset
from elasticsearch7 import Elasticsearch
from elastic.es_utils import makeDoc
from sentry_sdk import capture_exception

es = Elasticsearch([{'host': 'localhost',
                     'port': 9200,
                     'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
                     'timeout': 30,
                     'max_retries': 10,
                     'retry_on_timeout': True
                     }])
idx = 'whg'

dsid = 586  # 'diamonds' current file: user_whgadmin/diamonds135_rev3_g6cvm1l.tsv
# start: diamonds135_rev3; proposed update: diamonds135_rev21-125
ds = get_object_or_404(Dataset, pk=dsid)
[keepg, keepl] = [True, True]
oldids = list(Place.objects.filter(dataset=ds.label).values_list('src_id', flat=True))
curfile = 'user_whgadmin/diamonds135_hfl3svn.tsv'
# post tgn recon: 135 places, 135 names, 10 links, 25 geoms
newfile = 'user_whgadmin/diamonds135_rev2a-125.tsv'
tempfile = '/var/folders/f4/x09rdl7n3lg7r7gwt1n3wjsr0000gn/T/tmpwb8q_u5i.tsv'
# adf = pd.read_csv('media/'+curfile, delimiter='\t',dtype={'id':'str','ccodes':'str'})
# bdf = pd.read_csv(newfile, delimiter='\t',dtype={'id':'str','ccodes':'str'})

adf = pd.read_csv('media/' + curfile, delimiter='\t', dtype={'id': 'str', 'ccodes': 'str'})
bdf = pd.read_csv(tempfile, delimiter='\t', dtype={'id': 'str', 'ccodes': 'str'})
bdf = bdf.astype({"ccodes": str})

ids_a = adf['id'].tolist()
ids_b = bdf['id'].tolist()
# 
delete_srcids = [str(x) for x in (set(ids_a) - set(ids_b))]
replace_srcids = set.intersection(set(ids_b), set(ids_a))
places = Place.objects.filter(dataset=ds.label)
rows_delete = list(places.filter(src_id__in=delete_srcids).values_list('id', flat=True));
rows_replace = list(places.filter(src_id__in=replace_srcids).values_list('id', flat=True));
rows_add = [str(x) for x in (set(ids_b) - set(ids_a))];

# DATABASE actions
# delete places with ids missing in new data (CASCADE includes links & geoms)
places.filter(id__in=rows_delete).delete()

ncount = PlaceName.objects.filter(place_id__in=places).count()  # 134
gcount = PlaceGeom.objects.filter(place_id__in=places).count()  # 17
lcount = PlaceLink.objects.filter(place_id__in=places).count()  # 3

# delete related instances for the rest (except links and geoms)
PlaceName.objects.filter(place_id__in=places).delete()
PlaceType.objects.filter(place_id__in=places).delete()
PlaceWhen.objects.filter(place_id__in=places).delete()
PlaceDescription.objects.filter(place_id__in=places).delete()
PlaceDepiction.objects.filter(place_id__in=places).delete()
#
ncount = PlaceName.objects.filter(place_id__in=places).count()  # 134
gcount = PlaceGeom.objects.filter(place_id__in=places).count()  # 17
lcount = PlaceLink.objects.filter(place_id__in=places).count()  # 3

# keep links and/or geoms is a form choice (keepg, keepl)
# rows created during reconciliation review have a task_id
if keepg == 'false':
    # keep none (they are being replaced in update)
    PlaceGeom.objects.filter(place_id__in=places).delete()
else:
    # keep augmentation rows; delete the rest
    PlaceGeom.objects.filter(place_id__in=places, task_id=None).delete()
if keepl == 'false':
    # keep none (they are being replaced in update)
    PlaceLink.objects.filter(place_id__in=places).delete()
else:
    PlaceLink.objects.filter(place_id__in=places, task_id=None).delete()

ncount = PlaceName.objects.filter(place_id__in=places).count()  # 134
gcount = PlaceGeom.objects.filter(place_id__in=places).count()  # 17
lcount = PlaceLink.objects.filter(place_id__in=places).count()  # 3

# Place instances to be kept remain, related are gone
ds.places.count()  # 123

# now update values in places; recreate place_xxxxx rows
# from ds_update line 676-714
count_updated, count_new = [0, 0]
# update remaining place instances w/data from new file
# AND add new
from datasets.views import add_rels_tsv

place_fields = {'id', 'title', 'ccodes'}
for index, row in bdf.iterrows():
    rd = row.to_dict()
    rdp = {key: rd[key] for key in place_fields}
    # look for corresponding current place
    p = places.filter(src_id=rdp['id']).first()
    if p != None:
        # place exists, update it
        count_updated += 1
        p.title = rdp['title']
        p.ccodes = [] if str(rdp['ccodes']) == 'nan' else rdp['ccodes'].replace(' ', '').split(';')
        p.save()
        pobj = p
    else:
        # if not, entirely new place
        count_new += 1
        newpl = Place.objects.create(
            src_id=rdp['id'],
            title=re.sub('\(.*?\)', '', rdp['title']),
            ccodes=rdp['ccodes'].replace(' ', '').split(';'),
            dataset=ds
        )
        newpl.save()
        pobj = newpl

    # create related records (place_name, etc)
    # pobj is either a current (now updated) place or entirely new
    # rd is row dict
    add_rels_tsv(pobj, rd)

pcount = ds.places.count()
ncount = PlaceName.objects.filter(place_id__in=places).count()  # 134
gcount = PlaceGeom.objects.filter(place_id__in=places).count()  # 17
lcount = PlaceLink.objects.filter(place_id__in=places).count()  # 3

# 2 new rows, all w/names, 15+9 geoms, 9 links CHECK

# END DATABASE actions

# ES STUFF
# fetch place_ids for all docs in a dataset


pids = fetch_pids('diamonds')


# strip indexed places by Place.id; pids = place_id array
def deleteFromIndex(pids):
    if len(pids) > 0:
        delthese = []
        # child: 6293916; parent with children: 13549548; parent w/no children 6293837
        for pid in pids:
            # get its index document
            res = es.search(index=idx, body=esq_pid(pid))
            doc = res['hits']['hits'][0]
            src = doc['_source']
            role = src['relation']['name'];
            sugs = list(set(src['suggest']['input']))  # distinct only
            searchy = list(set([item for item in src['searchy'] if type(item) != list]))
            # role-dependent action
            if role == 'parent':
                # convert to integers
                # kids = ['5991423', '85196', '83140', '82439']
                kids = [int(x) for x in src['children']]
                eligible = list(set(kids) - set(pids))  # not slated for deletion
                if len(eligible) == 0:
                    # add to array for deletion
                    delthese.append(pid)
                else:
                    # > 0 eligible children, first promote one to parent
                    # TODO: can we make a logical choice?
                    newparent = eligible[0]
                    newkids = eligible.pop(newparent)
                    # get its index record and update it:
                    # make it a parent, give it a whg_id - _id,
                    # update its sugs and searchy
                    # update its children with newkids
                    qget = {"query": {"bool": {"must": [{"match": {"place_id": newparent}}]}}}
                    res = es.search(index=idx, body=qget)
                    hit = res['hits']['hits'][0]
                    _id = hit['_id']
                    # elevate to parent
                    q_update = {"script": {
                        "source": "ctx._source.whg_id = params._id; \
              ctx._source.relation.name = 'parent'; \
              ctx._source.relation.remove('parent'); \
              ctx._source.children.addAll(params.newkids); \
              ctx._source.suggest.input.addAll(params.sugs); \
              ctx._source.searchy.addAll(params.sugs);",
                        "lang": "painless",
                        "params": {"_id": _id, "newkids": newkids, "sugs": sugs}
                    },
                        "query": {"match": {"place_id": newparent}}
                    }
                    try:
                        es.update_by_query(index=idx, body=q_update)
                    except Exception as e:
                        capture_exception(e)
                    # parent status transfered to 'eligible' child, add to list
                    delthese.append(pid)
            elif role == 'child':
                # get its parent
                parent = src['relation']['parent']
                qget = {"query": {"bool": {"must": [{"match": {"_id": parent}}]}}}
                res = es.search(index=idx, body=qget)
                # parent _source, suggest, searchy
                psrc = res['hits']['hits'][0]['_source']
                psugs = list(set(psrc['suggest']['input']))
                psearchy = list(set([item for item in psrc['searchy'] if type(item) != list]))
                # is parent slated for deletion? (walking dead)
                zombie = psrc['place_id'] in pids
                if not zombie:  # skip zombies here; picked up above with if role == 'parent':
                    # remove this id from children and remove its variants (sugs) from suggest.input and searchy
                    newsugs = list(set(psugs) - set(sugs))
                    newsearchy = list(set(psearchy) - set(searchy))
                    q_update = {"script": {
                        "source": "ctx._source.children.remove(ctx._source.children.indexOf(params.val)); \
                ctx._source.suggest.input = params.sugs; ctx._source.searchy = params.searchy;",
                        "lang": "painless",
                        "params": {"val": str(pid), "sugs": newsugs, "searchy": newsearchy}
                    },
                        "query": {"match": {"_id": parent}}
                    }
                    try:
                        es.update_by_query(index=idx, body=q_update)
                    except Exception as e:
                        capture_exception(e)
                # child's presence in parent removed, add to delthese[]
                delthese.append(pid)
        es.delete_by_query(idx, body={"query": {"terms": {"place_id": delthese}}})


# ES ACTIONS (database now current)
# 1) delete records in rows_delete[]
# 2) update records in rows_replace[]
# defer indexing new records in rows_add until reconciled


if len(rows_delete) > 0:
    for r in rows_delete:
        res = es.search(index=idx, body=esq_pid(r))
        doc = res['hits']['hits'][0]

# delete from rows_delete
qdel = {}

# update from rows_replace
qrepl = {}

# get db record
place = get_object_or_404(Place, pk=pid)

# make an ES doc for it
# pdoc = makeDoc(place,'none')
pdoc = makeDoc(place)

# find it in the index, if exists
qget = {"query": {"bool": {"must": [{"match": {"place_id": pid}}]}}}
res = es.search(index=idx, body=qget)
hits = res['hits']['hits']

if len(hits) > 0:  # if indexed
    # if child get parent's _id else self._id
    if hits[0]['_source']['relation']['name'] == 'child':
        parent_whgid = res['hits']['hits'][0]['_source']['relation']['parent']
    else:
        parent_whgid = res['hits']['hits'][0]['_id']
else:
    pass

# 1) matches parent_whgid
# 2) adds str(place.id) to its children[]
# 3) adds match_names[] values to its suggest field
q_update = {"script": {
    "source": "ctx._source.suggest.input.addAll(params.names); ctx._source.children.add(params.id)",
    "lang": "painless",
    "params": {"names": match_names, "id": str(place.id)}
},
    "query": {"match": {"_id": parent_whgid}}}
es.update_by_query(index=idx, body=q_update, conflicts='proceed')
