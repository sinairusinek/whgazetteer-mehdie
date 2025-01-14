from __future__ import absolute_import, unicode_literals
from celery import shared_task
from django_celery_results.models import TaskResult
from django.conf import settings
from django.core import mail
from django.core.mail import EmailMultiAlternatives
from django.db.models import Q
from django.shortcuts import get_object_or_404
from django.contrib.auth.models import User

import csv, datetime, itertools, re, time
import pandas as pd
import simplejson as json
from copy import deepcopy
from itertools import chain
from sentry_sdk import capture_exception

from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset, Hit
from datasets.static.hashes.parents import ccodes as cchash
from datasets.static.hashes.qtypes import qtypes
from elastic.es_utils import makeDoc, build_qobj, profileHit
from datasets.utils import bestParent, elapsed, getQ, HitRecord, hully, makeNow, parse_wkt, post_recon_update
from main.models import Log
from elasticsearch7 import Elasticsearch
from whg.celery import app

## global for all es connections in this file?
# es = Elasticsearch([{'host': '0.0.0.0',
#                      'port': 9200,
#                      'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
#                      'timeout': 30,
#                      'max_retries': 10,
#                      'retry_on_timeout': True}])

es = Elasticsearch([  # TODO for test
    {
        "host": "34.147.87.121",
        "port": 9200,
        'api_key': (settings.ES_APIKEY_ID, settings.ES_APIKEY_KEY),
        "timeout": 30,
        "max_retries": 10,
        "retry_on_timeout": True
    }
])


@shared_task(name="testy")
def testy():
    print("I'm testy...who wouldn't be?")


""" 
  called by utils.downloader()
  builds download file, retrieved via ajax JS in ds_summary.html, ds_meta.html,
  collection_detail.html (modal), place_collection_browse.html (modal)
"""


@app.task(name="make_download")
def make_download(request, *args, **kwargs):
    # TODO: integrate progress_recorder for better progress bar in GUI
    username = request['username'] or "AnonymousUser"
    userid = request['userid'] or User.objects.get(username="AnonymousUser").id
    req_format = kwargs['format']
    dsid = kwargs['dsid'] or None
    collid = kwargs['collid'] or None
    date = makeNow()

    if collid and not dsid:
        coll = Collection.objects.get(id=collid)
        qs = coll.places.all()
        req_format = 'lpf'
        fn = 'media/downloads/' + username + '_' + collid + '_' + date + '.json'
        outfile = open(fn, 'w', encoding='utf-8')
        features = []
        for p in qs:
            rec = {"type": "Feature",
                   "properties": {"id": p.id, "src_id": p.src_id, "title": p.title, "ccodes": p.ccodes},
                   "geometry": {"type": "GeometryCollection",
                                "geometries": [g.jsonb for g in p.geoms.all()]},
                   "names": [n.jsonb for n in p.names.all()],
                   "types": [t.jsonb for t in p.types.all()],
                   "links": [l.jsonb for l in p.links.all()],
                   "whens": [w.jsonb for w in p.whens.all()],
                   }
            features.append(rec)

        count = str(len(qs))
        result = {"type": "FeatureCollection", "features": features,
                  "@context": "https://raw.githubusercontent.com/LinkedPasts/linked-places/master/linkedplaces-context-v1.1.jsonld",
                  "filename": "/" + fn}
        outfile.write(json.dumps(result, indent=2).replace('null', '""'))
        # TODO: Log object has dataset_id, no collection_id
    elif dsid:
        ds = Dataset.objects.get(pk=dsid)
        dslabel = ds.label
        if collid:
            coll = Collection.objects.get(id=collid)
            qs = coll.places.filter(dataset=ds)
        else:
            qs = ds.places.all()
        count = str(len(qs))

        if ds.format == 'delimited' and req_format in ['tsv', 'delimited']:

            # get header as uploaded and create newheader w/any "missing" columns
            # get latest dataset file
            dsf = ds.file
            # make pandas dataframe
            df = pd.read_csv('media/' + dsf.file.name, delimiter='\t', dtype={'id': 'str', 'aat_types': 'str'})
            # copy existing header to newheader for write
            header = list(df)
            newheader = deepcopy(header)
            # all exports should have these, empty or not
            newheader = list(set(newheader + ['lon', 'lat', 'matches', 'geo_id', 'geo_source', 'geowkt']))

            # name and open csv file for writer
            fn = 'media/downloads/' + username + '_' + dslabel + '_' + date + '.tsv'
            csvfile = open(fn, 'w', newline='', encoding='utf-8')
            writer = csv.writer(csvfile, delimiter='\t', quotechar='', quoting=csv.QUOTE_NONE)

            # TODO: better order?
            writer.writerow(newheader)
            # missing columns (were added to newheader)
            missing = list(set(newheader) - set(list(df)))

            for i, row in df.iterrows():
                dfrow = df.loc[i, :]
                # get db record
                # src_id is NOT distinct amongst all places!!
                p = qs.get(src_id=dfrow['id'], dataset=ds.label)

                # df row to newrow json object
                rowjs = json.loads(dfrow.to_json())
                newrow = deepcopy(rowjs)

                # add missing keys from newheader, if any
                for m in missing:
                    newrow[m] = ''
                # newrow now has all keys -> fill with db values as req.

                # LINKS (matches)
                # get all distinct matches in db as string
                links = (';').join(list(set([l.jsonb['identifier'] for l in p.links.all()])))
                # replace whatever was in file
                newrow['matches'] = links

                # GEOMETRY
                # if db has >0 geom and row has none, add lon/lat and geowkt
                # otherwise, don't bother
                geoms = p.geoms.all()
                if geoms.count() > 0:
                    geowkt = newrow['geowkt'] if 'geowkt' in newrow else None

                    lonlat = [newrow['lon'], newrow['lat']] if \
                        len(set(newrow.keys()) & set(['lon', 'lat'])) == 2 else None
                    # lon/lat may be empty
                    if not geowkt and (not lonlat or None in lonlat or lonlat[0] == ''):
                        # get first db geometry & add to newrow dict
                        g = geoms[0]
                        newrow['geowkt'] = g.geom.wkt if g.geom else ''
                        # there is always jsonb
                        xy = g.geom.coords[0] if g.jsonb['type'] == 'MultiPoint' else g.jsonb['coordinates']
                        newrow['lon'] = xy[0]
                        newrow['lat'] = xy[1]

                # match newrow order to newheader already written
                index_map = {v: i for i, v in enumerate(newheader)}
                ordered_row = sorted(newrow.items(), key=lambda pair: index_map[pair[0]])

                # write it
                csvrow = [o[1] for o in ordered_row]
                writer.writerow(csvrow)
            csvfile.close()
        else:
            # make file name
            fn = 'media/downloads/' + username + '_' + dslabel + '_' + date + '.json'
            outfile = open(fn, 'w', encoding='utf-8')
            features = []
            for p in qs:
                rec = {"type": "Feature",
                       "properties": {"id": p.id, "src_id": p.src_id, "title": p.title, "ccodes": p.ccodes},
                       "geometry": {"type": "GeometryCollection",
                                    "geometries": [g.jsonb for g in p.geoms.all()]},
                       "names": [n.jsonb for n in p.names.all()],
                       "types": [t.jsonb for t in p.types.all()],
                       "links": [l.jsonb for l in p.links.all()],
                       "whens": [w.jsonb for w in p.whens.all()],
                       }
                features.append(rec)

            count = str(len(qs))
            result = {"type": "FeatureCollection",
                      "@context": "https://raw.githubusercontent.com/LinkedPasts/linked-places/master/linkedplaces-context-v1.1.jsonld",
                      "filename": "/" + fn,
                      "decription": ds.description,
                      "features": features}

            outfile.write(json.dumps(result, indent=2).replace('null', '""'))

        Log.objects.create(
            # category, logtype, "timestamp", subtype, note, dataset_id, user_id
            category='dataset',
            logtype='ds_download',
            note={"format": req_format, "username": username},
            dataset_id=dsid,
            user_id=userid
        )

    # for ajax, just report filename
    completed_message = {"msg": req_format + " written", "filename": fn, "rows": count}
    return completed_message


@app.task(name="task_emailer")
def task_emailer(tid, dslabel, username, email, counthit, totalhits):
    # TODO: sometimes a valid tid is not recognized (race?)
    time.sleep(5)
    try:
        task = get_object_or_404(TaskResult, task_id=tid) or False
        tasklabel = 'Wikidata' if task.task_name[6:8] == 'wd' else \
            'Getty TGN' if task.task_name.endswith('tgn') else 'WHGazetteer'
        if task.status == "FAILURE":
            fail_msg = task.result['exc_message']
            text_content = "Greetings " + username + "! Unfortunately, your " + tasklabel + " reconciliation task has completed with status: " + \
                           task.status + ". \nError: " + fail_msg + "\nWHG staff have been notified. We will troubleshoot the issue and get back to you."
            html_content_fail = "<h3>Greetings, " + username + "</h3> <p>Unfortunately, your <b>" + tasklabel + "</b> reconciliation task for the <b>" + dslabel + "</b> dataset has completed with status: " + task.status + ".</p><p>Error: " + fail_msg + ". WHG staff have been notified. We will troubleshoot the issue and get back to you soon.</p>"
        else:
            text_content = "Greetings " + username + "! Your " + tasklabel + " reconciliation task has completed with status: " + \
                           task.status + ". \n" + str(counthit) + " records got a total of " + str(
                totalhits) + " hits.\nRefresh the dataset page and view results on the 'Reconciliation' tab."
            html_content_success = "<h3>Greetings, " + username + "</h3> <p>Your <b>" + tasklabel + "</b> reconciliation task for the <b>" + dslabel + "</b> dataset has completed with status: " + task.status + ". " + str(
                counthit) + " records got a total of " + str(totalhits) + " hits.</p>" + \
                                   "<p>View results on the 'Reconciliation' tab (you may have to refresh the page).</p>"
    except Exception as e:
        capture_exception(e)
        text_content = "Greetings " + username + "! Your reconciliation task for the <b>" + dslabel + "</b> dataset has completed.\n" + \
                       str(counthit) + " records got a total of " + str(
            totalhits) + " hits.\nRefresh the dataset page and view results on the 'Reconciliation' tab."
        html_content_success = "<h3>Greetings, " + username + "</h3> <p>Your reconciliation task for the <b>" + dslabel + "</b> dataset has completed. " + str(
            counthit) + " records got a total of " + str(totalhits) + " hits.</p>" + \
                               "<p>View results on the 'Reconciliation' tab (you may have to refresh the page).</p>"

    subject, from_email = 'WHG reconciliation result', 'whg@kgeographer.org'
    conn = mail.get_connection(
        host=settings.EMAIL_HOST,
        user=settings.EMAIL_HOST_USER,
        use_ssl=settings.EMAIL_USE_SSL,
        password=settings.EMAIL_HOST_PASSWORD,
        port=settings.EMAIL_PORT
    )
    # msg=EmailMessage(
    msg = EmailMultiAlternatives(
        subject,
        text_content,
        from_email,
        [email],
        connection=conn
    )
    msg.bcc = ['mehdie.org@gmail.com']
    msg.attach_alternative(html_content_success if task and task.status == 'SUCCESS' else html_content_fail,
                           "text/html")
    msg.send(fail_silently=False)


# test task for uptimerobot
@app.task(name="testAdd")
def testAdd(n1, n2):
    sum = n1 + n2
    return sum


def types(hit):
    type_array = []
    for t in hit["_source"]['types']:
        if bool(t['placetype'] != None):
            type_array.append(t['placetype'] + ', ' + str(t['display']))
    return type_array


def names(hit):
    name_array = []
    for t in hit["_source"]['names']:
        if bool(t['name'] != None):
            name_array.append(t['name'] + ', ' + str(t['display']))
    return name_array


def toGeoJSON(hit):
    src = hit['_source']
    feat = {"type": "Feature", "geometry": src['location'],
            "aatid": hit['_id'], "tgnid": src['tgnid'],
            "properties": {"title": src['title'], "parents": src['parents'], "names": names(hit), "types": types(hit)}}
    return feat


def reverse(coords):
    fubar = [coords[1], coords[0]]
    return fubar


def maxID(es, idx):
    q = {"query": {"bool": {"must": {"match_all": {}}}},
         "sort": [{"whg_id": {"order": "desc"}}],
         "size": 1
         }
    try:
        res = es.search(index=idx, body=q)
        maxy = int(res['hits']['hits'][0]['_source']['whg_id'])
    except:
        maxy = 12345677
    return maxy


def parseDateTime(string):
    year = re.search("(\d{4})-", string).group(1)
    if string[0] == '-':
        year = year + ' BCE'
    return year.lstrip('0')


def ccDecode(codes):
    countries = []
    for c in codes:
        countries.append(cchash[0][c]['gnlabel'])
    return countries


# generate a language-dependent {name} ({en}) from wikidata variants
def wdTitle(variants, lang):
    if len(variants) == 0:
        return 'unnamed'
    else:
        vl_en = next((v for v in variants if v['lang'] == 'en'), None)
        vl_pref = next((v for v in variants if v['lang'] == lang), None)
        vl_first = next((v for v in variants), None);
        title = vl_pref['names'][0] + (' (' + vl_en['names'][0] + ')' if vl_en else '') \
            if vl_pref and lang != 'en' else vl_en['names'][0] if vl_en else vl_first['names'][0]
        return title


def wdDescriptions(descrips, lang):
    dpref = next((v for v in descrips if v['lang'] == lang), None)
    dstd = next((v for v in descrips if v['lang'] == 'en'), None)

    result = [dstd, dpref] if lang != 'en' else [dstd] \
        if dstd else []
    return result


# create cluster payload from set of hits for a place
def normalize_whg(hits):
    result = []
    src = [h['_source'] for h in hits]
    parents = [h for h in hits if 'whg_id' in h['_source']]
    children = [h for h in hits if 'whg_id' not in h['_source']]
    titles = list(set([h['_source']['title'] for h in hits]))
    [links, countries] = [[], []]
    for h in src:
        countries.append(ccDecode(h['ccodes']))
        for l in h['links']:
            links.append(l['identifier'])
    # each parent seeds cluster of >=1 hit
    for par in parents:
        cluster = {
            "whg_id": par["_id"],
            "titles": titles,
            "countries": list(set(countries)),
            "links": list(set(links)),
            "geoms": [],
            "sources": []
        }
        result.append(cluster)
    return result.toJSON()


# normalize hit json from any authority
# language relevant only for wikidata local)
def normalize(h, auth, language=None):
    if auth.startswith('whg'):
        # for whg h is full hit, not only _source
        hit = deepcopy(h)
        h = hit['_source']
        # build a json object, for Hit.json field
        rec = HitRecord(
            h['place_id'],
            h['dataset'],
            h['src_id'],
            h['title']
        )
        rec.score = hit['_score']
        rec.passnum = hit['pass'][:5]

        # only parents have whg_id
        if 'whg_id' in h:
            rec.whg_id = h['whg_id']

        # add elements if non-empty in index record
        rec.variants = [n['toponym'] for n in h['names']]  # always >=1 names
        # TODO: fix grungy hack (index has both src_label and sourceLabel)
        key = 'src_label' if 'src_label' in h['types'][0] else 'sourceLabel'
        rec.types = [t['label'] + ' (' + t[key] + ')' if t['label'] != None else t[key] \
                     for t in h['types']] if len(h['types']) > 0 else []
        # TODO: rewrite ccDecode to handle all conditions coming from index
        rec.countries = ccDecode(h['ccodes']) if (
                'ccodes' in h.keys() and (len(h['ccodes']) > 0 and h['ccodes'][0] != '')) else []
        rec.parents = ['partOf: ' + r.label + ' (' + parseWhen(r['when']['timespans']) + ')' for r in h['relations']] \
            if 'relations' in h.keys() and len(h['relations']) > 0 else []
        rec.descriptions = h['descriptions'] if len(h['descriptions']) > 0 else []

        rec.geoms = [{
            "type": h['geoms'][0]['location']['type'],
            "coordinates": h['geoms'][0]['location']['coordinates'],
            "id": h['place_id'],
            "ds": "whg"}] \
            if len(h['geoms']) > 0 else []

        rec.minmax = dict(sorted(h['minmax'].items(), reverse=True)) if len(h['minmax']) > 0 else []

        # TODO: deal with whens
        rec.links = [l['identifier'] for l in h['links']] \
            if len(h['links']) > 0 else []
    elif auth == 'wd':
        try:
            # locations and links may be multiple, comma-delimited
            locs = []
            links = []
            if 'locations' in h.keys():
                for l in h['locations']['value'].split(', '):
                    loc = parse_wkt(l)
                    loc["id"] = h['place']['value'][31:]
                    loc['ds'] = 'wd'
                    locs.append(loc)

            rec = HitRecord(-1, 'wd', h['place']['value'][31:], h['placeLabel']['value'])
            rec.variants = []
            rec.types = h['types']['value'] if 'types' in h.keys() else []
            rec.ccodes = [h['countryLabel']['value']]
            rec.parents = h['parents']['value'] if 'parents' in h.keys() else []
            rec.geoms = locs if len(locs) > 0 else []
            rec.links = links if len(links) > 0 else []
            rec.minmax = []
            rec.inception = parseDateTime(h['inception']['value']) if 'inception' in h.keys() else ''
        except Exception as e:
            capture_exception(e)
    elif auth == 'wdlocal':
        # TODO: do it in index?
        variants = h['variants']
        title = wdTitle(variants, language)

        #  place_id, dataset, src_id, title
        rec = HitRecord(-1, 'wd', h['id'], title)

        # list of variant@lang (excldes chosen title)
        v_array = []
        for v in variants:
            for n in v['names']:
                if n != title:
                    v_array.append(n + '@' + v['lang'])
        rec.variants = v_array

        if 'location' in h.keys():
            # single MultiPoint geometry
            loc = h['location']
            loc['id'] = h['id']
            loc['ds'] = 'wd'
            # single MultiPoint geom if exists
            rec.geoms = [loc]

        # turn these identifier claims into links
        qlinks = {
            'P1566': 'gn', 'P1584': 'pl', 'P244': 'loc',
            'P1667': 'tgn', 'P214': 'viaf', 'P268': 'bnf',
            'P2503': 'gov', 'P1871': 'cerl', 'P227': 'gnd'
        }
        links = []
        hlinks = list(
            set(h['claims'].keys()) & set(qlinks.keys()))
        if len(hlinks) > 0:
            for l in hlinks:
                links.append(qlinks[l] + ':' + str(h['claims'][l][0]))

        # add en and FIRST {language} wikipedia sitelink OR first sitelink
        wplinks = []
        wplinks += [l['title'] for l in h['sitelinks'] if l['lang'] == 'en']
        if language != 'en':
            wplinks += [l['title'] for l in h['sitelinks'] if l['lang'] == language]
        # TODO: non-English wp pages do not resolve well

        links += ['wp:' + l for l in set(wplinks)]

        rec.links = links

        # look up Q class labels
        htypes = set(h['claims']['P31'])
        qtypekeys = set([t[0] for t in qtypes.items()])
        rec.types = [qtypes[t] for t in list(set(htypes & qtypekeys))]

        # countries

        rec.ccodes = [
            cchash[0][c]['gnlabel'] for c in cchash[0] if cchash[0][c]['wdid'] in h['claims'].get('P17', [])
        ]

        # include en + native lang if not en
        rec.descriptions = wdDescriptions(h['descriptions'], language) if 'descriptions' in h.keys() else []

        # not applicable
        rec.parents = []

        # no minmax in hit if no inception value(s)
        rec.minmax = [h['minmax']['gte'], h['minmax']['lte']] if 'minmax' in h else []
    elif auth == 'match_data':

        variants = h['variants']
        title = h['title']

        #  place_id, dataset, src_id, title
        rec = HitRecord(-1, 'md', h['src_id'], title)

        # list of variant@lang (excldes chosen title)
        v_array = []
        for v in variants:
            if v['name'] != title:
                v_array.append(v['name'] + '@' + v['lang'])
        rec.variants = v_array

        if 'geoms' in h.keys():
            # single MultiPoint geometry
            if len(h['geoms']) != 0:
                geoms = h['geoms']
                for geom in geoms:
                    geom.update({
                        'id': h['src_id'],
                        'ds': 'md'
                    })
                    # single MultiPoint geom if exists
                rec.geoms = geoms

        # turn these identifier claims into links
        qlinks = {
            'P1566': 'gn', 'P1584': 'pl', 'P244': 'loc',
            'P1667': 'tgn', 'P214': 'viaf', 'P268': 'bnf',
            'P2503': 'gov', 'P1871': 'cerl', 'P227': 'gnd'
        }
        links = []
        hlinks = list(
            set(h['claims'].keys()) & set(qlinks.keys()))
        if len(hlinks) > 0:
            for l in hlinks:
                links.append(qlinks[l] + ':' + str(h['claims'][l][0]))

        # add en and FIRST {language} wikipedia sitelink OR first sitelink
        # wplinks = []
        # wplinks += [l['title'] for l in h['sitelinks'] if l['lang'] == 'en']
        # if language != 'en':
        #     wplinks += [l['title'] for l in h['sitelinks'] if l['lang'] == language]
        # # TODO: non-English wp pages do not resolve well
        #
        # links += ['wp:' + l for l in set(wplinks)]
        #
        # rec.links = links

        # look up Q class labels
        # htypes = set(h['claims']['P31'])
        # qtypekeys = set([t[0] for t in qtypes.items()])
        # rec.types = [qtypes[t] for t in list(set(htypes & qtypekeys))]

        # countries

        # rec.ccodes = [
        #     cchash[0][c]['gnlabel'] for c in cchash[0] if cchash[0][c]['wdid'] in h['claims'].get('P17', [])
        # ]
        #
        # # include en + native lang if not en
        # rec.descriptions = wdDescriptions(h['descriptions'], language) if 'descriptions' in h.keys() else []
        #
        # # not applicable
        # rec.parents = []
        #
        # # no minmax in hit if no inception value(s)
        # rec.minmax = [h['minmax']['gte'], h['minmax']['lte']] if 'minmax' in h else []


    elif auth == 'tgn':
        rec = HitRecord(-1, 'tgn', h['tgnid'], h['title'])
        rec.variants = [n['toponym'] for n in h['names']]  # always >=1 names
        rec.types = [(t['placetype'] if 'placetype' in t and t['placetype'] != None else 'unspecified') + \
                     (' (' + t['id'] + ')' if 'id' in t and t['id'] != None else '') for t in h['types']] \
            if len(h['types']) > 0 else []
        rec.ccodes = []
        rec.parents = ' > '.join(h['parents']) if len(h['parents']) > 0 else []
        rec.descriptions = [h['note']] if h['note'] != None else []
        if 'location' in h.keys():
            rec.geoms = [{
                "type": "Point",
                "coordinates": h['location']['coordinates'],
                "id": h['tgnid'],
                "ds": "tgn"}]
        else:
            rec.geoms = []
        rec.minmax = []
        rec.links = []
    # TODO: raise any errors
    return rec.toJSON()


# ***
# elasticsearch filter from Area (types: predefined, ccodes, drawn)
# e.g. {'type': ['drawn'], 'id': ['128']}
# called from: es_lookup_tgn(), es_lookup_idx(), es_lookup_wdlocal(), search.SearchView(), 
# FUTURE: parse multiple areas
# ***
def get_bounds_filter(bounds, idx):
    id = bounds['id'][0]
    area = Area.objects.get(id=id)
    geofield = "geoms.location" if idx == 'whg' else "location"
    filter = {"geo_shape": {
        geofield: {
            "shape": {
                "type": area.geojson['type'],
                "coordinates": area.geojson['coordinates']
            },
            "relation": "intersects" if idx == 'whg' else 'within'  # within | intersects | contains
        }
    }}
    return filter


"""
perform elasticsearch > tgn queries
from align_tgn()

"""


def es_lookup_tgn(qobj, *args, **kwargs):
    bounds = kwargs['bounds']
    hit_count = 0

    # empty result object
    result_obj = {
        'place_id': qobj['place_id'], 'hits': [],
        'missed': -1, 'total_hits': -1
    }

    # array (includes title)
    variants = list(set(qobj['variants']))

    # bestParent() coalesces mod. country and region; countries.json
    parent = bestParent(qobj)

    # getty aat numerical identifiers
    placetypes = list(set(qobj['placetypes']))

    # base query: name, type, parent, bounds if specified
    # geo_polygon filter added later for pass1; used as-is for pass2
    qbase = {"query": {
        "bool": {
            "must": [
                {"terms": {"names.name": variants}},
                {"terms": {"types.id": placetypes}}
            ],
            "should": [
                {"terms": {"parents": parent}}
            ],
            "filter": [get_bounds_filter(bounds, 'tgn')] if bounds['id'] != ['0'] else []
        }
    }}

    qbare = {"query": {
        "bool": {
            "must": [
                {"terms": {"names.name": variants}}
            ],
            "should": [
                {"terms": {"parents": parent}}
            ],
            "filter": [get_bounds_filter(bounds, 'tgn')] if bounds['id'] != ['0'] else []
        }
    }}

    # grab deep copy of qbase, add w/geo filter if 'geom'
    q1 = deepcopy(qbase)

    # create 'within polygon' filter and add to q1
    if 'geom' in qobj.keys():
        location = qobj['geom']
        # always polygon returned from hully(g_list)
        filter_within = {"geo_polygon": {
            "repr_point": {
                "points": location['coordinates']
            }
        }
        }
        q1['query']['bool']['filter'].append(filter_within)

    # /\/\/\/\/\/
    # pass1: must[name]; should[type,parent]; filter[bounds,geom]
    # /\/\/\/\/\/
    try:
        res1 = es.search(index="tgn", body=q1)
        hits1 = res1['hits']['hits']
    except Exception as e:
        capture_exception(e)
    if len(hits1) > 0:
        for hit in hits1:
            hit_count += 1
            hit['pass'] = 'pass1'
            result_obj['hits'].append(hit)
    elif len(hits1) == 0:

        q2 = qbase
        try:
            res2 = es.search(index="tgn", body=q2)
            hits2 = res2['hits']['hits']
        except Exception as e:
            capture_exception(e)
        if len(hits2) > 0:
            for hit in hits2:
                hit_count += 1
                hit['pass'] = 'pass2'
                result_obj['hits'].append(hit)
        elif len(hits2) == 0:
            q3 = qbare
            try:
                res3 = es.search(index="tgn", body=q3)
                hits3 = res3['hits']['hits']
            except Exception as e:
                capture_exception(e)
            if len(hits3) > 0:
                for hit in hits3:
                    hit_count += 1
                    hit['pass'] = 'pass3'
                    result_obj['hits'].append(hit)
            else:
                result_obj['missed'] = qobj['place_id']
    result_obj['hit_count'] = hit_count
    return result_obj


"""
manage align/reconcile to tgn
get result_obj per Place via es_lookup_tgn()
parse, write Hit records for review

"""


@app.task(name='align_match_data')
def align_match_data(pk, *args, **kwargs):
    start = datetime.datetime.now()
    dataset = get_object_or_404(Dataset, id=pk)
    dataset_2 = get_object_or_404(Dataset, id=kwargs.get('dataset_2'))
    language = kwargs['lang']

    csv_data = pd.read_csv(kwargs.get('csv_url'))

    places = dataset.places.all()
    places_2 = dataset_2.places.all()
    count_hit = 0
    for data in csv_data.values:
        place = places.filter(src_id=data[0])
        place_2 = places_2.filter(src_id=data[1])

        if not (place.exists() and place_2.exists()):
            continue
        place = place.first()
        place_2 = place_2.first()

        qobj = {"place_id": place_2.id,
                "src_id": place_2.src_id,
                "type": "match_data",
                "title": place_2.title,
                "fclasses": place_2.fclasses or [],
                "geoms": [],
                "variants": [],
                "claims": {},
                }
        [variants, geoms, types, ccodes, parents] = [[], [], [], [], []]

        for c in place_2.ccodes:
            ccodes.append(c.upper())
        qobj['countries'] = place_2.ccodes

        for t in place_2.types.all():
            types.append(t.jsonb['identifier'])
        qobj['placetypes'] = types

        variants.append(place.title)
        for name in place.names.all():
            qobj['variants'].append({
                'name': name.toponym,
                'lang': language
            })

        if len(place_2.related.all()) > 0:
            for rel in place_2.related.all():
                if rel.jsonb['relationType'] == 'gvp:broaderPartitive':
                    parents.append(rel.jsonb['label'])
            qobj['parents'] = parents
        else:
            qobj['parents'] = []

        if len(place_2.geoms.all()) > 0:
            geoms = [g.jsonb for g in place_2.geoms.all()]
            # make everything a simple polygon hull for spatial filter
            for geom in geoms:
                geom['coordinates'] = [geom['coordinates']]
            qobj['geoms'] = geoms

        if len(place_2.links.all()) > 0:
            l_list = [l.jsonb['identifier'] for l in place_2.links.all()]
            qobj['authids'] = l_list
        else:
            qobj['authids'] = []

        Hit.objects.create(
            authority='md',
            authrecord_id=data[1],
            dataset=dataset,
            place=place,
            task_id=align_match_data.request.id,
            query_pass='pass1',
            json=normalize(qobj, qobj['type'], language),
            src_id=place.src_id,
            score=data[4],
            geom=None,
            reviewed=False,
            matched=False,
        )
        count_hit += 1
    end = datetime.datetime.now()

    return {
        'count': dataset.places.all().count(),
        'got_hits': count_hit,
        'total_hits': len(csv_data.values),
        'pass1': count_hit,
        'pass2': 0,  # TODO fix
        'pass3': 0,
        'no_hits': {'count': len(csv_data.values) - count_hit},
        'elapsed': elapsed(end - start)
    }


@app.task(name="align_tgn")
def align_tgn(pk, *args, **kwargs):
    task_id = align_tgn.request.id
    ds = get_object_or_404(Dataset, id=pk)
    user = get_object_or_404(User, pk=kwargs['user'])
    bounds = kwargs['bounds']
    scope = kwargs['scope']
    hit_parade = {"summary": {}, "hits": []}
    [nohits, tgn_es_errors, features] = [[], [], []]
    [count_hit, count_nohit, total_hits, count_p1, count_p2, count_p3] = [0, 0, 0, 0, 0, 0]
    start = datetime.datetime.now()

    # queryset depends 'scope'
    qs = ds.places.all() if scope == 'all' else \
        ds.places.filter(~Q(review_tgn=1))

    for place in qs:
        # build query object
        qobj = {"place_id": place.id,
                "src_id": place.src_id,
                "title": place.title}
        [variants, geoms, types, ccodes, parents] = [[], [], [], [], []]

        # ccodes (2-letter iso codes)
        for c in place.ccodes:
            ccodes.append(c.upper())
        qobj['countries'] = place.ccodes

        # types (Getty AAT identifiers)
        # all have 'aat:' prefixes
        for t in place.types.all():
            types.append(t.jsonb['identifier'])
        qobj['placetypes'] = types

        # names
        for name in place.names.all():
            variants.append(name.toponym)
        qobj['variants'] = variants

        # parents
        # TODO: other relations
        if len(place.related.all()) > 0:
            for rel in place.related.all():
                if rel.jsonb['relationType'] == 'gvp:broaderPartitive':
                    parents.append(rel.jsonb['label'])
            qobj['parents'] = parents
        else:
            qobj['parents'] = []

        # geoms
        if len(place.geoms.all()) > 0:
            g_list = [g.jsonb for g in place.geoms.all()]
            # make everything a simple polygon hull for spatial filter
            qobj['geom'] = hully(g_list)

        ## run pass1-pass3 ES queries
        result_obj = es_lookup_tgn(qobj, bounds=bounds)

        if result_obj['hit_count'] == 0:
            count_nohit += 1
            nohits.append(result_obj['missed'])
        else:
            place.review_tgn = 0
            place.save()
            count_hit += 1
            total_hits += len(result_obj['hits'])
            for hit in result_obj['hits']:
                if hit['pass'] == 'pass1':
                    count_p1 += 1
                elif hit['pass'] == 'pass2':
                    count_p2 += 1
                elif hit['pass'] == 'pass3':
                    count_p3 += 1
                hit_parade["hits"].append(hit)
                # correct lower case 'point' in tgn index
                # TODO: fix in index
                if 'location' in hit['_source'].keys():
                    loc = hit['_source']['location']
                    loc['type'] = "Point"
                else:
                    loc = {}
                new = Hit(
                    authority='tgn',
                    authrecord_id=hit['_id'],
                    dataset=ds,
                    place=place,
                    task_id=align_tgn.request.id,
                    query_pass=hit['pass'],
                    json=normalize(hit['_source'], 'tgn'),
                    src_id=qobj['src_id'],
                    score=hit['_score'],
                    geom=loc,
                    reviewed=False,
                    matched=False,
                )
                new.save()
    end = datetime.datetime.now()

    hit_parade['summary'] = {
        'count': qs.count(),
        'got_hits': count_hit,
        'total_hits': total_hits,
        'pass1': count_p1,
        'pass2': count_p2,
        'pass3': count_p3,
        'no_hits': {'count': count_nohit},
        'elapsed': elapsed(end - start)
    }

    # create log entry and update ds status
    post_recon_update(ds, user, 'tgn')

    # email owner when complete
    task_emailer.delay(
        task_id,
        ds.label,
        user.username,
        user.email,
        count_hit,
        total_hits
    )

    return hit_parade['summary']


"""
performs elasticsearch > wdlocal queries
from align_wdlocal()

"""


def es_lookup_wdlocal(qobj, *args, **kwargs):
    bounds = kwargs['bounds']
    hit_count = 0

    # empty result object
    result_obj = {
        'place_id': qobj['place_id'],
        'hits': [], 'missed': -1, 'total_hits': -1}

    # names (distinct, w/o language)
    variants = list(set(qobj['variants']))

    # types
    # wikidata Q ids for aat_ids, ccodes; strip wd: prefix
    # if no aatids, returns ['Q486972'] (human settlement)
    qtypes = [t[3:] for t in getQ(qobj['placetypes'], 'types')]

    # prep spatial

    # if no ccodes, returns []
    countries = [t[3:] for t in getQ(qobj['countries'], 'ccodes')]

    has_bounds = bounds['id'] != ['0']
    has_geom = 'geom' in qobj.keys()
    has_countries = len(countries) > 0
    if has_bounds:
        area_filter = get_bounds_filter(bounds, 'wd')
    if has_geom:
        shape_filter = {"geo_shape": {
            "location": {
                "shape": {
                    "type": qobj['geom']['type'],
                    "coordinates": qobj['geom']['coordinates']},
                "relation": "intersects"}
        }}
    if has_countries:
        countries_match = {"terms": {"claims.P17": countries}}

    # prelim query: any authid matches?
    # can be accepted without review
    # incoming qobj['authids'] might include
    # a wikidata identifier matching an index _id (Qnnnnnnn)
    # OR an id match in wikidata authids[] e.g. gn:, tgn:, pl:, bnf:, viaf:
    q0 = {"query": {
        "bool": {
            "must": [
                {"bool": {
                    "should": [
                        {"terms": {"authids": qobj['authids']}},
                        {"terms": {"_id": [i[3:] for i in qobj['authids']]}}
                    ],
                    "minimum_should_match": 1
                }}
            ]
        }}}

    # base query
    qbase = {"query": {
        "bool": {
            "must": [
                {"terms": {"variants.names": variants}}
            ],
            # boosts score if matched
            "should": [
                {"terms": {"authids": qobj['authids']}}
            ],
            "filter": []
        }
    }}

    # add spatial filter as available in qobj
    if has_geom:
        # shape_filter is polygon hull ~100km diameter
        qbase['query']['bool']['filter'].append(shape_filter)
        if has_countries:
            qbase['query']['bool']['should'].append(countries_match)
    elif has_countries:
        # matches ccodes
        qbase['query']['bool']['must'].append(countries_match)
    elif has_bounds:
        # area_filter (predefined region or study area)
        qbase['query']['bool']['filter'].append(area_filter)
        if has_countries:
            qbase['query']['bool']['should'].append(countries_match)

    # q1 = qbase + types
    q1 = deepcopy(qbase)
    q1['query']['bool']['must'].append(
        {"terms": {"types.id": qtypes}}
    )

    # add fclasses if any, drop types; geom if any remains
    q2 = deepcopy(qbase)
    if len(qobj['fclasses']) > 0:
        q2['query']['bool']['must'].append(
            {"terms": {"fclasses": qobj['fclasses']}})

    # /\/\/\/\/\/
    # pass0 (q0):
    # must[authid]; match any link
    # /\/\/\/\/\/
    try:
        res0 = es.search(index="wd", body=q0)
        hits0 = res0['hits']['hits']
    except Exception as e:
        hits0 = []
        capture_exception(e)

    if len(hits0) > 0:
        for hit in hits0:
            hit_count += 1
            hit['pass'] = 'pass0'
            result_obj['hits'].append(hit)
    elif len(hits0) == 0:
        # /\/\/\/\/\/
        # pass1 (q1):
        # must[name, placetype]; spatial filter
        # /\/\/\/\/\/
        try:
            res1 = es.search(index="wd", body=q1)
            hits1 = res1['hits']['hits']
        except Exception as e:
            hits1 = []
            capture_exception(e)
        if len(hits1) > 0:
            for hit in hits1:
                hit_count += 1
                hit['pass'] = 'pass1'
                result_obj['hits'].append(hit)
        elif len(hits1) == 0:
            try:
                res2 = es.search(index="wd", body=q2)
                hits2 = res2['hits']['hits']
            except Exception as e:
                hits2 = []
                capture_exception(e)
            if len(hits2) > 0:
                for hit in hits2:
                    hit_count += 1
                    hit['pass'] = 'pass2'
                    result_obj['hits'].append(hit)
            elif len(hits2) == 0:
                result_obj['missed'] = str(qobj['place_id']) + ': ' + qobj['title']
    result_obj['hit_count'] = hit_count
    return result_obj


"""
manage align/reconcile to local wikidata index
get result_obj per Place via es_lookup_wdlocal()
parse, write Hit records for review

"""


@app.task(name="align_wdlocal")
def align_wdlocal(pk, **kwargs):
    task_id = align_wdlocal.request.id
    ds = get_object_or_404(Dataset, id=pk)
    user = get_object_or_404(User, pk=kwargs['user'])
    bounds = kwargs['bounds']
    scope = kwargs['scope']
    language = kwargs['lang']
    hit_parade = {"summary": {}, "hits": []}
    [nohits, wdlocal_es_errors, features] = [[], [], []]
    [count_hit, count_nohit, total_hits, count_p0, count_p1, count_p2] = [0, 0, 0, 0, 0, 0]
    start = datetime.datetime.now()

    # queryset depends on 'scope'
    qs = ds.places.all() if scope == 'all' else \
        ds.places.filter(~Q(review_wd=1))

    for place in qs:
        # build query object
        qobj = {"place_id": place.id,
                "src_id": place.src_id,
                "title": place.title,
                "fclasses": place.fclasses or []}

        [variants, geoms, types, ccodes, parents, links] = [[], [], [], [], [], []]

        # ccodes (2-letter iso codes)
        for c in place.ccodes:
            ccodes.append(c.upper())
        qobj['countries'] = place.ccodes

        # types (Getty AAT integer ids if available)
        for t in place.types.all():
            if t.jsonb['identifier'].startswith('aat:'):
                types.append(int(t.jsonb['identifier'].replace('aat:', '')))
        qobj['placetypes'] = types

        # variants
        variants.append(place.title)
        for name in place.names.all():
            variants.append(name.toponym)
        qobj['variants'] = list(set(variants))

        # parents
        # TODO: other relations
        if len(place.related.all()) > 0:
            for rel in place.related.all():
                if rel.jsonb['relationType'] == 'gvp:broaderPartitive':
                    parents.append(rel.jsonb['label'])
            qobj['parents'] = parents
        else:
            qobj['parents'] = []

        # geoms
        if len(place.geoms.all()) > 0:
            g_list = [g.jsonb for g in place.geoms.all()]
            # make simple polygon hull for ES shape filter
            qobj['geom'] = hully(g_list)

        # 'P1566':'gn', 'P1584':'pleiades', 'P244':'loc', 'P214':'viaf', 'P268':'bnf', 'P1667':'tgn',
        # 'P2503':'gov', 'P1871':'cerl', 'P227':'gnd'
        if len(place.links.all()) > 0:
            l_list = [l.jsonb['identifier'] for l in place.links.all()]
            qobj['authids'] = l_list
        else:
            qobj['authids'] = []

        # TODO: ??? skip records that already have a Wikidata record in l_list
        # they are returned as Pass 0 hits right now
        # run pass0-pass2 ES queries
        result_obj = es_lookup_wdlocal(qobj, bounds=bounds)

        if result_obj['hit_count'] == 0:
            count_nohit += 1
            nohits.append(result_obj['missed'])
        else:
            place.review_wd = 0
            place.save()

            count_hit += 1
            total_hits += len(result_obj['hits'])
            for hit in result_obj['hits']:
                if hit['pass'] == 'pass0':
                    count_p0 += 1
                if hit['pass'] == 'pass1':
                    count_p1 += 1
                elif hit['pass'] == 'pass2':
                    count_p2 += 1
                hit_parade["hits"].append(hit)
                new = Hit(
                    authority='wd',
                    authrecord_id=hit['_id'],
                    dataset=ds,
                    place=place,
                    task_id=task_id,
                    query_pass=hit['pass'],
                    # prepare for consistent display in review screen
                    json=normalize(hit['_source'], 'wdlocal', language),
                    src_id=qobj['src_id'],
                    score=hit['_score'],
                    reviewed=False,
                    matched=False
                )
                new.save()
    end = datetime.datetime.now()

    hit_parade['summary'] = {
        'count': qs.count(),
        'got_hits': count_hit,
        'total_hits': total_hits,
        'pass0': count_p0,
        'pass1': count_p1,
        'pass2': count_p2,
        'no_hits': {'count': count_nohit},
        'elapsed': elapsed(end - start)
    }

    # create log entry and update ds status
    post_recon_update(ds, user, 'wdlocal')

    # email owner when complete
    task_emailer.delay(
        task_id,
        ds.label,
        user.username,
        user.email,
        count_hit,
        total_hits
    )

    return hit_parade['summary']


"""
# performs elasticsearch > whg index queries
# from align_idx(), returns result_obj

"""


def es_lookup_idx(qobj, *args, **kwargs):
    global whg_id
    idx = 'whg'
    bounds = kwargs['bounds']
    [hitobjlist, _ids] = [[], []]

    # empty result object
    result_obj = {
        'place_id': qobj['place_id'],
        'title': qobj['title'],
        'hits': [], 'missed': -1, 'total_hits': 0,
        'hit_count': 0
    }
    # de-dupe
    variants = list(set(qobj["variants"]))
    links = list(set(qobj["links"]))
    # copy for appends
    linklist = deepcopy(links)
    has_fclasses = len(qobj["fclasses"]) > 0

    # prep spatial constraints
    has_bounds = bounds["id"] != ["0"]
    has_geom = "geom" in qobj.keys()
    has_countries = len(qobj["countries"]) > 0

    if has_bounds:
        area_filter = get_bounds_filter(bounds, "whg")
    if has_geom:
        shape_filter = {"geo_shape": {
            "geoms.location": {
                "shape": {
                    "type": qobj["geom"]["type"],
                    "coordinates": qobj["geom"]["coordinates"]},
                "relation": "intersects"}
        }}
    if has_countries:
        countries_match = {"terms": {"ccodes": qobj["countries"]}}

    """
    prepare queries from qobj
    """
    # q0 is matching concordance identifiers, boosted by name matches
    # TODO: are scores used?
    q0 = {
        "query": {"bool": {"must": [
            {"terms": {"links.identifier": linklist}},
            {"bool": {"should": [
                {"terms": {"names.toponym": variants}},
                {"terms": {"searchy": variants}}]}}
        ]
        }}}

    # build q1 from qbase + spatial context, fclasses if any
    qbase = {"size": 100, "query": {
        "bool": {
            "must": [
                {"exists": {"field": "whg_id"}},
                # must match a or b or c
                {"bool": {
                    "should": [
                        {"terms": {"names.toponym": variants}},
                        {"terms": {"title": variants}},
                        {"terms": {"searchy": variants}}
                    ]
                }
                }
            ],
            "should": [
                # bool::should boosts score
                {"terms": {"links.identifier": qobj["links"]}},
                {"terms": {"types.identifier": qobj["placetypes"]}}
            ],
            # spatial filters added according to what"s available
            "filter": []
        }
    }}

    # ADD SPATIAL
    if has_geom:
        qbase["query"]["bool"]["filter"].append(shape_filter)

    # no geom, use country codes if there
    if not has_geom and has_countries:
        qbase["query"]["bool"]["must"].append(countries_match)

    # has no geom but has bounds (region or user study area)
    if not has_geom and has_bounds:
        # area_filter (predefined region or study area)
        qbase["query"]["bool"]["filter"].append(area_filter)
        if has_countries:
            # add weight for country match
            qbase["query"]["bool"]["should"].append(countries_match)

    # ADD fclasses IF ANY
    if has_fclasses:
        qbase["query"]["bool"]["must"].append(
            {"terms": {"fclasses": qobj["fclasses"]}})
    # grab a copy
    q1 = qbase

    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # pass0a, pass0b
    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

    result0a = es.search(index=idx, body=q0)
    hits0a = result0a["hits"]["hits"]

    if len(hits0a) > 0:
        # >=1 matching identifier
        result_obj['hit_count'] += len(hits0a)
        for h in hits0a:
            # add full hit to result
            result_obj["hits"].append(h)
            # pull some fields for analysis
            h["pass"] = "pass0a"
            relation = h["_source"]["relation"]
            hitobj = {
                "_id": h['_id'],
                "pid": h["_source"]['place_id'],
                "title": h["_source"]['title'],
                "dataset": h["_source"]['dataset'],
                "pass": "pass0",
                "links": [l["identifier"] \
                          for l in h["_source"]["links"]],
                "role": relation["name"],
                "children": h["_source"]["children"]
            }
            if "parent" in relation.keys():
                hitobj["parent"] = relation["parent"]
            # add profile to hitlist
            hitobjlist.append(hitobj)
        _ids = [h['_id'] for h in hitobjlist]
        for hobj in hitobjlist:
            for l in hobj['links']:
                linklist.append(l) if l not in linklist else linklist

        # if new links, crawl again
        if len(set(linklist) - set(links)) > 0:
            result0b = es.search(index=idx, body=q0)
            hits0b = result0b["hits"]["hits"]
            # add new results if any to hitobjlist and result_obj["hits"]
            result_obj['hit_count'] += len(hits0b)
            for h in hits0b:
                if h['_id'] not in _ids:
                    _ids.append(h['_id'])
                    relation = h["_source"]["relation"]
                    h["pass"] = "pass0b"
                    hitobj = {
                        "_id": h['_id'],
                        "pid": h["_source"]['place_id'],
                        "title": h["_source"]['title'],
                        "dataset": h["_source"]['dataset'],
                        "pass": "pass0b",
                        "links": [l["identifier"] \
                                  for l in h["_source"]["links"]],
                        "role": relation["name"],
                        "children": h["_source"]["children"]
                    }
                    if "parent" in relation.keys():
                        hitobj["parent"] = relation["parent"]
                    if hitobj['_id'] not in [h['_id'] for h in hitobjlist]:
                        result_obj["hits"].append(h)
                        hitobjlist.append(hitobj)
                    result_obj['total_hits'] = len((result_obj["hits"]))

    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # run pass1 whether pass0 had hits or not
    # q0 only found identifier matches
    # now get other potential hits in normal manner
    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    try:
        result1 = es.search(index=idx, body=q1)
        hits1 = result1["hits"]["hits"]
    except Exception as e:
        capture_exception(e)
        h["pass"] = "pass1"

    result_obj['hit_count'] += len(hits1)
    try:
        for h in hits1:
            # filter out _ids found in pass0
            # any hit on identifiers will also turn up here based on context
            if h['_id'] not in _ids:
                _ids.append(h['_id'])
                relation = h["_source"]["relation"]
                h["pass"] = "pass1"
                hitobj = {
                    "_id": h['_id'],
                    "pid": h["_source"]['place_id'],
                    "title": h["_source"]['title'],
                    "dataset": h["_source"]['dataset'],
                    "pass": "pass1",
                    "links": [l["identifier"] \
                              for l in h["_source"]["links"]],
                    "role": relation["name"],
                    "children": h["_source"]["children"]
                }
                if "parent" in relation.keys():
                    hitobj["parent"] = relation["parent"]
                if hitobj['_id'] not in [h['_id'] for h in hitobjlist]:
                    result_obj["hits"].append(h)
                    hitobjlist.append(hitobj)
                result_obj['total_hits'] = len(result_obj["hits"])

        return result_obj
    except Exception as e:
        capture_exception(e)


"""
# align/accession to whg index
# gets result_obj per Place
# writes 'union' Hit records to db for review
# OR writes seed parent to whg index
"""


@app.task(name="align_idx")
def align_idx(pk, *args, **kwargs):
    task_id = align_idx.request.id
    ds = get_object_or_404(Dataset, id=pk)
    idx = 'whg'
    user = get_object_or_404(User, id=kwargs['user'])
    # get last index identifier (used for _id)
    whg_id = maxID(es, idx)
    """
    kwargs: {'ds': 1231, 'dslabel': 'owt10b', 'owner': 14, 'user': 1, 
      'bounds': {'type': ['userarea'], 'id': ['0']}, 'aug_geom': 'on', 
      'scope': 'all', 'lang': 'en'}
    """
    """
      {'csrfmiddlewaretoken': ['Z3vg1TOlJRNTYSErmyNYuuaoTYMmk8235pMea2nXHtxvpfmvmdqPsqRHeefFqt2u'], 
      'ds': ['1231'], 
      'wd_lang': [''], 
      'recon': ['idx'], 
      'lang': ['']}>
    """
    bounds = kwargs['bounds']

    hit_parade = {"summary": {}, "hits": []}
    [count_hit, count_nohit, total_hits, count_p0, count_p1] = [0, 0, 0, 0, 0]
    [count_errors, count_seeds, count_kids, count_fail] = [0, 0, 0, 0]
    new_seeds = []
    start = datetime.datetime.now()

    # limit scope if some are already indexed
    qs = ds.places.filter(indexed=False)
    # TODO: scope = 'all' should be not possible for align_idx

    """
    for each place, create qobj and run es_lookup_idx(qobj)
    if hits: write Hit instances for review
    if no hits: write new parent doc in index
    """
    for p in qs:
        qobj = build_qobj(p)

        result_obj = es_lookup_idx(qobj, bounds=bounds)

        # PARSE RESULTS
        # no hits on any pass, it's a new seed/parent
        if len(result_obj['hits']) == 0:
            # create new parent (write to file for inspection)
            whg_id += 1
            doc = makeDoc(p)
            doc['relation']['name'] = 'parent'
            doc['whg_id'] = whg_id
            # get names for search fields
            names = [p.toponym for p in p.names.all()]
            doc['searchy'] = names
            doc['suggest']['input'] = names
            new_seeds.append(doc)
            p.indexed = True
            p.save()

        # got some hits, format json & write to db as for align_wdlocal, etc.
        elif len(result_obj['hits']) > 0:
            count_hit += 1  # this record got >=1 hits
            # set place/task status to 0 (has unreviewed hits)
            p.review_whg = 0
            p.save()

            hits = result_obj['hits']
            [count_kids, count_errors] = [0, 0]
            total_hits += result_obj['total_hits']

            # identify parents and children
            parents = [profileHit(h) for h in hits \
                       if h['_source']['relation']['name'] == 'parent']
            children = [profileHit(h) for h in hits \
                        if h['_source']['relation']['name'] == 'child']

            """ *** """
            p0 = len(set(['pass0a', 'pass0b']) & set([p['pass'] for p in parents])) > 0
            p1 = 'pass1' in [p['pass'] for p in parents]
            if p0:
                count_p0 += 1
            elif p1:
                count_p1 += 1

            def uniq_geom(lst):
                for _, grp in itertools.groupby(lst, lambda d: (d['coordinates'])):
                    yield list(grp)[0]

            # if there are any
            for par in parents:
                # 20220828 test
                # any children of *this* parent in this result?
                kids = [c for c in children if c['_id'] in par['children']] or None
                # merge values into hit.json object
                # profile keys ['_id', 'pid', 'title', 'role', 'dataset', 'parent',
                # 'children', 'links', 'countries', 'variants', 'geoms']
                # boost parent score if kids
                score = par['score'] + sum([k['score'] for k in kids]) if kids else par['score']
                hitobj = {
                    'whg_id': par['_id'],
                    'pid': par['pid'],
                    'score': score,
                    'titles': [par['title']],
                    'countries': par['countries'],
                    'geoms': list(uniq_geom(par['geoms'])),
                    'links': par['links'],
                    'sources': [
                        {'dslabel': par['dataset'],
                         'pid': par['pid'],
                         'variants': par['variants'],
                         'types': par['types'],
                         'children': par['children'],
                         'minmax': par['minmax'],
                         'pass': par['pass'][:5]
                         }]
                }
                if kids:
                    hitobj['titles'].extend([k['title'] for k in kids])
                    hitobj['countries'].extend([','.join(k['countries']) for k in kids])

                    # unnest
                    hitobj['geoms'].extend(list(chain.from_iterable([k['geoms'] for k in kids])))
                    hitobj['links'].extend(list(chain.from_iterable([k['links'] for k in kids])))

                    # add kids to parent in sources
                    hitobj['sources'].extend(
                        [{'dslabel': k['dataset'],
                          'pid': k['pid'],
                          'variants': k['variants'],
                          'types': k['types'],
                          'minmax': k['minmax'],
                          'pass': k['pass'][:5]} for k in kids])

                passes = list(set([s['pass'] for s in hitobj['sources']]))
                hitobj['passes'] = passes

                hitobj['titles'] = ', '.join(list(dict.fromkeys(hitobj['titles'])))

                if hitobj['links']:
                    hitobj['links'] = list(dict.fromkeys(hitobj['links']))

                hitobj['countries'] = ', '.join(list(dict.fromkeys(hitobj['countries'])))
                new = Hit(
                    task_id=task_id,
                    authority='whg',

                    # incoming place
                    dataset=ds,
                    place=p,
                    src_id=p.src_id,

                    # candidate parent, might have children
                    authrecord_id=par['_id'],
                    query_pass=', '.join(passes),  #
                    score=hitobj['score'],
                    geom=hitobj['geoms'],
                    reviewed=False,
                    matched=False,
                    json=hitobj
                )
                new.save()

    end = datetime.datetime.now()

    hit_parade['summary'] = {
        'count': qs.count(),  # records in dataset
        'got_hits': count_hit,  # count of parents
        'total_hits': total_hits,  # overall total
        'seeds': len(new_seeds),  # new index seeds
        'pass0': count_p0,
        'pass1': count_p1,
        'elapsed_min': elapsed(end - start),
        'skipped': count_fail
    }

    # create log entry and update ds status
    post_recon_update(ds, user, 'idx')

    # email owner when complete
    task_emailer.delay(
        task_id,
        ds.label,
        user.username,
        user.email,
        count_hit,
        total_hits
    )
    return hit_parade['summary']
