import re
import json
import copy
from pprint import pprint  # noqa

import requests

from memorious.helpers import make_id


# there's been some trouble in the past with regards to the
# greographic reference system used. the settings here
# should emit the closest that ESRI will give you in lieu of
# WGS84 (i.e. Google Maps).
QUERY = {
    "where": "1=1",
    "outFields": "*",
    "geometryType": "esriGeometryEnvelope",
    # 'geometryType': 'esriGeometryPolygon',
    "spatialRel": "esriSpatialRelIntersects",
    # 'units': 'esriSRUnit_Meter',
    # 'outSR': 102100,  # wgs 84
    # 'resultRecordCount': 500,
    # 'resultOffset': 0,
    # 'returnGeometry': 'true',
    "returnGeometry": "false",
    "f": "pjson",
}


def split_envelope(env):
    xmin = env["xmin"]
    xlen = env["xmax"] - env["xmin"]
    xhalf = xlen / 2.0
    ymin = env["ymin"]
    ylen = env["ymax"] - env["ymin"]
    yhalf = ylen / 2.0

    yield {
        "spatialReference": env["spatialReference"],
        "xmax": xmin + xhalf,
        "xmin": xmin,
        "ymax": ymin + yhalf,
        "ymin": ymin,
    }
    yield {
        "spatialReference": env["spatialReference"],
        "xmax": xmin + xlen,
        "xmin": xmin + xhalf,
        "ymax": ymin + yhalf,
        "ymin": ymin,
    }
    yield {
        "spatialReference": env["spatialReference"],
        "xmax": xmin + xhalf,
        "xmin": xmin,
        "ymax": ymin + ylen,
        "ymin": ymin + yhalf,
    }
    yield {
        "spatialReference": env["spatialReference"],
        "xmax": xmin + xlen,
        "xmin": xmin + xhalf,
        "ymax": ymin + ylen,
        "ymin": ymin + yhalf,
    }


def load_features(context, data, seen, extent):
    q = QUERY.copy()
    if data["token"] is not None:
        q["token"] = data["token"]
    q["geometry"] = json.dumps(extent)
    url = "%s/%s/query" % (data["rest_url"], data["id"])
    # TODO: For some weird reason, using context.http returns 403 errors.
    # Things I have tried: reseting the session, setting stealth to true so
    # that the User-Agent is randomized, getting rid of all request headers.
    # None of it helps. So using requests instead because it works.
    res = requests.get(url, params=q).json()
    for feature in res.get("features", []):
        attrs = feature.get("attributes")
        obj = make_id(
            data.get("name"),
            attrs.get("guidPart"),
            attrs.get("OBJECTID_1"),
            attrs.get("OBJECTID_12"),
            attrs.get("OBJECTID"),
            attrs.get("ESRI_OID"),
        )
        if obj is None:
            context.log.info("Missing ID: %r", attrs.keys())
        if obj not in seen:
            seen.add(obj)
            attrs["FeatureId"] = obj
            yield attrs
    if res.get("exceededTransferLimit"):
        for child in split_envelope(extent):
            for attrs in load_features(context, data, seen, child):
                yield attrs


def layer(context, data):
    for feature in load_features(context, data, set(), data["extent"]):
        out = copy.deepcopy(data)
        out["feature"] = feature
        context.emit(data=out)


def layers(context, data):
    url = context.params.get("url")
    res = context.http.get(url)
    # some ugly stuff to extraxt the access token from the portal
    # site.
    groups = re.search(r"MainPage\.Init\('(.*)'", res.text)
    text = groups.group(1)
    text = text.replace("\\\\\\'", "")
    text = text.replace("\\'", "")
    text = text.replace('\\\\\\"', "")

    text = '"%s"' % text
    cfg = json.loads(json.loads(text))

    extras = cfg.get("Extras")
    token = None
    if extras and len(extras):
        token = extras.pop()

    data = {"portal_title": cfg["Title"], "portal_url": url}
    layers = context.params.get("layers")
    context.log.info("Scraping: %(portal_title)s", data)
    for service in cfg["MapServices"]:
        if service["MapServiceType"] not in ["Dynamic", "Features"]:
            continue
        token = token or service.get("ArcGISToken")
        params = {"f": "json"}
        if token is not None:
            params["token"] = token
        res = requests.get(service["RestUrl"], params=params).json()
        for layer in res.get("layers"):
            layer["extent"] = res["fullExtent"]
            layer["token"] = token
            layer["rest_url"] = service["RestUrl"]
            layer.update(data)
            if layer["name"] not in layers:
                context.log.info("%(portal_title)s: skip %(name)s", layer)
                continue
            # context.log.info('%(portal_title)s [%(id)s]: %(name)s', layer)
            context.emit(data=layer)
