import geopandas as gpd
from shapely import ops
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon


def extract_polygons_2d(geometry):
    if isinstance(geometry, GeometryCollection):
        polygons = [geom for geom in geometry.geoms if isinstance(geom, Polygon)]
        if polygons:
            # return the first polygon if there is only one, otherwise return a MultiPolygon
            return (
                ops.transform(lambda x, y, z=None: (x, y), polygons[0])
                if len(polygons) == 1
                else ops.transform(lambda x, y, z=None: (x, y), MultiPolygon(polygons))
            )
    elif isinstance(geometry, Polygon):
        return ops.transform(lambda x, y, z=None: (x, y), geometry)
    return None


def get_tiles_polygons(geojson):
    geojson_grande = gpd.read_file(
        "./S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00.kml"
    )
    if geojson_grande.crs != geojson.crs:
        geojson = geojson.to_crs(geojson_grande.crs)

    geojson_grande["geometry"] = geojson_grande["geometry"].apply(extract_polygons_2d)

    interseccion = gpd.overlay(geojson_grande, geojson, how="intersection")

    tiles_zones_list = set(list(interseccion["Name"]))

    return tiles_zones_list
