import glob
import json
import os
import re
import shutil
import tempfile
import zipfile
from datetime import datetime
from typing import List, Tuple
import imageio

import folium
import geopandas as gpd
import gradio as gr
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from gradio_calendar import Calendar
from shapely.geometry import shape
from sigpac_tools.find import find_from_cadastral_registry

from app.cut_from_geometry import cut_from_geometry
from app.download_merge import download_tif_files
from app.download_merge_rgb import workflow_generar_gif, descargar_archivos_tif,rgb,crear_gif,merge_tifs_por_fecha_banda
from app.generate_map import generate_map_from_geojson
from app.generate_map import crear_gif_no_rgb
from app.generate_map import merge_tifs_por_fecha

from app.get_tiles import get_tiles_polygons
from app.plots import all_statistics, plot_statistics, temporal_means
from app.sigpac_to_geometry import sigpac_to_geometry
from app.statistics_shapefile import calculate_statistics_in_polygon

lat = 37.5443
lon = -4.7278
zoom_start = 8

initial_map = folium.Map(
    location=[lat, lon], zoom_start=zoom_start, tiles="OpenStreetMap"
)


def process_catastral_data(    catastral_registry: int, images: List[str]) -> Tuple[str, str]:
    """
    Processes images by cutting them according to SIGPAC geometry and returns a ZIP file with cropped images and geometry in GeoJSON format.

    Args:
        catastral_registry (int): Cadastral registry number.
        format (str): Output image format (e.g., 'tif', 'jp2').
        images (List[str]): List of image file paths to process.

    Returns:
        Tuple[str, str]: Paths to the ZIP file containing cropped images and the GeoJSON file with geometry.
    """
    try:
        unique_formats = list(
            set(
                f.split(".")[-1].lower()
                for f in images
                if isinstance(f, str) and "." in f
            )
        )
        if len(unique_formats) > 1:
            raise ValueError(
                f"Unsupported format. You must upload images in one unique format."
            )
        try:
            geometry, metadata = find_from_cadastral_registry(catastral_registry)
        except Exception as e:
            gr.Warning("Referencia catastral no válida")
            gr.Warning(str(e))
            return None,None

        geojson_data = {
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "geometry": geometry, "properties": metadata}
            ],
        }
        total_html_files = []
        total_geojson_files = []
        combined_df = pd.DataFrame()
        temporal_df = pd.DataFrame()
        geojson_path = "geometry.json"
        with open(geojson_path, "w") as geojson_file:
            json.dump(geojson_data, geojson_file)
        polygon_id = str(catastral_registry)
        images_dir = cut_from_geometry(geometry, unique_formats[0], images, polygon_id)
        indices = list(
            {file.split("/")[-1].split("_")[0].strip() for file in images_dir}
        )

        for indice in indices:
            stats_index = {}
            stats = []
            for feature in geojson_data["features"]:
                geometry = feature["geometry"]
                polygon_id = str(catastral_registry)
                feature["objectID"] = polygon_id
                stats.append(
                    calculate_statistics_in_polygon(
                        geometry, images_dir, polygon_id, indice
                    )
                )

            stats_index[indice] = stats

            for key, polygons in stats_index.items():
                records = []
                for polygon in polygons:
                    for polygon_id, metrics in polygon.items():
                        record = {"polygon_id": polygon_id, "indice": key}
                        record.update(metrics)
                        records.append(record)

                df = pd.DataFrame(records)
                df_result, csv_path, monthly_means = all_statistics(df, indice)
                combined_df = pd.concat([combined_df, df_result], ignore_index=True)

            temporal_df = temporal_means(combined_df[combined_df["indice"] == indice])
            temporal_df["indice"] = indice

            temporal_dict = (
                temporal_df.groupby("polygon_id")
                .apply(
                    lambda x: {
                        f"{row['mes']}/{row['años']}".replace(" ", ""): {
                            "median": row["media_mediana"],
                            "mean": row["media_media"],
                            "std": row["media_desviacion"],
                        }
                        for _, row in x.iterrows()
                    }
                )
                .to_dict()
            )

            convinced_dict = (
                combined_df[combined_df["indice"] == indice]
                .groupby("polygon_id")
                .apply(
                    lambda x: {
                        f"{row['mes']}-{row['anio']}".replace(" ", ""): {
                            "median": row["mediana"],
                            "mean": row["media"],
                            "std": row["desviacion"],
                        }
                        for _, row in x.iterrows()
                    }
                )
                .to_dict()
            )

            for feature in geojson_data["features"]:
                if feature["objectID"] in temporal_dict:
                    feature["temporalStatistics"] = temporal_dict[feature["objectID"]]
                if feature["objectID"] in convinced_dict:
                    feature["zonalStatistics"] = convinced_dict[feature["objectID"]]

            updated_file_name = f"Geojson_{indice}.geojson"
            with open(updated_file_name, "w") as file:
                json.dump(geojson_data, file, indent=4)
            total_geojson_files.append(updated_file_name)

        unique_polygons = combined_df["polygon_id"].unique()
        for polygon in unique_polygons:
            df_polygon = combined_df[combined_df["polygon_id"] == polygon].drop(
                columns=["polygon_id"]
            )
            total_html_files.extend(
                plot_statistics(df_polygon, ["zonal", "temporal"], polygon)
            )

        zip_output_geojson = os.path.join(tempfile.mkdtemp(), "Geojson.zip")
        with zipfile.ZipFile(zip_output_geojson, "w") as zipf:
            for geojson_file in total_geojson_files:
                zipf.write(geojson_file, os.path.basename(geojson_file))

        zip_output_plots = os.path.join(tempfile.mkdtemp(), "Plots.zip")
        with zipfile.ZipFile(zip_output_plots, "w") as zipf:
            for html_file in total_html_files:
                zipf.write(html_file, os.path.basename(html_file))
        main_map = generate_map_from_geojson(geojson_data, images_dir)

        return zip_output_plots, zip_output_geojson, main_map._repr_html_()
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {str(e)}")
    except Exception as e:
        raise Exception(f"An error occurred: {str(e)}")


def process_catastral_data_sentinel(
    catastral_registry: int, indexes: list, date_start: str, date_end: str) -> str:
    """
    Processes images by cutting them according to SIGPAC geometry and returns a ZIP file with cropped images and geometry in GeoJSON format.

    Args:
        catastral_registry (int): Cadastral registry number.
        format (str): Output image format (e.g., 'tif', 'jp2').
        images (List[str]): List of image file paths to process.

    Returns:
        Tuple[str, str]: Paths to the ZIP file containing cropped images and the GeoJSON file with geometry.
    """
    year_start = date_start.strftime("%Y")
    month_start = date_start.strftime("%B")
    year_end = date_end.strftime("%Y")
    month_end = date_end.strftime("%B")
    years = [year_start, year_end]
    months = [month_start, month_end]
    indexes = [indexes.upper()]
    total_html_files = []
    total_geojson_files = []
    combined_df = pd.DataFrame()
    temporal_df = pd.DataFrame()

    indexes = [x.upper() for x in indexes]
    try:
        geometry, metadata = find_from_cadastral_registry(catastral_registry)
    except Exception as e:
        gr.Warning("Referencia catastral no válida")
        gr.Warning(str(e))
        return None,None

    geojson_data = {
        "type": "FeatureCollection",
        "features": [{"type": "Feature", "geometry": geometry, "properties": metadata}],
    }
    features = geojson_data["features"]
    geometries = [shape(feature["geometry"]) for feature in features]
    properties = [feature["properties"] for feature in features]

    gdf = gpd.GeoDataFrame(properties, geometry=geometries)
    gdf = gdf.set_crs(geojson_data["features"][0]["geometry"].get("CRS", ""))

    zones_utm = get_tiles_polygons(gdf)
    list_zones_utm = list(zones_utm)
    if(indexes==["RGB"]):
        images_dir = descargar_archivos_tif(list_zones_utm, years, months)
    else:
        images_dir = download_tif_files(list_zones_utm, years, indexes, months)

    if not images_dir:
        gr.Warning("No hay imagenes disponibles para la fecha seleccionada, las imágenes son procesadas a final de cada mes.")
        gr.Warning("No images are available for the selected date, images are processed at the end of each month.")
        return None, None

    unique_formats = list(
            set(
                f.split(".")[-1].lower()
                for f in images_dir
                if isinstance(f, str) and "." in f
            )
        )
    if len(unique_formats) > 1:
        raise ValueError(
            f"Unsupported format. You must upload images in one unique format."
        )
    cropped_images = []
    for feature in geojson_data["features"]:
        geometry = feature["geometry"]
        geometry_id = catastral_registry
        cropped_images.extend(cut_from_geometry(geometry, unique_formats[0], images_dir, geometry_id))
    
    if(indexes==["RGB"]):
        rgb_folder, rutas_png, images_dir_rgb, output_gif = rgb(cropped_images)
    else:
        output_gif = crear_gif_no_rgb(cropped_images)
    
    main_map = generate_map_from_geojson(geojson_data, cropped_images, output_gif, indexes)

    return output_gif,main_map._repr_html_()


def process_geojson_data(geojson: str, images: List[str]) -> str:
    """
    Processes images by cutting them according to the provided GeoJSON geometry and returns a ZIP file with cropped images.

    Args:
        geojson (str): Path to the GeoJSON file.
        format (str): Output image format (e.g., 'tif', 'jp2').
        images (List[str]): List of image file paths to process.

    Returns:
        str: Path to the ZIP file containing the cropped images.
    """
    indices = set()

    total_html_files = []
    total_geojson_files = []
    combined_df = pd.DataFrame()
    temporal_df = pd.DataFrame()
    unique_formats = list(
        set(f.split(".")[-1].lower() for f in images if isinstance(f, str) and "." in f)
    )
    if len(unique_formats) > 1:
        raise ValueError(
            f"Unsupported format. You must upload images in one unique format."
        )
    try:
        with open(geojson, "r") as file:
            geojson_data = json.load(file)

        for image in images:
            match = re.match(r"(\w+)_\d{4}_\d{2}\.tif", os.path.basename(image))
            if match:
                indices.add(match.group(1))
        cropped_images=[]
        for indice in indices:
            stats_index = {}
            stats = []
            for feature in geojson_data["features"]:
                geometry = feature["geometry"]
                polygon_id = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
                feature["objectID"] = polygon_id
                images_dir = cut_from_geometry(geometry, unique_formats[0], images, polygon_id)
                cropped_images.extend(images_dir)
                stats.append(
                    calculate_statistics_in_polygon(
                        geometry, images_dir, polygon_id, indice
                    )
                )

            stats_index[indice] = stats

            for key, polygons in stats_index.items():
                records = []
                for polygon in polygons:
                    for polygon_id, metrics in polygon.items():
                        record = {"polygon_id": polygon_id, "indice": key}
                        record.update(metrics)
                        records.append(record)

                df = pd.DataFrame(records)
                df_result, csv_path, monthly_means = all_statistics(df, indice)
                combined_df = pd.concat([combined_df, df_result], ignore_index=True)

            temporal_df = temporal_means(combined_df[combined_df["indice"] == indice])
            temporal_df["indice"] = indice
            temporal_dict = (
                temporal_df.groupby("polygon_id")
                .apply(
                    lambda x: {
                        f"{row['mes']}/{row['años']}".replace(" ", ""): {
                            "median": row["media_mediana"],
                            "mean": row["media_media"],
                            "std": row["media_desviacion"],
                        }
                        for _, row in x.iterrows()
                    }
                )
                .to_dict()
            )

            convinced_dict = (
                combined_df[combined_df["indice"] == indice]
                .groupby("polygon_id")
                .apply(
                    lambda x: {
                        f"{row['mes']}-{row['anio']}".replace(" ", ""): {
                            "median": row["mediana"],
                            "mean": row["media"],
                            "std": row["desviacion"],
                        }
                        for _, row in x.iterrows()
                    }
                )
                .to_dict()
            )

            for feature in geojson_data["features"]:
                if feature["objectID"] in temporal_dict:
                    feature["temporalStatistics"] = temporal_dict[feature["objectID"]]
                if feature["objectID"] in convinced_dict:
                    feature["zonalStatistics"] = convinced_dict[feature["objectID"]]

            updated_file_name = f"Geojson_{indice}.geojson"
            with open(updated_file_name, "w") as file:
                json.dump(geojson_data, file, indent=4)
            total_geojson_files.append(updated_file_name)

        unique_polygons = combined_df["polygon_id"].unique()
        for polygon in unique_polygons:
            df_polygon = combined_df[combined_df["polygon_id"] == polygon].drop(
                columns=["polygon_id"]
            )
            total_html_files.extend(
                plot_statistics(df_polygon, ["zonal", "temporal"], polygon)
            )

        zip_output_geojson = os.path.join(tempfile.mkdtemp(), "Geojson.zip")
        with zipfile.ZipFile(zip_output_geojson, "w") as zipf:
            for geojson_file in total_geojson_files:
                zipf.write(geojson_file, os.path.basename(geojson_file))

        zip_output_plots = os.path.join(tempfile.mkdtemp(), "Plots.zip")
        with zipfile.ZipFile(zip_output_plots, "w") as zipf:
            for html_file in total_html_files:
                zipf.write(html_file, os.path.basename(html_file))
        main_map = generate_map_from_geojson(geojson_data, cropped_images)


        return zip_output_plots, zip_output_geojson, main_map._repr_html_()
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {str(e)}")
    except Exception as e:
        raise Exception(f"An error occurred: {str(e)}")


def process_geojson_data_sentinel(
    geojson: dict, indexes: list, date_start: str, date_end: str) -> str:
    """
    Processes images based on GeoJSON and returns a ZIP file with cropped images.

    Args:
        geojson (dict): GeoJSON data.
        years (list): List of years for data.
        indexes (list): List of indexes to apply.
        months (list): List of months for data.

    Returns:
        str: Path to the ZIP file with cropped images.
    """
    
    # Procesamiento de las fechas
    year_start = date_start.strftime("%Y")
    month_start = date_start.strftime("%B")
    year_end = date_end.strftime("%Y")
    month_end = date_end.strftime("%B")
    years = [year_start, year_end]
    months = [month_start, month_end]
    indexes = [indexes.upper()]
    

    total_html_files = []
    total_geojson_files = []
    combined_df = pd.DataFrame()
    temporal_df = pd.DataFrame()
    indexes = [x.upper() for x in indexes]

    gdf = gpd.read_file(geojson)
    json_path = os.path.join(tempfile.gettempdir(), "temp_shapefile.json")
    gdf.to_file(json_path, driver="GeoJSON")
    

    with open(json_path) as f:
        geojson_data = json.load(f)

    zones_utm = get_tiles_polygons(gdf)
    list_zones_utm = list(zones_utm)
    if(indexes==["RGB"]):
        images_dir = descargar_archivos_tif(list_zones_utm, years, months)
    else:
        images_dir = download_tif_files(list_zones_utm, years, indexes, months)

    if not images_dir:
        gr.Warning("No hay imagenes disponibles para la fecha seleccionada, las imágenes son procesadas a final de cada mes.")
        gr.Warning("No images are available for the selected date, images are processed at the end of each month.")
        return None, None

    unique_formats = list(
            set(
                f.split(".")[-1].lower()
                for f in images_dir
                if isinstance(f, str) and "." in f
            )
        )
    if len(unique_formats) > 1:
        raise ValueError(
            f"Unsupported format. You must upload images in one unique format."
        )
    cropped_images = []
    for feature in geojson_data["features"]:
        geometry = feature["geometry"]
        polygon_id = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
        feature["objectID"] = polygon_id
        geometry_id = feature["objectID"]
        cropped_images.extend(cut_from_geometry(geometry, unique_formats[0], images_dir, geometry_id))
    
    if(indexes==["RGB"]):
        cropped_images_merge=merge_tifs_por_fecha_banda(cropped_images)
        rgb_folder, rutas_png, images_dir_rgb, output_gif = rgb(cropped_images_merge)
    else:
        cropped_images_merge=merge_tifs_por_fecha(cropped_images)
        output_gif = crear_gif_no_rgb(cropped_images_merge)
    
    main_map = generate_map_from_geojson(geojson_data, cropped_images_merge, output_gif, indexes)

    return output_gif,main_map._repr_html_()


def add_stats_to_dbf(    dbf_path: str, stats: list, indice: str, output_dir: str) -> Tuple[str, str]:
    indice_dir = os.path.join(output_dir, indice)
    os.makedirs(indice_dir, exist_ok=True)

    gdf = gpd.read_file(dbf_path)
    for stat_dict in stats:
        for obj_id, stat_values in stat_dict.items():
            row_index = gdf[gdf["objectid"] == obj_id].index
            if not row_index.empty:
                for stat_name, value in stat_values.items():
                    if stat_name not in gdf.columns:
                        gdf[stat_name] = None
                    gdf.at[row_index[0], stat_name] = value

    updated_dbf_path = os.path.join(
        indice_dir, os.path.basename(dbf_path).replace(".dbf", f"_{indice}.dbf")
    )
    gdf.to_file(updated_dbf_path, driver="ESRI Shapefile")

    csv_path = os.path.join(
        indice_dir, os.path.basename(dbf_path).replace(".dbf", f"_{indice}.csv")
    )
    gdf.to_csv(csv_path, index=False)
    return updated_dbf_path, csv_path


def process_shp_data(shp: str, images: List[str]) -> str:
    """
    Processes images by cutting them according to the provided shapefile geometry and returns a ZIP file with cropped images.

    Args:
        shp (str): Path to the shapefile ZIP.
        format (str): Output image format (e.g., 'tif', 'jp2').
        images (List[str]): List of image file paths to process.

    Returns:
        str: Path to the ZIP file containing the cropped images.
    """
    unique_formats = list(
        set(f.split(".")[-1].lower() for f in images if isinstance(f, str) and "." in f)
    )
    if len(unique_formats) > 1:
        raise ValueError(
            f"Unsupported format. You must upload images in one unique format."
        )
    extract_path = "./temp_extracted_files"
    output_zip_path = os.path.join(tempfile.gettempdir(), "Shapefile.zip")
    updated_shapefiles_path = tempfile.mkdtemp()
    total_html_files = []
    total_geojson_files = []
    combined_df = pd.DataFrame()
    temporal_df = pd.DataFrame()
    json_path = "./temp_shapefile.json"
    indices = set()
    for image in images:
        match = re.match(r"(\w+)_\d{4}_\d{2}\.tif", os.path.basename(image))
        if match:
            indices.add(match.group(1))

    if not os.path.exists(extract_path):
        os.makedirs(extract_path)
    cropped_images=[]
    try:
        with zipfile.ZipFile(shp, "r") as zip_ref:
            zip_ref.extractall(extract_path)

        shp_file = dbf_file = None
        for file in os.listdir(extract_path):
            if file.endswith(".shp"):
                shp_file = os.path.join(extract_path, file)
            elif file.endswith(".dbf"):
                dbf_file = os.path.join(extract_path, file)
        if not shp_file or not dbf_file:
            raise FileNotFoundError("No .shp or .dbf file found in ZIP.")

        if shp_file:
            gdf = gpd.read_file(shp_file)
            first_column_name = gdf.columns[0]
            gdf.to_file(json_path, driver="GeoJSON")

            with open(json_path) as f:
                geojson_data = json.load(f)

            for indice in indices:
                stats = []
                for feature in geojson_data["features"]:
                    geometry = feature["geometry"]
                    polygon_id = feature["properties"][first_column_name]
                    images_dir = cut_from_geometry(geometry, unique_formats[0], images, polygon_id)
                    cropped_images.extend(images_dir)
                    stats.append(
                        calculate_statistics_in_polygon(
                            geometry, images_dir, polygon_id, indice
                        )
                    )
                indice_dir = os.path.join(extract_path, indice)
                os.makedirs(indice_dir, exist_ok=True)
                updated_dbf_path, csv_path = add_stats_to_dbf(
                    dbf_file, stats, indice, extract_path
                )

            for index_folder in os.listdir(extract_path):
                folder_path = os.path.join(extract_path, index_folder)
                updated_folder_path = os.path.join(
                    updated_shapefiles_path, index_folder
                )
                os.makedirs(updated_folder_path, exist_ok=True)

                if os.path.isdir(folder_path):
                    dbf_files = [
                        file
                        for file in os.listdir(folder_path)
                        if file.endswith(".dbf")
                    ]
                    if dbf_files:
                        dbf_path = os.path.join(folder_path, dbf_files[0])
                        dbf_df = gpd.read_file(dbf_path)
                        df_result, csv_path, monthly_means = all_statistics(
                            dbf_df, index_folder
                        )
                        combined_df = pd.concat(
                            [combined_df, df_result], ignore_index=True
                        )
                        temporal_df = temporal_means(combined_df)
                        for _, row in temporal_df.iterrows():
                            mean_col = f"{row['mes']}{row['años'].replace('-', '').replace(' ', '')}mean"
                            median_col = f"{row['mes']}{row['años'].replace('-', '').replace(' ', '')}medi"
                            std_col = f"{row['mes']}{row['años'].replace('-', '').replace(' ', '')}std"

                            if mean_col not in dbf_df.columns:
                                dbf_df[mean_col] = None
                            if std_col not in dbf_df.columns:
                                dbf_df[std_col] = None

                            dbf_df.loc[
                                dbf_df[first_column_name] == row["polygon_id"],
                                median_col,
                            ] = row["media_mediana"]
                            dbf_df.loc[
                                dbf_df[first_column_name] == row["polygon_id"], mean_col
                            ] = row["media_media"]
                            dbf_df.loc[
                                dbf_df[first_column_name] == row["polygon_id"], std_col
                            ] = row["media_desviacion"]

                        dbf_df.to_file(updated_dbf_path, driver="ESRI Shapefile")

                        for file in os.listdir(folder_path):
                            if file.endswith((".shp", ".shx", ".prj", ".cpg")):
                                shutil.copy(
                                    os.path.join(folder_path, file), updated_folder_path
                                )

            unique_polygons = combined_df["polygon_id"].unique()
            for polygon in unique_polygons:
                df_polygon = combined_df[combined_df["polygon_id"] == polygon].drop(
                    columns=["polygon_id"]
                )
                total_html_files.extend(
                    plot_statistics(df_polygon, ["zonal", "temporal"], polygon)
                )
            zip_output_plots = os.path.join(tempfile.mkdtemp(), "Plots.zip")
            with zipfile.ZipFile(zip_output_plots, "w") as zipf:
                for html_file in total_html_files:
                    zipf.write(html_file, os.path.basename(html_file))
            with zipfile.ZipFile(output_zip_path, "w") as zipf:
                for indice in indices:
                    indice_dir = os.path.join(extract_path, indice)
                    for folder_name, _, filenames in os.walk(indice_dir):
                        for filename in filenames:
                            file_path = os.path.join(folder_name, filename)
                            zipf.write(
                                file_path, os.path.relpath(file_path, extract_path)
                            )
        else:
            raise FileNotFoundError("No .shp file found in ZIP.")

        main_map = generate_map_from_geojson(geojson_data, cropped_images)

        shutil.rmtree(extract_path)
        os.remove(json_path)
        return zip_output_plots, output_zip_path, main_map._repr_html_()

    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {str(e)}")
    except Exception as e:
        raise Exception(f"An error occurred: {str(e)}")


def process_shp_data_sentinel(    shp: str, indexes: list, date_start: str, date_end: str) -> str:
    """
    Processes images by cutting them according to the provided shapefile geometry and returns a ZIP file with cropped images.

    Args:
        shp (str): Path to the shapefile ZIP.
        format (str): Output image format (e.g., 'tif', 'jp2').
        images (List[str]): List of image file paths to process.

    Returns:
        str: Path to the ZIP file containing the cropped images.
    """
    year_start = date_start.strftime("%Y")
    month_start = date_start.strftime("%B")
    year_end = date_end.strftime("%Y")
    month_end = date_end.strftime("%B")
    years = [year_start, year_end]
    months = [month_start, month_end]
    indexes = [indexes.upper()]
    extract_path = tempfile.mkdtemp()
    output_zip_path = os.path.join(tempfile.gettempdir(), "Shapefile.zip")
    updated_shapefiles_path = tempfile.mkdtemp()
    total_html_files = []
    total_geojson_files = []
    combined_df = pd.DataFrame()
    temporal_df = pd.DataFrame()
    with zipfile.ZipFile(shp, "r") as zip_ref:
        zip_ref.extractall(extract_path)

    shp_file = dbf_file = None
    for file in os.listdir(extract_path):
        if file.endswith(".shp"):
            shp_file = os.path.join(extract_path, file)
        elif file.endswith(".dbf"):
            dbf_file = os.path.join(extract_path, file)

    if not shp_file or not dbf_file:
        raise FileNotFoundError("No .shp or .dbf file found in ZIP.")

    gdf = gpd.read_file(shp_file)
    indexes = [x.upper() for x in indexes]
    gdf = gpd.read_file(shp_file)
    json_path = os.path.join(tempfile.gettempdir(), "temp_shapefile.json")
    gdf.to_file(json_path, driver="GeoJSON")
    first_column_name = gdf.columns[0]
    with open(json_path) as f:
        geojson_data = json.load(f)
    zones_utm = get_tiles_polygons(gdf)
    list_zones_utm = list(zones_utm)
    if(indexes==["RGB"]):
        images_dir = descargar_archivos_tif(list_zones_utm, years, months)
    else:
        images_dir = download_tif_files(list_zones_utm, years, indexes, months)

    if not images_dir:
        gr.Warning("No hay imagenes disponibles para la fecha seleccionada, las imágenes son procesadas a final de cada mes.")
        gr.Warning("No images are available for the selected date, images are processed at the end of each month.")
        return None, None

    unique_formats = list(
            set(
                f.split(".")[-1].lower()
                for f in images_dir
                if isinstance(f, str) and "." in f
            )
        )
    if len(unique_formats) > 1:
        raise ValueError(
            f"Unsupported format. You must upload images in one unique format."
        )
    cropped_images = []
    for feature in geojson_data["features"]:
        geometry = feature["geometry"]
        geometry_id = feature["properties"][first_column_name]
        cropped_images.extend(cut_from_geometry(geometry, unique_formats[0], images_dir, geometry_id))
    
    if(indexes==["RGB"]):
        cropped_images_merge=merge_tifs_por_fecha_banda(cropped_images)
        rgb_folder, rutas_png, images_dir_rgb, output_gif = rgb(cropped_images_merge)
    else:
        cropped_images_merge=merge_tifs_por_fecha(cropped_images)
        output_gif = crear_gif_no_rgb(cropped_images_merge)
    
    main_map = generate_map_from_geojson(geojson_data, cropped_images_merge, output_gif, indexes)

    return output_gif,main_map._repr_html_()


def process_csv_data(csv: str, images: List[str], latitude_column: str, longitude_column: str) -> str:
    """
    Processes images by cutting them according to the provided shapefile geometry and returns a ZIP file with cropped images.

    Args:
        shp (str): Path to the shapefile ZIP.
        format (str): Output image format (e.g., 'tif', 'jp2').
        images (List[str]): List of image file paths to process.

    Returns:
        str: Path to the ZIP file containing the cropped images.
    """
    unique_formats = list(
        set(f.split(".")[-1].lower() for f in images if isinstance(f, str) and "." in f)
    )
    if len(unique_formats) > 1:
        raise ValueError(
            f"Unsupported format. You must upload images in one unique format."
        )
    df = pd.read_csv(csv)
    if (longitude_column in df.columns) and (latitude_column in df.columns):
        coordinates = df[[longitude_column, latitude_column]].values.tolist()
        if len(coordinates) < 3:
            gr.Warning("El archivo CSV debe contener al menos tres puntos válidos (filas con datos).")
            gr.Warning("The CSV file must contain at least three valid points (rows with data).")
            return None, None

    elif (longitude_column in df.columns) and (latitude_column not in df.columns):
        gr.Warning("El nombre de la columna latitud debe coincidir con el del CSV.")
        gr.Warning("The name of the latitude column must match the name of the CSV column.")
        return None, None, None
    elif (longitude_column not in df.columns) and (latitude_column in df.columns):
        gr.Warning("El nombre de la columna longitud debe coincidir con el del CSV.")
        gr.Warning("The name of the longitude column must match the name of the CSV column.")
        return None, None, None
    else:
        gr.Warning("Los nombres de las columnas latitud y longitud deben coincidir con los del CSV.")
        gr.Warning("The names of the latitude and longitude columns must match those in the CSV.")
        return None, None, None

    coordinates.append(coordinates[0])
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [coordinates]},
                "properties": {},
            }
        ],
    }
    total_html_files = []
    total_geojson_files = []
    combined_df = pd.DataFrame()
    temporal_df = pd.DataFrame()
    geojson_path = "geometry.json"
    with open(geojson_path, "w") as geojson_file:
        json.dump(geojson_data, geojson_file)
    geometry = geojson_data["features"][0]["geometry"]
    polygon_id = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
    images_dir = cut_from_geometry(geometry, unique_formats[0], images, polygon_id)
    indices = list({file.split("/")[-1].split("_")[0].strip() for file in images_dir})
    for indice in indices:
        stats_index = {}
        stats = []

        for feature in geojson_data["features"]:
            geometry = feature["geometry"]
            feature["objectID"] = polygon_id
            stats.append(
                calculate_statistics_in_polygon(
                    geometry, images_dir, polygon_id, indice
                )
            )

        stats_index[indice] = stats

        for key, polygons in stats_index.items():
            records = []
            for polygon in polygons:
                for polygon_id, metrics in polygon.items():
                    record = {"polygon_id": polygon_id, "indice": key}
                    record.update(metrics)
                    records.append(record)

            df = pd.DataFrame(records)
            df_result, csv_path, monthly_means = all_statistics(df, indice)
            combined_df = pd.concat([combined_df, df_result], ignore_index=True)

        temporal_df = temporal_means(combined_df[combined_df["indice"] == indice])
        temporal_df["indice"] = indice
        temporal_dict = (
            temporal_df.groupby("polygon_id")
            .apply(
                lambda x: {
                    f"{row['mes']}/{row['años']}".replace(" ", ""): {
                        "median": row["media_mediana"],
                        "mean": row["media_media"],
                        "std": row["media_desviacion"],
                    }
                    for _, row in x.iterrows()
                }
            )
            .to_dict()
        )

        convinced_dict = (
            combined_df[combined_df["indice"] == indice]
            .groupby("polygon_id")
            .apply(
                lambda x: {
                    f"{row['mes']}-{row['anio']}".replace(" ", ""): {
                        "median": row["mediana"],
                        "mean": row["media"],
                        "std": row["desviacion"],
                    }
                    for _, row in x.iterrows()
                }
            )
            .to_dict()
        )

        for feature in geojson_data["features"]:
            if feature["objectID"] in temporal_dict:
                feature["temporalStatistics"] = temporal_dict[feature["objectID"]]
            if feature["objectID"] in convinced_dict:
                feature["zonalStatistics"] = convinced_dict[feature["objectID"]]

        updated_file_name = f"Geojson_{indice}.geojson"
        with open(updated_file_name, "w") as file:
            json.dump(geojson_data, file, indent=4)
        total_geojson_files.append(updated_file_name)

    unique_polygons = combined_df["polygon_id"].unique()
    for polygon in unique_polygons:
        df_polygon = combined_df[combined_df["polygon_id"] == polygon].drop(
            columns=["polygon_id"]
        )
        total_html_files.extend(
            plot_statistics(df_polygon, ["zonal", "temporal"], polygon)
        )

    zip_output_geojson = os.path.join(tempfile.mkdtemp(), "Geojson.zip")
    with zipfile.ZipFile(zip_output_geojson, "w") as zipf:
        for geojson_file in total_geojson_files:
            zipf.write(geojson_file, os.path.basename(geojson_file))

    zip_output_plots = os.path.join(tempfile.mkdtemp(), "Plots.zip")
    with zipfile.ZipFile(zip_output_plots, "w") as zipf:
        for html_file in total_html_files:
            zipf.write(html_file, os.path.basename(html_file))
    main_map = generate_map_from_geojson(geojson_data, images_dir)

    return zip_output_plots, zip_output_geojson, main_map._repr_html_()


def process_csv_data_sentinel(
    csv: str,
    indexes: list,
    date_start: str,
    date_end: str,
    latitude_column: str,
    longitude_column: str,) -> str:
    """
    Processes images by cutting them according to the provided shapefile geometry and returns a ZIP file with cropped images.

    Args:
        shp (str): Path to the shapefile ZIP.
        format (str): Output image format (e.g., 'tif', 'jp2').
        images (List[str]): List of image file paths to process.

    Returns:
        str: Path to the ZIP file containing the cropped images.
    """
    year_start = date_start.strftime("%Y")
    month_start = date_start.strftime("%B")
    year_end = date_end.strftime("%Y")
    month_end = date_end.strftime("%B")
    years = [year_start, year_end]
    months = [month_start, month_end]
    indexes = [indexes.upper()]
    indexes = [x.upper() for x in indexes]
    total_html_files = []
    total_geojson_files = []
    combined_df = pd.DataFrame()
    temporal_df = pd.DataFrame()

    df = pd.read_csv(csv)

    if (longitude_column in df.columns) and (latitude_column in df.columns):
        coordinates = df[[longitude_column, latitude_column]].values.tolist()
        if len(coordinates) < 3:
            gr.Warning("El archivo CSV debe contener al menos tres puntos válidos (filas con datos).")
            gr.Warning("The CSV file must contain at least three valid points (rows with data).")
            return None, None

    elif (longitude_column in df.columns) and (latitude_column not in df.columns):
        gr.Warning("El nombre de la columna latitud debe coincidir con el del CSV.")
        gr.Warning("The name of the latitude column must match the name of the CSV column.")
        return None, None, None
    elif (longitude_column not in df.columns) and (latitude_column in df.columns):
        gr.Warning("El nombre de la columna longitud debe coincidir con el del CSV.")
        gr.Warning("The name of the longitude column must match the name of the CSV column.")
        return None, None, None
    else:
        gr.Warning("Los nombres de las columnas latitud y longitud deben coincidir con los del CSV.")
        gr.Warning("The names of the latitude and longitude columns must match those in the CSV.")
        return None, None, None

    coordinates.append(coordinates[0])
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [coordinates]},
                "properties": {},
            }
        ],
    }

    features = geojson_data["features"]
    geometries = [shape(feature["geometry"]) for feature in features]
    properties = [feature["properties"] for feature in features]

    gdf = gpd.GeoDataFrame(properties, geometry=geometries)
    gdf.set_crs("EPSG:4326", allow_override=True, inplace=True)

    zones_utm = get_tiles_polygons(gdf)
    list_zones_utm = list(zones_utm)
    if(indexes==["RGB"]):
        images_dir = descargar_archivos_tif(list_zones_utm, years, months)
    else:
        images_dir = download_tif_files(list_zones_utm, years, indexes, months)

    if not images_dir:
        gr.Warning("No hay imagenes disponibles para la fecha seleccionada, las imágenes son procesadas a final de cada mes.")
        gr.Warning("No images are available for the selected date, images are processed at the end of each month.")
        return None, None

    unique_formats = list(
            set(
                f.split(".")[-1].lower()
                for f in images_dir
                if isinstance(f, str) and "." in f
            )
        )
    if len(unique_formats) > 1:
        raise ValueError(
            f"Unsupported format. You must upload images in one unique format."
        )
    cropped_images = []
    for feature in geojson_data["features"]:
        geometry = feature["geometry"]
        geometry_id = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
        cropped_images.extend(cut_from_geometry(geometry, unique_formats[0], images_dir, geometry_id))
    
    if(indexes==["RGB"]):
        rgb_folder, rutas_png, images_dir_rgb, output_gif = rgb(cropped_images)
    else:
        output_gif = crear_gif_no_rgb(cropped_images)
    
    main_map = generate_map_from_geojson(geojson_data, cropped_images, output_gif, indexes)

    return output_gif,main_map._repr_html_()

def cambiar_idioma(lang):
    if lang=="Español":
        return "es"
    elif lang == "English":
        return "en"
    

def build_interface() -> gr.Blocks:
    """
    Builds the Gradio interface with separate tabs for SIGPAC and GeoJSON image processing.

    Returns:
        gr.Blocks: The complete Gradio interface with tabs.
    """
    df = pd.read_csv("multilenguaje.csv",index_col="indice")
    with gr.Blocks(theme="soft", title="Creador de GIFs") as interface:
        language_request= gr.State()     
        language= gr.State()
        
        @interface.load(outputs=[language_request,language])
        def cargar_idioma(request:gr.Request):
            return "Español","es"

        @gr.render(inputs=language_request)
        def selector_idioma(lang):
            lang_selector = gr.Radio(choices=["Español", "English"],label="Selecciona el idioma / Select the language: ",value=lang)
            
            lang_selector.change(cambiar_idioma,inputs=lang_selector,outputs=language)
            
        @gr.render(inputs=language)
        def show_app(idioma):
            gr.Markdown(f"""
                         {df.loc['desc_general_title', idioma]}
                         {df.loc['desc_general', idioma]}
                         {df.loc['desc_general_fuente', idioma]}
                        """
            )
            with gr.Tab(label=df.loc['archivo', idioma]):
                with gr.Row():
                    with gr.Column():
                        gr.Markdown(f"""
                        {df.loc['detalles_inputs', idioma]}
                        - {df.loc['indice_desc', idioma]}
                        - {df.loc['tipo_geometria_desc', idioma]}
                        - {df.loc['archivo_geometria_desc', idioma]}
                            - {df.loc['si_csv_desc', idioma]}:
                                {df.loc['csv_requisitos', idioma]}
                                ```csv
                                X,Y
                                37.66314226,-5.302012025
                                37.66634019,-5.22130994
                                37.72700059,-5.246265025
                                37.72265957,-5.334305567
                                ```  
                        - {df.loc['columna_latitud_desc', idioma]}
                        - {df.loc['columna_longitud_desc', idioma]}
                        - {df.loc['registro_catastral_desc', idioma]}
                        - {df.loc['fecha_desc', idioma]}
                        {df.loc['detalles_outputs', idioma]}
                        - {df.loc['gif_output', idioma]}
                        - {df.loc['mapa_desc', idioma]}
                        """)
                    
                    with gr.Column():
                        indexes = gr.Radio(
                            label=df.loc['seleccionar_indice', idioma],
                            choices=[
                                "RGB","moisture", "ndvi", "ndwi", "ndsi", "evi", "osavi", "evi2", "ndre", "ndyi", "mndwi", "bri", "ri", "bsi", "cril"
                            ],
                            value="RGB"
                        )
                        input_file_type = gr.Radio(
                            label=df.loc['seleccionar_geometria', idioma],
                            choices=["Shapefile", "CSV", "Geojson", "Catastral"]
                        )
                        geometry_file = gr.File(
                            label=df.loc['subir_archivo', idioma], visible=True
                        )
                        latitude_column = gr.Text(
                            label=df.loc['latitud', idioma], value="X", visible=False
                        )
                        longitude_column = gr.Text(
                            label=df.loc['longitud', idioma], value="Y", visible=False
                        )
                        catastral_registry = gr.Text(
                            label=df.loc['registro_catastral', idioma], value="XXX0000000000X", visible=False
                        )
                        selected_date_start = gr.DateTime(
                            type="datetime", value="2020-01-01 00:00:00", label=df.loc['seleccionar_fecha_ini', idioma]
                        )

                        selected_date_end = gr.DateTime(
                            type="datetime", value="2020-02-02 00:00:00", label=df.loc['seleccionar_fecha_fin', idioma]
                        )

                with gr.Row():
                    submit_button = gr.Button(value=df.loc['procesar', idioma])
                    clear_button = gr.Button(value=df.loc['limpiar', idioma])
                with gr.Row():
                    output_gif = gr.File(label=df.loc['descargar_gif', idioma])
                map_view = gr.HTML(
                    label=df.loc['mapa', idioma],
                    elem_id="output-map",
                    value=initial_map._repr_html_(),
                    visible=True,
                )

                def clear_inputs():
                    """
                    Resetea todos los campos del formulario a sus valores iniciales.
                    """
                    return (
                        gr.update(value=None),
                        gr.update(value=None),
                        gr.update(value=None),
                        gr.update(value=""),
                        gr.update(value=""),
                        gr.update(value=""),
                        gr.update(value=None),
                        gr.update(value=None),
                        gr.update(value=None),
                    )

                clear_button.click(
                    fn=clear_inputs,
                    inputs=[],
                    outputs=[
                        indexes,
                        input_file_type,
                        geometry_file,
                        latitude_column,
                        longitude_column,
                        catastral_registry,
                        selected_date_start,
                        selected_date_end,
                    ],
                )

                def update_visibility(file_type):
                    if file_type == "Catastral":
                        return (
                            gr.update(visible=False),
                            gr.update(visible=True),
                            gr.update(visible=False),
                            gr.update(visible=False),
                        )
                    elif file_type == "CSV":
                        return (
                            gr.update(visible=True),
                            gr.update(visible=False),
                            gr.update(visible=True),
                            gr.update(visible=True),
                        )
                    else:
                        return (
                            gr.update(visible=True),
                            gr.update(visible=False),
                            gr.update(visible=False),
                            gr.update(visible=False),
                        )

                def process_inputs(
                    input_file_type,
                    geometry_file,
                    catastral_registry,
                    indexes,
                    selected_date_start,
                    selected_date_end,
                    latitude_column,
                    longitude_column,
                ):
                    if not input_file_type:
                        gr.Warning(df.loc['seleccionar_geometria_war', idioma])
                        return None, None, None
                    
                    if input_file_type in ["Shapefile", "CSV", "Geojson"] and not geometry_file:
                        gr.Warning(df.loc['archivo_geometria_war', idioma])
                        return None, None, None
                    
                    if input_file_type in ["CSV"] and ((not latitude_column or not longitude_column) or (latitude_column=="" or longitude_column =="")) :
                        gr.Warning(df.loc['nombres_latlon_war', idioma])
                        return None, None, None
                    
                    if input_file_type == "Catastral" and (not catastral_registry or catastral_registry=="XXX0000000000X" or catastral_registry==""):
                        gr.Warning(df.loc['insertar_catastro_war', idioma])
                        return None, None, None

                    if not indexes:
                        gr.Warning(df.loc['seleccionar_indice_war', idioma])
                        return None, None, None

                    if not selected_date_start or not selected_date_end:
                        gr.Warning(df.loc['seleccionar_fechas_war', idioma])
                        return None, None, None

                    if input_file_type == "Catastral":
                        return process_catastral_data_sentinel(
                            catastral_registry,
                            indexes,
                            selected_date_start,
                            selected_date_end,
                        )
                    elif input_file_type == "Shapefile":
                        return process_shp_data_sentinel(
                            geometry_file, indexes, selected_date_start, selected_date_end
                        )
                    elif input_file_type == "CSV":
                        return process_csv_data_sentinel(
                            geometry_file,
                            indexes,
                            selected_date_start,
                            selected_date_end,
                            latitude_column,
                            longitude_column,
                        )
                    else:
                        return process_geojson_data_sentinel(
                            geometry_file, indexes, selected_date_start, selected_date_end
                        )

                input_file_type.change(
                    fn=update_visibility,
                    inputs=[input_file_type],
                    outputs=[
                        geometry_file,
                        catastral_registry,
                        latitude_column,
                        longitude_column,
                    ],
                )

                submit_button.click(
                    fn=process_inputs,
                    inputs=[
                        input_file_type,
                        geometry_file,
                        catastral_registry,
                        indexes,
                        selected_date_start,
                        selected_date_end,
                        latitude_column,
                        longitude_column,
                    ],
                    outputs=[output_gif,map_view],
                )

        return interface


io = build_interface()
