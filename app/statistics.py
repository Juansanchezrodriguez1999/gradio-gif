import os

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
from shapely.geometry import shape


def calculate_statistics_in_polygon(gdf_parcela, image_paths, polygon_id, indice):
    """
    Calculates mean and standard deviation of values within a polygon in multiple rasters and exports to CSV.

    Args:
        gdf_parcela (GeoDataFrame or dict): GeoDataFrame with the geometry, or a dictionary representing the parcel geometry.
        image_paths (list of str): List of paths to raster files.
        polygon_id (str): Identifier for the polygon being processed.

    Returns:
        dict: Dictionary with mean and standard deviation for each raster for each polygon.
        pd.DataFrame: DataFrame with statistics for each raster to save as CSV.
    """
    try:
        # Verificar si la geometría es un diccionario y convertirla a GeoDataFrame si es necesario
        if isinstance(gdf_parcela, dict):
            if "coordinates" not in gdf_parcela:
                raise ValueError(
                    "Invalid parcel geometry dictionary: 'coordinates' key missing."
                )

            parcela_geometry = shape(gdf_parcela)
            parcela_crs = gdf_parcela.get("CRS", "EPSG:4326")
            gdf_parcela = gpd.GeoDataFrame(geometry=[parcela_geometry], crs=parcela_crs)

        valid_files = [f for f in image_paths if f.endswith(".tif") and (indice in f)]
        if not valid_files:
            raise FileNotFoundError("No files found with the .tif format.")

        stats = {polygon_id: {}}
        data_for_csv = []

        for image_path in valid_files:
            year_list = list(os.path.basename(image_path).split("_")[1])
            month = (os.path.basename(image_path).split("_")[2]).split(".")[0]
            original_filename = f"{month}{year_list[2]}{year_list[3]}"
            with rasterio.open(image_path) as src:
                gdf_parcela = gdf_parcela.to_crs(src.crs)
                if gdf_parcela.is_empty.any():
                    print(f"Parcel geometry is empty for image {image_path}.")
                    continue
                geometries = [gdf_parcela.geometry.iloc[0]]
                out_image, out_transform = mask(src, geometries, crop=False)

                # Calcular estadísticas solo si hay datos válidos
                if np.isnan(out_image).all():
                    print(
                        f"No data found in masked area for image {image_path}. Skipping."
                    )
                    continue

                mean_val = np.nanmean(out_image)
                median_val = np.nanmedian(out_image)
                std_dev_val = np.nanstd(out_image)

                # Guardar datos en el diccionario de estadísticas por imagen
                stats[polygon_id][f"{original_filename}_mean"] = mean_val
                stats[polygon_id][f"{original_filename}_medi"] = median_val
                stats[polygon_id][f"{original_filename}_std"] = std_dev_val

                # Añadir datos al DataFrame de CSV
                data_for_csv.append(
                    {
                        "polygon_id": polygon_id,
                        "image_name": original_filename,
                        "mean": mean_val,
                        "median": median_val,
                        "std_dev": std_dev_val,
                    }
                )

        # Convertir a DataFrame para crear el CSV
        csv_df = pd.DataFrame(data_for_csv)
        return stats

    except FileNotFoundError as e:
        print(str(e))
        raise
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise
