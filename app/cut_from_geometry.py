import os
import tempfile
from datetime import datetime

import geopandas as gpd
import rasterio
from rasterio.mask import mask
from shapely.geometry import shape


def save_raster(image, temp_file, src, transform, format):
    """Saves a raster image to a temporary file.

    Args:
        image (numpy.ndarray): The raster image array to save.
        temp_file (str): Path to the temporary file where the image will be saved.
        src (rasterio.io.DatasetReader): The source raster data to extract metadata.
        transform (Affine): The affine transform to apply to the raster.
        format (str): Format to save the file, e.g., 'tif' or 'jp2'.

    Raises:
        Exception: If there is an error saving the raster file.
    """
    try:
        out_meta = src.meta.copy()
        out_meta.update(
            {
                "driver": "GTiff" if format == "tif" else "JP2OpenJPEG",
                "height": image.shape[1],
                "width": image.shape[2],
                "transform": transform,
            }
        )
        with rasterio.open(temp_file, "w", **out_meta) as dest:
            dest.write(image)
    except Exception as e:
        raise Exception(f"Failed to save raster: {str(e)}")


def cut_from_geometry(gdf_parcela, format, image_paths, geometry_id):
    """Cuts multiple rasters based on a parcel geometry and returns a list of temporary files.

    Args:
        gdf_parcela (GeoDataFrame or dict): GeoDataFrame containing the geometry, or a dictionary representing the parcel geometry.
        format (str): Format for output raster files, e.g., 'tif' or 'jp2'.
        image_paths (list of str): List of paths to raster files to be cut.

    Returns:
        list: List of file paths to the cropped raster images.

    Raises:
        FileNotFoundError: If no raster files match the format.
        Exception: For other errors during the cutting process.
    """
    try:
        if isinstance(gdf_parcela, dict):
            if "coordinates" not in gdf_parcela:
                raise ValueError(
                    "Invalid parcel geometry dictionary: 'coordinates' key missing."
                )

            parcela_geometry = shape(gdf_parcela)
            parcela_crs = gdf_parcela.get("CRS", {"init": "epsg:4326"})
            gdf_parcela = gpd.GeoDataFrame(geometry=[parcela_geometry], crs=parcela_crs)

        cropped_images = []
        valid_files = [f for f in image_paths if f.endswith(f".{format}")]
        if not valid_files:
            raise FileNotFoundError(f"No files found with the .{format} format.")

        for image_path in valid_files:
            original_filename = os.path.basename(image_path)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
            with rasterio.open(image_path) as src:
                gdf_parcela = gdf_parcela.to_crs(src.crs)
                if gdf_parcela.is_empty.any():
                    print(f"Parcel geometry is empty for image {image_path}.")
                    continue

                geometries = [gdf_parcela.geometry.iloc[0]]
                out_image, out_transform = mask(src, geometries, crop=True)
                extension = format.lower()
                filename = original_filename.replace(".tif", f"_{geometry_id}.{extension}").replace(".jp2", f"_{geometry_id}.{extension}")
                temp_file = os.path.join(tempfile.gettempdir(), filename)

                save_raster(out_image, temp_file, src, out_transform, format)
                cropped_images.append(temp_file)

        return cropped_images

    except FileNotFoundError as e:
        print(str(e))
        raise
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise
