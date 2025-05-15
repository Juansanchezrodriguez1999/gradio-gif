import os
import tempfile
from typing import List

import folium
import numpy as np
import rasterio
from rasterio.warp import transform_bounds
from pyproj import CRS
from matplotlib import pyplot as plt
from matplotlib.colors import Normalize
from PIL import Image
from shapely.geometry import shape
from shapely.ops import unary_union
from matplotlib.colors import LinearSegmentedColormap
from branca.colormap import LinearColormap
from collections import defaultdict
from rasterio.merge import merge
from PIL import Image, ImageDraw, ImageFont




colors = [
    (0.5, 0.25, 0.0),  # Marrón (suelo abierto)
    (1.0, 0.0, 0.0),    # Rojo (estrés severo)
    (1.0, 0.5, 0.0),    # Naranja (estrés moderado)
    (1.0, 1.0, 0.0),    # Amarillo (estrés leve)
    (0.0, 1.0, 1.0),    # Verde claro (estrés emergente)
    (0.0, 0.0, 1.0)     # Azul (sin estrés)
]
custom_cmap = LinearSegmentedColormap.from_list("NDWI_cmap", colors, N=256)


def generate_map_from_geojson(geojson_data: dict, image_paths: List[str], gif_path, indexes) -> str:
    vmin = -0.6
    vmax = 0.25
    date_groups = {}

    # Obtener CRS del GeoJSON (suponiendo que usa EPSG:4326 por defecto)
    geojson_crs = CRS.from_epsg(4326)

    for tiff_file in image_paths:
        base_name = os.path.basename(tiff_file)
        parts = base_name.split("_")
        date = f"{parts[1]}_{parts[2]}"  
        
        png_name = os.path.splitext(base_name)[0] + ".png"
        output_png_path = os.path.join(tempfile.gettempdir(), png_name)

        with rasterio.open(tiff_file) as src:
            array = src.read(1)
            norm = Normalize(vmin=vmin, vmax=vmax)

            plt.imshow(array, norm=norm, cmap=custom_cmap)
            plt.axis("off")
            temp_png_path = os.path.join(tempfile.gettempdir(), f"temp_{png_name}")
            plt.savefig(temp_png_path, bbox_inches="tight", pad_inches=0, dpi=800)
            plt.close()


            tiff_crs = CRS(src.crs)
            bounds = src.bounds 
            

            if tiff_crs != geojson_crs:
                bounds = transform_bounds(tiff_crs, geojson_crs, *bounds)


            offset_x = (bounds[2] - bounds[0]) 
            offset_y = (bounds[3] - bounds[1]) 
            
            image_bounds = [[bounds[3] - offset_y, bounds[0] + offset_x], 
                            [bounds[1] + offset_y, bounds[2] - offset_x]] 

        with Image.open(temp_png_path) as img:
            img = img.convert("RGBA")
            data = np.array(img)
            white_pixels = (data[:, :, 0] == 255) & (data[:, :, 1] == 255) & (data[:, :, 2] == 255)
            data[white_pixels, 3] = 0
            processed_img = Image.fromarray(data, "RGBA")
            processed_img.save(output_png_path)

        if date not in date_groups:
            date_groups[date] = []
        date_groups[date].append((output_png_path, image_bounds))  


    geometries = [shape(feature["geometry"]) for feature in geojson_data["features"]]
    unified_geometry = unary_union(geometries)
    bounds = unified_geometry.bounds
    center = [(bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2]


    '''m = folium.Map(location=center, zoom_start=15, tiles="Esri.WorldImagery")'''
    m = folium.Map(location=center, zoom_start=15)
    m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])


    #folium.GeoJson(geojson_data, name="Geometrías").add_to(m)

    _, image_bounds = next(iter(date_groups.values()))[0]


    gif_layer = folium.raster_layers.ImageOverlay(
        image=gif_path,
        bounds=image_bounds,
        opacity=1,
        name="Evolución temporal (GIF)",
        interactive=True,
        cross_origin=False
    )
    gif_layer.add_to(m)

    if(indexes!=["RGB"]):

        color_scale = LinearColormap(
            colors=[(0.5, 0.25, 0.0), (1.0, 0.0, 0.0), (1.0, 0.5, 0.0),
                    (1.0, 1.0, 0.0), (0.0, 1.0, 1.0), (0.0, 0.0, 1.0)],
            vmin=vmin, vmax=vmax
        )

        color_scale.add_to(m)
    folium.LayerControl().add_to(m)
    return m

def merge_tifs_por_fecha(tif_paths):
    """
    Recibe una lista de imágenes TIFF, las agrupa por índice y fecha (año y mes en el nombre),
    y las fusiona. Devuelve la lista de rutas a los TIFFs fusionados por índice y mes.

    Args:
        tif_paths (list): Lista de rutas a archivos .tif

    Returns:
        list: Lista de rutas a los TIFFs fusionados por índice y mes.
    """
    print(tif_paths)
    grupos = defaultdict(list)
    for path in tif_paths:
        filename = os.path.basename(path)
        parts = filename.split("_")
        if len(parts) < 3:
            continue 
        indice = parts[0].upper()
        anio = parts[1]
        mes = parts[2]
        clave = f"{indice}_{anio}_{mes}"
        grupos[clave].append(path)

    rutas_mergeadas = []
    temp_dir = tempfile.mkdtemp()

    for clave, archivos in grupos.items():
        datasets = [rasterio.open(f) for f in archivos]
        merged, output_transform = merge(datasets, method="last")

        perfil = datasets[0].profile
        perfil.update(
            driver="GTiff",
            height=merged.shape[1],
            width=merged.shape[2],
            transform=output_transform,
            count=datasets[0].count,
            dtype=merged.dtype,
        )

        output_path = os.path.join(temp_dir, f"{clave}.tif")
        with rasterio.open(output_path, "w", **perfil) as dst:
            dst.write(merged)

        for ds in datasets:
            ds.close()

        rutas_mergeadas.append(output_path)

    return rutas_mergeadas

def merge_tifs_por_fecha(tif_paths):
    """
    Recibe una lista de imágenes TIFF, las agrupa por índice y fecha (año y mes en el nombre),
    y las fusiona. Devuelve la lista de rutas a los TIFFs fusionados por índice y mes.

    Args:
        tif_paths (list): Lista de rutas a archivos .tif

    Returns:
        list: Lista de rutas a los TIFFs fusionados por índice y mes.
    """
    print(tif_paths)
    grupos = defaultdict(list)
    for path in tif_paths:
        filename = os.path.basename(path)
        parts = filename.split("_")
        if len(parts) < 3:
            continue   
        indice = parts[0].upper()
        anio = parts[1]
        mes = parts[2]
        clave = f"{indice}_{anio}_{mes}"
        grupos[clave].append(path)

    rutas_mergeadas = []
    temp_dir = tempfile.mkdtemp()

    for clave, archivos in grupos.items():
        datasets = [rasterio.open(f) for f in archivos]
        merged, output_transform = merge(datasets, method="last")

        perfil = datasets[0].profile
        perfil.update(
            driver="GTiff",
            height=merged.shape[1],
            width=merged.shape[2],
            transform=output_transform,
            count=datasets[0].count,
            dtype=merged.dtype,
        )

        output_path = os.path.join(temp_dir, f"{clave}.tif")
        with rasterio.open(output_path, "w", **perfil) as dst:
            dst.write(merged)

        for ds in datasets:
            ds.close()

        rutas_mergeadas.append(output_path)

    return rutas_mergeadas

def crear_gif_no_rgb(image_paths: List[str]) -> str:
    png_list=[]
    vmin = -0.6
    vmax = 0.25
    date_groups = {}

    geojson_crs = CRS.from_epsg(4326)

    for tiff_file in image_paths:
        base_name = os.path.basename(tiff_file)
        parts = base_name.split("_")
        date = f"{parts[1]}_{parts[2]}"
        
        png_name = os.path.splitext(base_name)[0] + ".png"
        output_png_path = os.path.join(tempfile.gettempdir(), png_name)

        with rasterio.open(tiff_file) as src:
            array = src.read(1)
            norm = Normalize(vmin=vmin, vmax=vmax)

            plt.imshow(array, norm=norm, cmap=custom_cmap)
            plt.axis("off")
            temp_png_path = os.path.join(tempfile.gettempdir(), f"temp_{png_name}")
            plt.savefig(temp_png_path, bbox_inches="tight", pad_inches=0, dpi=800)
            plt.close()


            tiff_crs = CRS(src.crs)
            bounds = src.bounds 
            

            if tiff_crs != geojson_crs:
                bounds = transform_bounds(tiff_crs, geojson_crs, *bounds)


            offset_x = (bounds[2] - bounds[0]) 
            offset_y = (bounds[3] - bounds[1]) 
            
            image_bounds = [[bounds[3] - offset_y, bounds[0] + offset_x], 
                            [bounds[1] + offset_y, bounds[2] - offset_x]] 

        with Image.open(temp_png_path) as img:
            img = img.convert("RGBA")
            data = np.array(img)
            white_pixels = (data[:, :, 0] == 255) & (data[:, :, 1] == 255) & (data[:, :, 2] == 255)
            data[white_pixels, 3] = 0
            processed_img = Image.fromarray(data, "RGBA")
            processed_img.save(output_png_path)
            png_list.append(output_png_path)

        if date not in date_groups:
            date_groups[date] = []
        date_groups[date].append((output_png_path, image_bounds))  

    font_size = 50  

    font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

    try:
        font = ImageFont.truetype(font_path, font_size)
    except Exception as e:
        print("Error cargando la fuente:", e)
        font = ImageFont.load_default()

    frames = []
    for img_path in png_list:
        img = Image.open(img_path).convert("RGBA")
        draw = ImageDraw.Draw(img)

        texto = img_path.split("/")[-1].replace(".png", "")

        text_bbox = draw.textbbox((0, 0), texto, font=font)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]

        text_x = 20
        text_y = 20
        text_position = (text_x, text_y)

        bg_padding = 30
        bg_position = [
            text_x - bg_padding, text_y - bg_padding,
            text_x + text_width + bg_padding, text_y + text_height + bg_padding
        ]
        draw.rectangle(bg_position, fill=(0, 0, 0, 255))

        draw.text(text_position, texto, font=font, fill="white")
        
        frames.append(img)

    output_gif = os.path.join(tempfile.gettempdir(), "animation.gif")
    frames[0].save(output_gif, save_all=True, append_images=frames[1:], duration=1000, loop=0)

    return output_gif