import os
import rasterio
import numpy as np
import cv2
from PIL import Image
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tempfile import TemporaryDirectory
from minio import Minio
from minio.error import S3Error
import tempfile
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont
from app.generate_map import merge_tifs_por_fecha
from collections import defaultdict
from rasterio.merge import merge


load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
bucket_name = os.getenv("bucket_name")

client = Minio(
    endpoint=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)



def convertir_mes_a_numero(nombre_mes):
    return str(datetime.strptime(nombre_mes, "%B").month).zfill(2)

def generar_rango_fechas(years, months):
    start_date = datetime.strptime(f"{months[0]} {years[0]}", "%B %Y")
    end_date = datetime.strptime(f"{months[1]} {years[1]}", "%B %Y")
    fechas = []
    while start_date <= end_date:
        fechas.append((start_date.year, start_date.strftime("%B")))
        start_date += relativedelta(months=1)
    return fechas

def descargar_archivo(client, obj, local_file_path):
    client.fget_object(bucket_name, obj.object_name, local_file_path)

def merge_tifs(input_dir, year, banda, month_number):
    archivos = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".tif")]
    if not archivos:
        return None
    mosaico, out_transform = rasterio.merge.merge([rasterio.open(f) for f in archivos])
    out_meta = rasterio.open(archivos[0]).meta.copy()
    out_meta.update({
        "driver": "GTiff",
        "height": mosaico.shape[1],
        "width": mosaico.shape[2],
        "transform": out_transform
    })
    salida_path = f"data/merge/{year}/{banda}"
    os.makedirs(salida_path, exist_ok=True)
    merge_path = f"{salida_path}/RGB_{year}_{month_number}_{banda}.tif"
    with rasterio.open(merge_path, "w", **out_meta) as dest:
        dest.write(mosaico)
    return merge_path

def descargar_archivos_tif(utm_zones, years, months):
    year_month_pairs = generar_rango_fechas(years, months)
    bandas = ["B02_20m", "B03_20m", "B04_20m"]
    rutas_mergeadas = []

    with TemporaryDirectory() as local_download_path:
        download_tasks = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for zone in utm_zones:
                for year, month_folder in year_month_pairs:
                    composites_path = f"{zone}/{year}/{month_folder}/composites/"
                    try:
                        objects = client.list_objects(bucket_name, prefix=composites_path, recursive=True)
                        for obj in objects:
                            if obj.object_name.endswith(".tif") and "raw" in obj.object_name:
                                band_name = obj.object_name.split("/")[-1].split(".")[0]
                                if band_name in bandas:
                                    month_number = convertir_mes_a_numero(month_folder)
                                    download_dir = os.path.join(local_download_path, str(year), band_name, month_number)
                                    os.makedirs(download_dir, exist_ok=True)
                                    local_file_path = os.path.join(download_dir, f"{zone}.tif")
                                    download_tasks.append(executor.submit(descargar_archivo, client, obj, local_file_path))
                    except S3Error as exc:
                        print(f"Error al acceder a {composites_path}: {exc}")

            for future in as_completed(download_tasks):
                future.result()

        for year, month_folder in year_month_pairs:
            month_number = convertir_mes_a_numero(month_folder)
            for banda in bandas:
                carpeta_mes = os.path.join(local_download_path, str(year), banda, month_number)
                if os.path.exists(carpeta_mes):
                    merge_path = merge_tifs(carpeta_mes, year, banda, month_number)
                    if merge_path:
                        rutas_mergeadas.append(merge_path)

    return rutas_mergeadas


MESES = {
    "01": "Enero", "02": "Febrero", "03": "Marzo", "04": "Abril",
    "05": "Mayo", "06": "Junio", "07": "Julio", "08": "Agosto",
    "09": "Septiembre", "10": "Octubre", "11": "Noviembre", "12": "Diciembre"
}

def rgb(rutas_mergeadas):
    salida_dir = tempfile.mkdtemp()
    rutas_png = []
    rutas_tif_rgb = []

    agrupadas = {}
    for ruta in rutas_mergeadas:
        nombre = os.path.basename(ruta)
        nombre_sin_ext = os.path.splitext(nombre)[0]
        partes = nombre_sin_ext.split("_")
        year = partes[1]
        mes_tif = partes[2]
        banda = partes[3] + "_" + partes[4]
        clave = (year, mes_tif)
        if clave not in agrupadas:
            agrupadas[clave] = {}
        agrupadas[clave][banda] = ruta

    frames = []

    for (year, month_number), bandas_dict in agrupadas.items():
        try:
            ruta_banda_2 = bandas_dict["B02_20m"]
            ruta_banda_3 = bandas_dict["B03_20m"]
            ruta_banda_4 = bandas_dict["B04_20m"]
        except KeyError:
            continue

        with rasterio.open(ruta_banda_4) as src4, \
             rasterio.open(ruta_banda_3) as src3, \
             rasterio.open(ruta_banda_2) as src2:

            red = handle_nodata(src4.read(1), src4.nodata)
            green = handle_nodata(src3.read(1), src3.nodata)
            blue = handle_nodata(src2.read(1), src2.nodata)

            profile = src4.profile
            profile.update(count=3, dtype=rasterio.uint16, nodata=None)
            nombre_tif = os.path.join(salida_dir, f"RGB_{year}_{month_number}.tif")

            with rasterio.open(nombre_tif, 'w', **profile) as dst:
                dst.write(red, 1)
                dst.write(green, 2)
                dst.write(blue, 3)

            rutas_tif_rgb.append((nombre_tif, year, month_number))

    for ruta_rgb, year, month_number in rutas_tif_rgb:
        with rasterio.open(ruta_rgb) as src:
            red = handle_nodata(src.read(1), src.nodata)
            green = handle_nodata(src.read(2), src.nodata)
            blue = handle_nodata(src.read(3), src.nodata)

            red_norm = normalize(red)
            green_norm = normalize(green)
            blue_norm = normalize(blue)

            alpha = np.where(
                (red_norm == 0) & (green_norm == 0) & (blue_norm == 0),
                0,
                255
            ).astype(np.uint8)

            rgb_image = np.stack([red_norm, green_norm, blue_norm], axis=-1)
            rgb_image = gamma_correction(rgb_image, gamma=1.5)
            rgba_image = np.dstack([rgb_image, alpha])

            img_pil = Image.fromarray(rgba_image, mode="RGBA")

            escala = 8
            img_grande = img_pil.resize(
                (img_pil.width * escala, img_pil.height * escala),
                resample=Image.BICUBIC
            )

            overlay = Image.new("RGBA", img_grande.size, (255, 255, 255, 0))
            draw = ImageDraw.Draw(overlay)

            mes_nombre = MESES.get(month_number, month_number)
            texto = f"{mes_nombre} {year}"

            font_size = max(24, img_grande.width // 40)
            try:
                font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
            except:
                font = ImageFont.load_default()

            try:
                bbox = draw.textbbox((0, 0), texto, font=font)
                text_w = bbox[2] - bbox[0]
                text_h = bbox[3] - bbox[1]
            except AttributeError:
                text_w, text_h = font.getsize(texto)

            padding = 10
            fondo_padding = int(font_size * 0.6)  

            x, y = fondo_padding, fondo_padding

            draw.rectangle(
                [x - fondo_padding, y - fondo_padding, x + text_w + fondo_padding, y + text_h + fondo_padding],
                fill=(0, 0, 0, 160)
            )

            sombra_offset = int(font_size * 0.08)
            draw.text((x + sombra_offset, y + sombra_offset), texto, font=font, fill=(0, 0, 0, 200))

            draw.text((x, y), texto, font=font, fill=(255, 255, 255, 255))
            final_img = Image.alpha_composite(img_grande, overlay)

            nombre_png = os.path.join(salida_dir, f"{year}_{month_number}.png")
            final_img.save(nombre_png)
            rutas_png.append(nombre_png)
            frames.append(final_img)

    output_gif = os.path.join(tempfile.gettempdir(), "animation.gif")
    if frames:
        frames[0].save(output_gif, save_all=True, append_images=frames[1:], duration=1000, loop=0)

    return salida_dir, rutas_png, rutas_tif_rgb, output_gif
    
def crear_tiff_rgb(rutas_mergeadas):
    salida_dir = tempfile.mkdtemp()
    rutas_tif_rgb = []

    agrupadas = {}
    for ruta in rutas_mergeadas:
        nombre = os.path.basename(ruta)
        year, banda, mes_tif = ruta.split("/")[-3], ruta.split("/")[-2], os.path.splitext(nombre)[0]
        clave = (year, mes_tif)
        if clave not in agrupadas:
            agrupadas[clave] = {}
        agrupadas[clave][banda] = ruta

    for (year, month_number), bandas_dict in agrupadas.items():
        try:
            ruta_banda_2 = bandas_dict["B02_20m"]
            ruta_banda_3 = bandas_dict["B03_20m"]
            ruta_banda_4 = bandas_dict["B04_20m"]
        except KeyError:
            continue

        with rasterio.open(ruta_banda_4) as src4, \
             rasterio.open(ruta_banda_3) as src3, \
             rasterio.open(ruta_banda_2) as src2:

            red = handle_nodata(src4.read(1), src4.nodata)
            green = handle_nodata(src3.read(1), src3.nodata)
            blue = handle_nodata(src2.read(1), src2.nodata)

            profile = src4.profile
            profile.update(count=3, dtype=rasterio.uint16, nodata=None)
            nombre_tif = os.path.join(salida_dir, f"{year}_{month_number}.tif")
            with rasterio.open(nombre_tif, 'w', **profile) as dst:
                dst.write(red, 1)
                dst.write(green, 2)
                dst.write(blue, 3)
            rutas_tif_rgb.append(nombre_tif)

    return salida_dir, rutas_tif_rgb

def gamma_correction(image, gamma=1.5):
    inv_gamma = 1.0 / gamma
    table = np.array([(i / 255.0) ** inv_gamma * 255 for i in np.arange(0, 256)]).astype("uint8")
    return cv2.LUT(image, table)

def handle_nodata(array, nodata_value):
    if nodata_value is not None:
        array = np.where(array == nodata_value, 0, array)
    array = np.where(np.isnan(array), 0, array)
    return array

def normalize(array):
    valid_pixels = array[array > 0]
    if len(valid_pixels) == 0:
        return np.zeros_like(array, dtype=np.uint8)
    array_min, array_max = np.percentile(valid_pixels, [2, 99.999])
    if array_max - array_min == 0:
        return np.zeros_like(array, dtype=np.uint8)
    norm_array = (array - array_min) / (array_max - array_min)
    norm_array = np.clip(norm_array * 255, 0, 255)
    return norm_array.astype(np.uint8)

def crear_gif(ruta_imagenes):
    salida_gif = tempfile.mkdtemp()
    imagenes = sorted([f for f in os.listdir(ruta_imagenes) if f.endswith('.png')])
    if not imagenes:
        print("No se encontraron imágenes para el GIF.")
        return
    frames = [Image.open(os.path.join(ruta_imagenes, img)) for img in imagenes]
    gif_path = os.path.join(salida_gif, "output.gif")
    os.makedirs(salida_gif, exist_ok=True)
    frames[0].save(gif_path, format="GIF", save_all=True, append_images=frames[1:], duration=800, loop=0)
    print(f"GIF guardado en: {gif_path}")
    return gif_path

def workflow_generar_gif(utm_zones, years, months):
    rutas_mergeadas = descargar_archivos_tif(utm_zones, years, months)
    rgb_folder, rutas_png, rutas_tif_rgb = rgb(rutas_mergeadas)
    gif_path = crear_gif(rgb_folder)
    return gif_path, rutas_tif_rgb, rutas_png

def merge_tifs_por_fecha_banda(tif_paths):
    """
    Recibe una lista de imágenes TIFF, las agrupa por índice, fecha (año y mes) y banda,
    y las fusiona. Devuelve la lista de rutas a los TIFFs fusionados por grupo.

    Args:
        tif_paths (list): Lista de rutas a archivos .tif

    Returns:
        list: Lista de rutas a los TIFFs fusionados por grupo.
    """
    grupos = defaultdict(list)
    for path in tif_paths:
        filename = os.path.basename(path)
        parts = filename.split("_")
        if len(parts) < 4:
            continue 
        indice = parts[0].upper()
        anio = parts[1]
        mes = parts[2]
        banda1 = parts[3]
        banda2 = parts[4]
        clave = f"{indice}_{anio}_{mes}_{banda1}_{banda2}"
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
            count=merged.shape[0],
            dtype=merged.dtype,
        )

        output_path = os.path.join(temp_dir, f"{clave}.tif")
        with rasterio.open(output_path, "w", **perfil) as dst:
            dst.write(merged)

        for ds in datasets:
            ds.close()

        rutas_mergeadas.append(output_path)

    return rutas_mergeadas
