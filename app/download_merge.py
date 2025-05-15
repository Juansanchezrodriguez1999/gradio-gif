import os
import tempfile
from concurrent.futures import ThreadPoolExecutor

import rasterio
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform, reproject
import shutil


load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")


def download_tif_file(client, bucket_name, obj, download_dir):
    original_file_name = obj.object_name.split("/")[-1]
    unique_file_name = f"{obj.object_name.split('/')[0]}_{original_file_name}"
    local_file_path = os.path.join(download_dir, unique_file_name)
    print("LOCAL EN DOWNLOAD",local_file_path)

    if not os.path.exists(local_file_path):
        client.fget_object(bucket_name, obj.object_name, local_file_path)
    return local_file_path


def parallel_download(client, bucket_name, download_tasks):
    downloaded_files = []

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(download_tif_file, client, bucket_name, obj, download_dir)
            for obj, download_dir in download_tasks
        ]
        print(futures)
        for future in futures:
            try:
                downloaded_files.append(future.result())
            except Exception as e:
                print(f"Error descargando archivo: {e}")
        print(downloaded_files)
    return downloaded_files


def get_months_for_year(year, start_year, start_month, end_year, end_month):
    """
    Devuelve una lista de meses a descargar para un año específico, considerando el rango de años y meses.
    """
    all_months = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]

    if year == start_year and year == end_year: 
        start_index = all_months.index(start_month)
        end_index = all_months.index(end_month) + 1
        return all_months[start_index:end_index]
    elif year == start_year: 
        start_index = all_months.index(start_month)
        return all_months[start_index:]
    elif year == end_year:  
        end_index = all_months.index(end_month) + 1
        return all_months[:end_index]
    else:  
        return all_months


def download_tif_files(utm_zones, years, indexes, months):
    """
    Descarga imágenes TIFF desde MinIO, las organiza por año, índice y mes,
    y opcionalmente fusiona las imágenes de cada mes en un único archivo.

    Args:
        utm_zones (list): Lista de zonas UTM.
        years (list): Lista de años a descargar.
        indexes (list): Lista de índices (como NDVI, NDWI) a incluir.
        months (list): Lista con el mes inicial y final (e.g., ["March", "June"]).

    Returns:
        list: Lista de rutas de las imágenes TIFF fusionadas.
    """
    local_download_path = tempfile.mkdtemp()
    bucket_name = "test-am-products"
    tiff_paths = []

    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    download_tasks = []
    valid_utm_zones = []

    for zone in utm_zones:
        has_data = False
        for year in range(int(years[0]), int(years[1]) + 1):
            applicable_months = get_months_for_year(
                str(year), years[0], months[0], years[1], months[1]
            )
            for month_folder in applicable_months:
                composites_path = f"{zone}/{year}/{month_folder}/composites/"
                try:
                    objects_list = list(client.list_objects(
                        bucket_name, prefix=composites_path, recursive=True
                    ))

                    if not objects_list:
                        continue 

                    has_data = True  

                    for obj in objects_list:
                        if (
                            obj.object_name.endswith(".tif")
                            and "indexes" in obj.object_name
                        ):
                            file_parts = obj.object_name.split("/")
                            index_name = file_parts[-1].split(".")[0].upper()
                            if index_name in indexes:
                                month_number = convertir_mes_a_numero(month_folder)
                                download_dir = os.path.join(
                                    local_download_path,
                                    str(year),
                                    index_name,
                                    str(month_number),
                                )
                                os.makedirs(download_dir, exist_ok=True)
                                download_tasks.append((obj, download_dir))
                except S3Error as exc:
                    print(f"裡 S3Error al acceder a {composites_path}: {exc}")
                except Exception as e:
                    print(f"❌ Error inesperado accediendo a {composites_path}: {e}")

        if has_data:
            valid_utm_zones.append(zone)
            print(f"✅ Zona UTM válida: '{zone}'")
        else:
            print(f"⚠️ No se encontraron datos para la zona UTM '{zone}', se omitirá.")

    parallel_download(client, bucket_name, download_tasks)
    for year in range(int(years[0]), int(years[1]) + 1):
        applicable_months = get_months_for_year(
            str(year), years[0], months[0], years[1], months[1]
        )
        for index in indexes:
            for month in applicable_months:
                month_number = convertir_mes_a_numero(month)
                carpeta_mes = os.path.join(
                    local_download_path, str(year), index, month_number
                )


                if os.path.exists(carpeta_mes):
                    merge_path = os.path.join(
                        carpeta_mes, f"{index}_{year}_{month_number}.tif"
                    )
                    if merge_tifs(carpeta_mes, merge_path):
                        print(f"✅ TIFF fusionado: {merge_path}")
                        tiff_paths.append(merge_path)
                    else:
                        print(f"❌ No se pudo fusionar TIFFs en: {carpeta_mes}")
                else:
                    print(f"⚠️ Carpeta no encontrada: {carpeta_mes}")

    print(f" Zonas UTM procesadas: {valid_utm_zones}")
    return tiff_paths

def reproyectar_tif(tif_path, destino_crs, salida_path):
    """
    Reproyecta un archivo TIFF a un CRS específico.
    """
    try:
        with rasterio.open(tif_path) as src:
            if src.crs.to_epsg() == destino_crs:
                shutil.copy(tif_path, salida_path)
                return True 
            else:
                transform, width, height = calculate_default_transform(
                    src.crs, destino_crs, src.width, src.height, *src.bounds
                )
                profile = src.profile.copy()
                profile.update({
                    'crs': destino_crs,
                    'transform': transform,
                    'width': width,
                    'height': height
                })

                with rasterio.open(salida_path, 'w', **profile) as dst:
                    for i in range(1, src.count + 1):
                        reproject(
                            source=rasterio.band(src, i),
                            destination=rasterio.band(dst, i),
                            src_transform=src.transform,
                            src_crs=src.crs,
                            dst_transform=transform,
                            dst_crs=destino_crs,
                            resampling=rasterio.enums.Resampling.nearest
                        )
                print(f"✅ Reproyectado TIFF {tif_path} a {destino_crs}")
                return True
    except Exception as e:
        print(f"❌ Error reproyectando TIFF {tif_path}: {e}")
        return False

def merge_tifs(carpeta_entrada, salida_path, destino_crs=4326):
    """
    Fusiona archivos TIFF en una carpeta en un único archivo TIFF. Reproyecta los TIFFs al CRS destino antes de fusionar.
    """
    imagenes_tif = [
        os.path.join(carpeta_entrada, f)
        for f in os.listdir(carpeta_entrada)
        if f.endswith(".tif")
    ]

    if not imagenes_tif:
        print(f"⚠️ No se encontraron imágenes TIFF en la carpeta: {carpeta_entrada}")
        return False

    with rasterio.open(imagenes_tif[0]) as src:
        crs_ref = src.crs

    reproyectados = []
    for tif in imagenes_tif:
        tif_reproyectado = tif.replace(".tif", f"_reproyectado.tif")
        if reproyectar_tif(tif, crs_ref, tif_reproyectado):
            reproyectados.append(tif_reproyectado)

    if len(reproyectados) < 1:
        print("❌ No se pudieron reproyectar las imágenes.")
        return False

    datasets = []
    for imagen in reproyectados:
        try:
            print(f" Abriendo TIFF reproyectado: {imagen}")
            ds = rasterio.open(imagen)
            datasets.append(ds)
        except Exception as e:
            print(f"❌ Error abriendo TIFF reproyectado {imagen}: {e}")

    if not datasets:
        print(f"❌ No se pudieron abrir imágenes TIFF reproyectadas en {carpeta_entrada}")
        return False

    try:
        print(f"⚙️ Fusionando {len(datasets)} archivos TIFF...")
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

        with rasterio.open(salida_path, "w", **perfil) as dst:
            dst.write(merged)

        print(f"✅ Fusión completada: {salida_path}")
        return True

    except Exception as e:
        print(f"❌ Error durante la fusión de TIFFs en {carpeta_entrada}: {e}")
        return False

    finally:
        for ds in datasets:
            ds.close()
            
def convertir_mes_a_numero(mes):
    meses = {
        "January": "01",
        "February": "02",
        "March": "03",
        "April": "04",
        "May": "05",
        "June": "06",
        "July": "07",
        "August": "08",
        "September": "09",
        "October": "10",
        "November": "11",
        "December": "12",
    }
    num = meses.get(mes, "00")
    print(f" Mes '{mes}' convertido a número '{num}'")
    return num


