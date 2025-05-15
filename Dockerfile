FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgl1 libglib2.0-0\
    libgdal-dev \
    git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*


ENV GDAL_DATA=/usr/share/gdal

ENV PROJ_LIB=/usr/share/proj

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY ./app /app

CMD ["fastapi", "run", "main.py", "--port", "80"]