import os
import sys
import zipfile
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi

DATA_DIR = Path("data")
FILES_TO_DOWNLOAD = [
    "yelp_academic_dataset_business.json",
    "yelp_academic_dataset_review.json"
]

def download_yelp():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Verifica credenciales en el entorno o en ~/.kaggle/kaggle.json
    api = KaggleApi()
    api.authenticate()

    print("Iniciando descarga automatizada desde Kaggle...")
    for filename in FILES_TO_DOWNLOAD:
        final_short_name = "business.json" if "business" in filename else "review.json"
        target_path = DATA_DIR / final_short_name
        
        if target_path.exists():
            print(f"El archivo {final_short_name} ya existe. Omitiendo descarga.")
            continue

        print(f"Descargando {filename}...")
        api.dataset_download_file(
            dataset="yelp-dataset/yelp-dataset",
            file_name=filename,
            path=str(DATA_DIR),
            force=False,
            quiet=False
        )

        # Si descarga en zip individual, se descomprime
        zip_path = DATA_DIR / f"{filename}.zip"
        if zip_path.exists():
            print(f"Descomprimiendo {zip_path.name}...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(DATA_DIR)
            zip_path.unlink()

        # Estandarizar nombre corto
        raw_extracted = DATA_DIR / filename
        if raw_extracted.exists() and raw_extracted != target_path:
            raw_extracted.rename(target_path)

    print("Descarga y descompresión completadas con éxito.")

if __name__ == "__main__":
    download_yelp()
