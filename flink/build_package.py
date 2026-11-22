
# ============================================================
# EMPAQUETADO REPRODUCIBLE DE PYFLINK
# PRE-ENTREGA 05 - DATA ENGINEERING
# ============================================================

from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import hashlib

BASE_DIR = Path(__file__).resolve().parent

MAIN_FILE = BASE_DIR / "main.py"
JAR_FILE = BASE_DIR / "target" / "pyflink-dependencies.jar"

OUTPUT_FILE = BASE_DIR / "application-pre05.zip"


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def main():

    # Comprobar que los artefactos necesarios existan.
    for file in (MAIN_FILE, JAR_FILE):
        if not file.is_file():
            raise FileNotFoundError(
                f"Archivo requerido no encontrado: {file}"
            )

    # Crear ZIP con rutas internas compatibles con Flink.
    with ZipFile(
        OUTPUT_FILE,
        "w",
        compression=ZIP_DEFLATED
    ) as zip_file:

        zip_file.write(
            MAIN_FILE,
            arcname="main.py"
        )

        zip_file.write(
            JAR_FILE,
            arcname="lib/pyflink-dependencies.jar"
        )

    # Validar estructura, integridad y contenido.
    with ZipFile(OUTPUT_FILE, "r") as zip_file:

        expected = {
            "main.py",
            "lib/pyflink-dependencies.jar"
        }

        if set(zip_file.namelist()) != expected:
            raise ValueError(
                "Estructura del ZIP incorrecta"
            )

        if zip_file.testzip() is not None:
            raise ValueError(
                "Error de integridad del ZIP"
            )

        for archive_name, source in (
            ("main.py", MAIN_FILE),
            ("lib/pyflink-dependencies.jar", JAR_FILE)
        ):
            if sha256(zip_file.read(archive_name)) != sha256(
                source.read_bytes()
            ):
                raise ValueError(
                    f"Hash incorrecto: {archive_name}"
                )

    print("ZIP GENERADO CORRECTAMENTE")
    print("ESTRUCTURA OK")
    print("INTEGRIDAD OK")
    print("HASHES OK")
    print(
        f"TAMANO: "
        f"{OUTPUT_FILE.stat().st_size / 1024 / 1024:.2f} MiB"
    )


if __name__ == "__main__":
    main()
