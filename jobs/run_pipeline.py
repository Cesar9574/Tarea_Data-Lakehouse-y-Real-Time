"""
run_pipeline.py  —  Orquestador del Pipeline Completo FreshMart DataOps Lakehouse
Ejecuta en orden las cinco etapas del pipeline:
  01 → Generación del dataset crudo (CSV)
  02 → Ingesta Bronze (Delta Lake, sin transformar)
  03 → Limpieza Silver (Delta Lake, datos validados)
  04 → Agregaciones Gold (tablas analíticas + CSV de evidencia)
  05 → Consultas de verificación sobre la capa Gold

Uso:
  python jobs/run_pipeline.py --rows 1000000   # producción completa
  python jobs/run_pipeline.py --rows 10000     # prueba rápida
"""

import argparse
import subprocess
import sys
from pathlib import Path


def ejecutar_paso(comando: list[str]) -> None:
    """Lanza un subproceso y detiene el pipeline si retorna error."""
    print(f"\n>>> Ejecutando: {' '.join(comando)}")
    resultado = subprocess.run(comando)
    if resultado.returncode != 0:
        print(f"[ERROR] El paso falló con código {resultado.returncode}. Pipeline detenido.")
        raise SystemExit(resultado.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline DataOps — FreshMart Lakehouse")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Registros a generar (default: 1,000,000)")
    args = parser.parse_args()

    # Crear carpetas necesarias si no existen
    for carpeta in ["data", "delta", "output"]:
        Path(carpeta).mkdir(exist_ok=True)

    print("=" * 60)
    print("  FreshMart DataOps Lakehouse — Inicio de Pipeline")
    print(f"  Registros configurados: {args.rows:,}")
    print("=" * 60)

    ejecutar_paso([sys.executable, "jobs/01_generate_dataset.py", "--rows", str(args.rows)])
    ejecutar_paso([sys.executable, "-m", "jobs.02_bronze"])
    ejecutar_paso([sys.executable, "-m", "jobs.03_silver"])
    ejecutar_paso([sys.executable, "-m", "jobs.04_gold"])
    ejecutar_paso([sys.executable, "-m", "jobs.05_queries"])

    print("\n" + "=" * 60)
    print("  Pipeline finalizado correctamente.")
    print("  Revisa delta/ y output/ para ver los resultados.")
    print("=" * 60)


if __name__ == "__main__":
    main()
