"""
01_generate_dataset.py
Genera un dataset simulado de ventas de supermercado para FreshMart Centroamérica.
Se producen N registros con datos realistas de productos, sucursales y canales de venta.
"""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Catálogo de productos: (nombre, categoría, precio_base_GTQ)
CATALOGO_PRODUCTOS = [
    ("Arroz Blanco 5lb", "Granos y Cereales", 28),
    ("Frijol Negro 2lb", "Granos y Cereales", 18),
    ("Aceite Vegetal 1L", "Aceites y Grasas", 32),
    ("Leche Entera 1L", "Lácteos", 14),
    ("Queso Fresco 500g", "Lácteos", 38),
    ("Pollo Entero 1kg", "Carnes y Aves", 52),
    ("Carne Molida 500g", "Carnes y Aves", 45),
    ("Pan Molde Blanco", "Panadería", 16),
    ("Huevos Blancos x12", "Huevos", 30),
    ("Azúcar Blanca 2kg", "Endulzantes", 22),
    ("Sal Refinada 500g", "Condimentos", 6),
    ("Tomate Rojo 1kg", "Frutas y Verduras", 12),
    ("Cebolla Blanca 1kg", "Frutas y Verduras", 10),
    ("Jabón de Baño x3", "Higiene Personal", 35),
    ("Detergente 1kg", "Limpieza del Hogar", 40),
    ("Papel Higiénico x6", "Higiene Personal", 42),
    ("Gaseosa 2L", "Bebidas", 20),
    ("Agua Purificada 1.5L", "Bebidas", 8),
    ("Shampoo 400ml", "Higiene Personal", 55),
    ("Pasta Dental 100ml", "Higiene Personal", 24),
]

PAISES_CENTROAMERICA = ["Guatemala", "El Salvador", "Honduras", "Nicaragua", "Costa Rica"]

CANALES_VENTA = ["Caja Presencial", "App Móvil", "Página Web", "Kiosco Autoservicio"]

METODOS_PAGO = ["Efectivo", "Tarjeta Débito", "Tarjeta Crédito", "Transferencia QR"]


def generar_dataset(num_registros: int, ruta_salida: str) -> None:
    """
    Genera el CSV de ventas brutas y lo escribe en ruta_salida.
    Cada fila representa una línea de compra individual en una sucursal.
    """
    Path(ruta_salida).parent.mkdir(parents=True, exist_ok=True)

    # Semilla fija para reproducibilidad, diferente a la del ejemplo base
    random.seed(2024)

    fecha_inicio = datetime(2024, 1, 1)

    with open(ruta_salida, "w", newline="", encoding="utf-8") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow([
            "id_transaccion", "fecha_compra", "id_cliente", "pais",
            "sucursal", "producto", "categoria", "unidades",
            "precio_unitario_gtq", "canal_venta", "metodo_pago"
        ])

        for i in range(1, num_registros + 1):
            producto, categoria, precio_base = random.choice(CATALOGO_PRODUCTOS)
            unidades = random.randint(1, 10)
            # Variación de precio entre ±15% del precio base
            precio = round(precio_base * random.uniform(0.85, 1.15), 2)
            dias_offset = random.randint(0, 365)
            fecha = fecha_inicio + timedelta(days=dias_offset)
            pais = random.choice(PAISES_CENTROAMERICA)
            sucursal = f"Sucursal-{random.randint(1, 50)}"
            cliente = random.randint(1, 200000)
            canal = random.choice(CANALES_VENTA)
            pago = random.choice(METODOS_PAGO)

            escritor.writerow([
                i, fecha.strftime("%Y-%m-%d"), cliente, pais, sucursal,
                producto, categoria, unidades, precio, canal, pago
            ])

    print(f"[OK] Dataset generado en: {ruta_salida} — {num_registros:,} registros")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Genera dataset de ventas FreshMart")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Número de registros a generar")
    parser.add_argument("--output", default="data/ventas_crudas.csv", help="Ruta del archivo CSV de salida")
    args = parser.parse_args()

    generar_dataset(args.rows, args.output)
