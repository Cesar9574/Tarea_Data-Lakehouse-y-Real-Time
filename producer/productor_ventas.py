"""
producer/productor_ventas.py
----------------------------
Simula el flujo de eventos de caja en tiempo real para FreshMart.
Cada vez que un cliente paga en cualquier sucursal centroamericana,
el sistema de punto de venta genera un evento JSON que este productor
publica en el tópico Kafka 'ventas-tiempo-real'.

En producción, este rol lo cumple el sistema POS de cada sucursal.
Aquí lo simulamos con datos aleatorios a una frecuencia configurable.

Uso:
    python producer/productor_ventas.py                  # 1 evento/segundo
    python producer/productor_ventas.py --intervalo 0.5  # 2 eventos/segundo
    python producer/productor_ventas.py --total 500      # solo 500 eventos
"""

import argparse
import json
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer

# ── Catálogo de datos de simulación ──────────────────────────────────────────

PRODUCTOS = [
    ("Arroz Blanco 5lb",     "Granos y Cereales",  28.0),
    ("Frijol Negro 2lb",     "Granos y Cereales",  18.0),
    ("Aceite Vegetal 1L",    "Aceites y Grasas",   32.0),
    ("Leche Entera 1L",      "Lácteos",            14.0),
    ("Queso Fresco 500g",    "Lácteos",            38.0),
    ("Pollo Entero 1kg",     "Carnes y Aves",      52.0),
    ("Huevos Blancos x12",   "Huevos",             30.0),
    ("Pan Molde Blanco",     "Panadería",          16.0),
    ("Jabón de Baño x3",     "Higiene Personal",   35.0),
    ("Detergente 1kg",       "Limpieza del Hogar", 40.0),
    ("Gaseosa 2L",           "Bebidas",            20.0),
    ("Agua Purificada 1.5L", "Bebidas",             8.0),
]

SUCURSALES = {
    "Guatemala":   [f"GT-{i:02d}" for i in range(1, 16)],
    "El Salvador": [f"SV-{i:02d}" for i in range(1, 11)],
    "Honduras":    [f"HN-{i:02d}" for i in range(1, 11)],
    "Nicaragua":   [f"NI-{i:02d}" for i in range(1,  8)],
    "Costa Rica":  [f"CR-{i:02d}" for i in range(1,  8)],
}

CANALES = ["Caja Presencial", "App Móvil", "Página Web", "Kiosco Autoservicio"]
PAGOS   = ["Efectivo", "Tarjeta Débito", "Tarjeta Crédito", "Transferencia QR"]

# ── Generador de eventos ──────────────────────────────────────────────────────

def generar_evento() -> dict:
    """Crea un evento de compra aleatorio con todos los campos del esquema."""
    producto, categoria, precio_base = random.choice(PRODUCTOS)
    pais = random.choice(list(SUCURSALES.keys()))
    sucursal = random.choice(SUCURSALES[pais])
    unidades = random.randint(1, 8)
    precio = round(precio_base * random.uniform(0.88, 1.12), 2)

    return {
        "id_transaccion": random.randint(10_000_000, 99_999_999),
        "timestamp":      datetime.utcnow().isoformat() + "Z",
        "id_cliente":     random.randint(1, 200_000),
        "pais":           pais,
        "sucursal":       sucursal,
        "producto":       producto,
        "categoria":      categoria,
        "unidades":       unidades,
        "precio_unitario_gtq": precio,
        "total_gtq":      round(unidades * precio, 2),
        "canal_venta":    random.choice(CANALES),
        "metodo_pago":    random.choice(PAGOS),
    }

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Productor Kafka — FreshMart Ventas en Tiempo Real")
    parser.add_argument("--broker",    default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topico",    default="ventas-tiempo-real")
    parser.add_argument("--intervalo", type=float, default=1.0,  help="Segundos entre eventos")
    parser.add_argument("--total",     type=int,   default=0,    help="0 = infinito")
    args = parser.parse_args()

    productor = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    print(f"[Productor] Conectado a Kafka en {args.broker}")
    print(f"[Productor] Publicando en tópico '{args.topico}' — intervalo {args.intervalo}s")
    print("[Productor] Ctrl+C para detener\n")

    enviados = 0
    try:
        while args.total == 0 or enviados < args.total:
            evento = generar_evento()
            productor.send(args.topico, value=evento)
            enviados += 1
            print(
                f"[{enviados:>6}] {evento['timestamp']}  "
                f"{evento['sucursal']} | {evento['producto']:22s} | "
                f"Q{evento['total_gtq']:7.2f} | {evento['canal_venta']}"
            )
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print("\n[Productor] Detenido por el usuario.")
    finally:
        productor.flush()
        productor.close()
        print(f"[Productor] Total de eventos publicados: {enviados:,}")


if __name__ == "__main__":
    main()
