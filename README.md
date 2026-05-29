# FreshMart DataOps Lakehouse — Apache Spark + Delta Lake

Proyecto académico que implementa una arquitectura **Data Lakehouse** con patrón **Medallion** (Bronze → Silver → Gold) para **FreshMart**, una cadena de supermercados centroamericana con operaciones en Guatemala, El Salvador, Honduras, Nicaragua y Costa Rica.

El sistema genera **1,000,000 de transacciones de ventas simuladas** y las procesa con Apache Spark y Delta Lake, produciendo tablas analíticas listas para consumo por dashboards e IA.

---

## Tecnologías utilizadas

- Docker y Docker Compose
- Python 3.11
- Apache Spark 3.5 / PySpark
- Delta Lake 3.1
- Arquitectura Medallion: Bronze, Silver y Gold

---

## Estructura del proyecto

```text
freshmart_lakehouse/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
├── jobs/
│   ├── spark_session.py        # Fábrica de SparkSession con Delta Lake
│   ├── 01_generate_dataset.py  # Generador de 1M de ventas simuladas
│   ├── 02_bronze.py            # Ingesta cruda sin transformar
│   ├── 03_silver.py            # Limpieza, tipado y campos derivados
│   ├── 04_gold.py              # KPIs y tablas analíticas de negocio
│   ├── 05_queries.py           # Verificación de resultados Gold
│   └── run_pipeline.py         # Orquestador del pipeline completo
├── data/                       # CSV crudo generado (gitignore)
├── delta/                      # Tablas Delta Lake Bronze/Silver/Gold
└── output/                     # CSV de evidencias por tabla Gold
```

---

## Requisitos previos

1. Tener **Docker Desktop** instalado y en ejecución.
2. Abrir una terminal (PowerShell en Windows, bash en Mac/Linux) dentro de la carpeta del proyecto.

---

## Ejecución paso a paso

### 1. Construir la imagen Docker

```powershell
docker compose build
```

### 2. Levantar el contenedor en segundo plano

```powershell
docker compose up -d
```

### 3. Ingresar al contenedor

```powershell
docker exec -it freshmart_lakehouse_spark bash
```

### 4. Prueba rápida (10,000 registros)

Recomendado para verificar que todo funciona antes del run completo:

```bash
python jobs/run_pipeline.py --rows 10000
```

### 5. Pipeline completo con 1 millón de registros

```bash
python jobs/run_pipeline.py --rows 1000000
```

---

## Resultados esperados

Al finalizar el pipeline correctamente se generan estas rutas:

```text
data/
  ventas_crudas.csv

delta/
  bronze/ventas_raw/
  silver/ventas_limpias/
  gold/kpis_globales/
  gold/ventas_por_categoria/
  gold/ventas_por_pais/
  gold/ventas_por_trimestre/
  gold/top10_productos/
  gold/ventas_por_canal/

output/
  kpis_globales/
  ventas_por_categoria/
  ventas_por_pais/
  ventas_por_trimestre/
  top10_productos/
  ventas_por_canal/
```

---

## Evidencias recomendadas (capturas de pantalla)

1. `docker compose build` — construcción exitosa de la imagen
2. `docker compose up -d` y `docker ps` — contenedor activo
3. Ejecución del pipeline: `python jobs/run_pipeline.py --rows 1000000`
4. Carpeta `delta/` mostrando las tres capas Medallion
5. Carpeta `output/` con los CSV de resultados
6. Consola con los KPIs globales y consultas Gold impresas

---

## Explicación de cada capa

| Capa | Responsabilidad |
|------|----------------|
| **Bronze** | Almacena el CSV crudo sin ninguna modificación. Garantiza inmutabilidad y trazabilidad del origen. |
| **Silver** | Limpia duplicados, convierte tipos, estandariza texto y calcula el campo `total_gtq`. |
| **Gold** | Genera 6 tablas analíticas: KPIs globales, ventas por categoría, por país, por trimestre, top productos y participación por canal de venta. |

---

## Dataset simulado

El dataset representa ventas de supermercado con los siguientes atributos:

- **20 productos** de categorías como Granos, Lácteos, Carnes, Higiene y Bebidas
- **5 países** centroamericanos
- **4 canales**: Caja Presencial, App Móvil, Página Web, Kiosco Autoservicio
- **4 métodos de pago**: Efectivo, Tarjeta Débito, Tarjeta Crédito, Transferencia QR
- **50 sucursales** distribuidas regionalmente
- Precios en **Quetzales (GTQ)** con variación realista de ±15%
