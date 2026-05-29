# FreshMart Real-Time Analytics — Apache Kafka + Spark Streaming

Proyecto académico que implementa un pipeline de **procesamiento en tiempo real** para **FreshMart**, la cadena de supermercados centroamericana. Cada vez que un cliente realiza una compra en cualquier sucursal de Guatemala, El Salvador, Honduras, Nicaragua o Costa Rica, el evento fluye en milisegundos a través de Kafka y es procesado por Spark Streaming para generar KPIs ejecutivos en vivo.

Este proyecto es la continuación del **FreshMart DataOps Lakehouse** (proyecto anterior), completando la arquitectura de datos de extremo a extremo.

---

## Arquitectura del Pipeline

```
Sucursales POS          Kafka               Spark Streaming       Consola / Dashboard
─────────────────  →  ──────────────  →  ──────────────────  →  ─────────────────────
Eventos de venta       Tópico:             KPIs por ventana        Ingresos en vivo
en tiempo real         ventas-tiempo-real  deslizante 30s          por país y categoría
(simulados aquí)       (broker local)      cada 10 segundos
```

**Componentes:**
- **Productor Python** — simula el POS de cada sucursal publicando eventos JSON en Kafka
- **Apache Kafka** — broker de mensajería que desacopla la generación del procesamiento
- **Spark Structured Streaming** — consume el tópico y calcula KPIs en ventanas temporales
- **Zookeeper** — coordinador del clúster Kafka

---

## Estructura del proyecto

```text
freshmart_streaming/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
├── producer/
│   └── productor_ventas.py     # Publica eventos de compra en Kafka
└── consumer/
    └── consumidor_streaming.py # Spark lee Kafka y calcula KPIs en tiempo real
```

---

## KPIs calculados en tiempo real

El consumidor Spark genera tres tablas de métricas sobre **ventanas deslizantes de 30 segundos** (actualizadas cada 10s):

| KPI | Descripción |
|-----|-------------|
| **KPI Global** | Transacciones, ingresos totales (GTQ), ticket promedio, unidades vendidas |
| **KPI por País** | Ingresos y transacciones desglosados por los 5 países centroamericanos |
| **KPI por Categoría** | Categorías de producto más activas (Granos, Lácteos, Bebidas, etc.) |

---

## Requisitos previos

- Docker Desktop instalado y en ejecución
- Terminal PowerShell (Windows) o bash (Mac/Linux)

---

## Ejecución paso a paso

### 1. Construir las imágenes

```powershell
docker compose build
```

### 2. Levantar Kafka, Zookeeper y el contenedor de la app

```powershell
docker compose up -d
```

### 3. Verificar que los tres contenedores estén activos

```powershell
docker ps
```
Deberías ver: `fm_zookeeper`, `fm_kafka`, `fm_app`

### 4. Crear el tópico Kafka

```powershell
docker exec -it fm_kafka bash
```
Dentro del contenedor Kafka:
```bash
kafka-topics.sh --create --topic ventas-tiempo-real --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --list --bootstrap-server localhost:9092
```
Sal con `exit`.

### 5. Abrir dos terminales

**Terminal A — Consumidor Spark (inícialo primero):**
```powershell
docker exec -it fm_app bash
python consumer/consumidor_streaming.py --broker fm_kafka:9092
```

**Terminal B — Productor de eventos:**
```powershell
docker exec -it fm_app bash
python producer/productor_ventas.py --broker fm_kafka:9092 --intervalo 1
```

### 6. Observar los KPIs en vivo

En la Terminal A verás tablas actualizándose cada 10 segundos con los KPIs calculados por Spark sobre los eventos que llegan desde el productor.

---

## Evidencias recomendadas (capturas de pantalla)

1. `docker compose build` — construcción exitosa
2. `docker ps` — tres contenedores activos
3. Creación del tópico `ventas-tiempo-real` y verificación con `--list`
4. Terminal del productor mostrando eventos publicados en tiempo real
5. Terminal del consumidor mostrando tablas de KPIs actualizándose (KPI Global, por País, por Categoría)
6. Ambas terminales simultáneas (si puedes hacer captura de pantalla dividida)

---

## Explicación de conceptos clave

**¿Por qué Kafka y no simplemente llamar directo a Spark?**
Kafka actúa como buffer tolerante a fallos entre los sistemas que generan eventos y los que los procesan. Si Spark cae o se reinicia, los mensajes no se pierden — siguen en Kafka hasta que son consumidos. Esto es esencial en producción.

**¿Qué es una ventana deslizante (Sliding Window)?**
En lugar de procesar evento por evento, Spark agrupa los eventos de los últimos N segundos y calcula métricas sobre ese grupo. Con una ventana de 30s que desliza cada 10s, cada batch incluye los últimos 30 segundos de datos, pero se publica cada 10 segundos — balance entre frescura y estabilidad del KPI.

**Diferencia con el proyecto Lakehouse anterior:**
El Lakehouse procesa millones de registros históricos en batch (minutos/horas). El pipeline de streaming procesa cada venta en menos de un segundo. Ambos son complementarios: streaming para alertas y dashboards en vivo, batch para reportes históricos y modelos de IA.
