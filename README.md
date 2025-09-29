# ETL-Code-Python.-Proceso-de-ETL-Lectura-y-procesamiento-de-archivo-CSV-

Sistema de Análisis de Opiniones de Clientes - ETL

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) en Python para cargar datos de encuestas, comentarios sociales y reseñas web en SQL Server. El objetivo es centralizar la información de clientes, productos y fuentes, permitiendo realizar análisis posteriores de satisfacción y opiniones. Flujo del ETL Extracción: Se leen archivos CSV ubicados en la carpeta data/ usando Pandas. Transformación: Eliminación de duplicados. Normalización de correos y nombres. Renombrado de columnas para coincidir con la base de datos. Carga: Los datos se insertan en tablas SQL Server mediante SQLAlchemy, respetando claves primarias y foráneas. Modelo de Datos clientes y productos se relacionan con encuestas, comentarios_sociales y resenas_web (relaciones 1:N). fuentes se registra como tabla independiente y es referenciada en el texto de las encuestas. Evidencia de Ejecución Se incluyen: Script SQL de creación de tablas y relaciones. Pipeline ETL en Python (etl_pipeline.py). Capturas de conteo de registros y muestras de datos cargados. Diagrama ER que resume las relaciones entre entidades.

## Diagrama de Base de Datos
```mermaid
erDiagram
    PRODUCTOS {
        INT IdProducto PK
        NVARCHAR Nombre
        NVARCHAR Categoria
        DATETIME fecha_creacion
    }
    
    CLIENTES {
        INT IdCliente PK
        NVARCHAR Nombre
        NVARCHAR Email UK
        DATETIME fecha_creacion
    }
    
    ENCUESTAS {
        INT IdOpinion PK
        INT IdCliente FK
        INT IdProducto FK
        DATE Fecha
        NVARCHAR Comentario
        NVARCHAR Clasificacion
        TINYINT PuntajeSatisfaccion
        NVARCHAR FuenteTexto
        DATETIME fecha_creacion
    }
    
    RESENAS_WEB {
        NVARCHAR IdReview PK
        INT IdCliente FK
        INT IdProducto FK
        DATE Fecha
        NVARCHAR Comentario
        TINYINT Rating
        DATETIME fecha_creacion
    }
    
    COMENTARIOS_SOCIALES {
        NVARCHAR IdComment PK
        INT IdCliente FK
        INT IdProducto FK
        NVARCHAR FuenteTexto
        DATE Fecha
        NVARCHAR Comentario
        DATETIME fecha_creacion
    }
    
    FUENTES {
        NVARCHAR IdFuente PK
        NVARCHAR TipoFuente
        DATE FechaCarga
        DATETIME fecha_creacion
    }
    
    PRODUCTOS ||--o{ ENCUESTAS : "incluye_en"
    CLIENTES ||--o{ ENCUESTAS : "responde"
    PRODUCTOS ||--o{ RESENAS_WEB : "recibe"
    CLIENTES ||--o{ RESENAS_WEB : "escribe"
    PRODUCTOS ||--o{ COMENTARIOS_SOCIALES : "menciona_en"
    CLIENTES ||--o{ COMENTARIOS_SOCIALES : "publica"
