from pathlib import Path

class Config:
    """
    Configuración para el Sistema de Análisis de Opiniones de Clientes - ETL
    Compatible con SQL Server y autenticación de Windows
    """

    # =============================
    # CONFIGURACIÓN DE BASE DE DATOS
    # =============================
    SERVER = r"localhost"  # tu instancia SQL Server
    DATABASE_NAME = "CustomerOpinions"
    DRIVER = "ODBC Driver 17 for SQL Server"

    # Cadena de conexión para pyodbc/SQLAlchemy
    CONNECTION_STRING = (
        f"mssql+pyodbc://@{SERVER}/{DATABASE_NAME}"
        f"?driver={DRIVER}&trusted_connection=yes"
    )

    # =============================
    # CONFIGURACIÓN DE ARCHIVOS CSV
    # =============================
    DATA_FOLDER = Path.cwd() / "data"  # Carpeta donde están tus CSV

    CSV_FILES = {
        'clientes': 'clients.csv',
        'productos': 'products.csv',
        'fuentes': 'fuente_datos.csv',
        'encuestas': 'surveys_part1.csv',
        'comentarios_sociales': 'social_comments.csv',
        'resenas_web': 'web_reviews.csv'
    }

    # =============================
    # CONFIGURACIÓN DE LOGGING
    # =============================
    LOG_FILE = 'etl_process.log'
    LOG_LEVEL = 'INFO'
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

    # =============================
    # CONFIGURACIÓN DE VALIDACIÓN
    # =============================
    MAX_RETRY_ATTEMPTS = 3
    BATCH_SIZE = 1000

    # =============================
    # CONFIGURACIÓN DE LIMPIEZA DE DATOS
    # =============================
    REMOVE_DUPLICATES = True
    VALIDATE_EMAILS = True
    CLEAN_TEXT_FIELDS = True

    # =============================
    # MAPEO DE COLUMNAS CSV → BD
    # =============================
    COLUMN_MAPPINGS = {
        'clients.csv': {
            'IdCliente': 'IdCliente',
            'Nombre': 'Nombre',
            'Email': 'Email'
        },
        'products.csv': {
            'IdProducto': 'IdProducto',
            'Nombre': 'Nombre',
            'Categoría': 'Categoria'
        },
        'fuente_datos.csv': {
            'IdFuente': 'IdFuente',
            'TipoFuente': 'TipoFuente',
            'FechaCarga': 'FechaCarga'
        },
        'surveys_part1.csv': {
        'IdOpinion': 'IdOpinion',
        'IdCliente': 'IdCliente',
        'IdProducto': 'IdProducto',
        'Fecha': 'Fecha',
        'Comentario': 'Comentario',
        'Clasificacion': 'Clasificacion',       
        'PuntajeSatisfaccion': 'PuntajeSatisfaccion',  
        'FuenteTexto': 'FuenteTexto'                
        },
        'social_comments.csv': {
            'IdComment': 'IdComment',
            'IdCliente': 'IdCliente',
            'IdProducto': 'IdProducto',
            'Fuente': 'FuenteTexto',
            'Fecha': 'Fecha',
            'Comentario': 'Comentario'
        },
        'web_reviews.csv': {
            'IdReview': 'IdReview',
            'IdCliente': 'IdCliente',
            'IdProducto': 'IdProducto',
            'Fecha': 'Fecha',
            'Comentario': 'Comentario',
            'Rating': 'Rating'
        }
    }

    # =============================
    # MÉTODOS AUXILIARES
    # =============================
    @staticmethod
    def ensure_data_folder():
        """Crear la carpeta 'data' si no existe"""
        Config.DATA_FOLDER.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def get_csv_path(csv_file_name):
        """Retorna la ruta completa del CSV"""
        return Config.DATA_FOLDER / csv_file_name

    @staticmethod
    def validate_csv_files():
        """Valida que todos los CSV existan"""
        missing_files = []
        for csv_name in Config.CSV_FILES.values():
            if not Config.get_csv_path(csv_name).exists():
                missing_files.append(csv_name)
        return missing_files
