import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine, text

from config import Config  # CONFIG.PY


class CustomerOpinionETL:
    def __init__(self):
        self.connection_string = Config.CONNECTION_STRING
        self.data_folder = Config.DATA_FOLDER
        self.engine = None
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=getattr(logging, Config.LOG_LEVEL),
            format=Config.LOG_FORMAT,
            handlers=[
                logging.FileHandler(Config.LOG_FILE, encoding="utf-8"),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger(__name__)

    def connect_db(self):
        """Conectar a SQL Server con SQLAlchemy"""
        try:
            self.engine = create_engine(self.connection_string)
            with self.engine.connect() as conn:
                self.logger.info(f"Conexión establecida con {Config.SERVER}/{Config.DATABASE_NAME}")
            return True
        except Exception as e:
            self.logger.error(f"Error de conexión: {str(e)}")
            return False

    def extract_data(self):
        """Lectura de archivos CSV definidos en config.py"""
        extracted_data = {}
        for table_name, file_name in Config.CSV_FILES.items():
            file_path = Config.get_csv_path(file_name)
            if file_path.exists():
                df = pd.read_csv(file_path, encoding="utf-8")
                self.logger.info(f"{file_name} leído: {len(df)} registros")
                extracted_data[table_name] = df
            else:
                self.logger.warning(f"No encontrado: {file_path}")
        return extracted_data

    def transform_data(self, raw_data):
        """Transformaciones simples y renombrado de columnas según COLUMN_MAPPINGS"""
        transformed_data = {}
        for table_name, df in raw_data.items():
            df_clean = df.copy()

            # Limpieza general
            if Config.REMOVE_DUPLICATES:
                df_clean = df_clean.drop_duplicates().dropna(how="all")

            # Normalización básica
            if Config.CLEAN_TEXT_FIELDS:
                if "Email" in df_clean.columns and Config.VALIDATE_EMAILS:
                    df_clean["Email"] = df_clean["Email"].str.strip().str.lower()
                if "Nombre" in df_clean.columns:
                    df_clean["Nombre"] = df_clean["Nombre"].str.strip().str.title()

            # Renombrar columnas según COLUMN_MAPPINGS
            csv_file = Config.CSV_FILES[table_name]
            if csv_file in Config.COLUMN_MAPPINGS:
                mapping = Config.COLUMN_MAPPINGS[csv_file]
                df_clean = df_clean.rename(columns=mapping)

            transformed_data[table_name] = df_clean

        return transformed_data

    def filter_foreign_keys(self, df, fk_rules):
        """
        Filtra un DataFrame según reglas de llave foránea.
        fk_rules = {'columna_df': 'tabla_bd.columna_bd'}
        """
        with self.engine.connect() as conn:
            for df_col, table_col in fk_rules.items():
                table, col = table_col.split('.')
                valid_values = pd.read_sql(f"SELECT {col} FROM {table}", conn)
                df = df[df[df_col].isin(valid_values[col])]
        return df

    def load_data(self, transformed_data):
        """Carga en SQL Server usando pandas.to_sql"""
        load_results = {}
        try:
            for table, df in transformed_data.items():
                if len(df) == 0:
                    self.logger.warning(f"No hay datos para {table}")
                    load_results[table] = 0
                    continue

                # Filtrado de llaves foráneas según tabla
                if table == 'encuestas':
                    fk_rules = {'IdCliente': 'clientes.IdCliente', 'IdProducto': 'productos.IdProducto'}
                    df = self.filter_foreign_keys(df, fk_rules)
                elif table == 'comentarios_sociales':
                    fk_rules = {'IdCliente': 'clientes.IdCliente', 'IdProducto': 'productos.IdProducto'}
                    df = self.filter_foreign_keys(df, fk_rules)
                elif table == 'resenas_web':
                    fk_rules = {'IdCliente': 'clientes.IdCliente', 'IdProducto': 'productos.IdProducto'}
                    df = self.filter_foreign_keys(df, fk_rules)

                df.to_sql(table, self.engine, if_exists="append", index=False)
                load_results[table] = len(df)
                self.logger.info(f"{len(df)} registros cargados en {table}")

            return load_results
        except Exception as e:
            self.logger.error(f"Error en carga: {str(e)}")
            return False

    def verify_data_load(self):
        """Muestra conteos básicos por tabla"""
        try:
            with self.engine.connect() as conn:
                print("\n===== RESUMEN DE CARGA =====")
                for table in Config.CSV_FILES.keys():
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()[0]
                    print(f"{table}: {count} registros")
                print("============================")
            return True
        except Exception as e:
            self.logger.error(f"Error en verificación: {str(e)}")
            return False

    def run_etl_process(self):
        start_time = datetime.now()
        self.logger.info("=== INICIO PROCESO ETL ===")

        if not self.connect_db():
            return False

        raw_data = self.extract_data()
        if not raw_data:
            return False

        transformed_data = self.transform_data(raw_data)
        load_results = self.load_data(transformed_data)
        if not load_results:
            return False

        self.verify_data_load()

        end_time = datetime.now()
        self.logger.info(f"ETL completado en {end_time - start_time}")
        return True


def main():
    print("Sistema de Análisis de Opiniones de Clientes - ETL SQL Server")
    etl = CustomerOpinionETL()
    if etl.run_etl_process():
        print("ETL completado")
    else:
        print("Error en ETL")


if __name__ == "__main__":
    main()
