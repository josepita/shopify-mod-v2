"""
Clase base para la conexión con MySQL
"""
import mysql.connector
from mysql.connector import Error
from typing import Optional, Dict, Any, List
import logging
from config.settings import MYSQL_CONFIG

class MySQLConnector:
    """Clase base para manejar conexiones MySQL"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Inicializa la conexión a MySQL
        
        Args:
            config (Dict[str, Any]): Configuración de conexión MySQL
        """
        self.config = config
        self.connection = None
        self._connect()

    def _connect(self) -> None:
        """Establece la conexión con MySQL"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                logging.info("Conectado a MySQL exitosamente")
                db_info = self.connection.get_server_info()
                logging.info(f"MySQL server version: {db_info}")
        except Error as e:
            logging.error(f"Error conectando a MySQL: {e}")
            raise

    def execute_query(self, query: str, params: tuple = None, fetch: bool = False) -> Optional[List[Dict]]:
        """
        Ejecuta una consulta SQL
        
        Args:
            query (str): Consulta SQL a ejecutar
            params (tuple, optional): Parámetros para la consulta
            fetch (bool): Si debe devolver resultados
            
        Returns:
            Optional[List[Dict]]: Resultados de la consulta si fetch=True
        """
        if not self.connection or not self.connection.is_connected():
            self._connect()

        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params or ())
            
            if fetch:
                return cursor.fetchall()
            else:
                self.connection.commit()
                return None
                
        except Error as e:
            self.connection.rollback()
            logging.error(f"Error ejecutando query: {e}")
            logging.error(f"Query: {query}")
            logging.error(f"Params: {params}")
            raise
        finally:
            cursor.close()

    def execute_many(self, query: str, params: list) -> None:
        """
        Ejecuta una consulta SQL múltiples veces con diferentes parámetros
        
        Args:
            query (str): Consulta SQL a ejecutar
            params (list): Lista de tuplas con parámetros
        """
        if not self.connection or not self.connection.is_connected():
            self._connect()

        cursor = self.connection.cursor()
        try:
            cursor.executemany(query, params)
            self.connection.commit()
        except Error as e:
            self.connection.rollback()
            logging.error(f"Error en execute_many: {e}")
            logging.error(f"Query: {query}")
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        """Cierra la conexión a MySQL"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("Conexión a MySQL cerrada")

    def __enter__(self):
        """Soporte para context manager (with statement)"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cierra la conexión al salir del context manager"""
        self.close()