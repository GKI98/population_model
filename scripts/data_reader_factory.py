from scripts.data_reader_csv import CSVReader
from scripts.data_reader_db import DBReader

class DataReaderFactory:
    @staticmethod
    def from_files(folder_path: str):
        return CSVReader(folder_path)

    @staticmethod
    def from_database(db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str, city_id: int):
        return DBReader(db_addr, db_port, db_name, db_user, db_pass, city_id)