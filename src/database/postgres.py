from typing import List, Type

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker, declarative_base

from config.logging import logger

Base = declarative_base()


class PostgresDB:
    def __init__(self, user, password, host, port, dbname):
        self.engine = create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        )
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        logger.info("Connected to PostgreSQL database")

    def base_insert(self, data: List, table: Type[Base]) -> None:
        with self.session as session:
            insert_stmt = insert(table).values(data)
            session.execute(insert_stmt)
            session.commit()
