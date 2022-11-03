from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models import Base


def create_tables():
    SQLALCHEMY_DATABASE_URL = "postgresql://root:root@localhost:5432/sicredi_data_challenge"
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    Base.metadata.create_all(engine)

