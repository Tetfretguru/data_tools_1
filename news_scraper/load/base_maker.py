from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#sqllite crea una db sin necesidad de tener un DB manager
engine = create_engine('sqlite:///newspaper.db') 

# una sesión o conección
Session = sessionmaker(bind=engine)

#db schema
Base = declarative_base()
