from sqlalchemy import Column, String, Integer

from base_maker import Base

# Simplemente creamos el constructor de un objeto utilizando el parser de sqlalchemy
class Article(Base):
    
    # nombre de la tabla en la DB
    __tablename__ = 'articles'

    #Cada tupla es un objeto
    id = Column(String, primary_key=True)
    body = Column(String)
    host = Column(String)
    title = Column(String)
    newspaper_uid = Column(String)
    n_tokens_body = Column(Integer)
    n_tokens_title = Column(Integer)
    url = Column(String, unique=True)

    def __init__(self,
                uid,
                body,
                title,
                url,
                newspaper_uid,
                host,
                title_tokens,
                body_tokens
                ):
        self.id = uid
        self.body = body
        self.host = host
        self.newspaper_uid = newspaper_uid
        self.body_tokens = body_tokens
        self.title_tokens = title_tokens
        self.title = title
        self.url = url
