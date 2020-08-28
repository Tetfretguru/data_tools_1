# The OOP paradigm is esential for data loading, and it makes it easier
import argparse
import logging
logging.basicConfig(level=logging.INFO)

# pandas para poder leer csv facilmente
import pandas as pd

# Traemos la clase
from article_mkr import Article
from base_maker import Base, engine, Session

logger = logging.getLogger(__name__)


def main(filename):
    # create engine for db schema
    Base.metadata.create_all(engine)
    # open session
    session = Session()
    articles = pd.read_csv(filename)

    # iterate though dataset's rows
    for index, row in articles.iterrows():
        logger.info('Loading article uid {} into DB'.format(row['uid']))
        # now each artice becomes a Object
        article = Article(row['uid'], 
                          row['body'],
                          row['title'],
                          row['url'],
                          row['newspaper_uid'],
                          row['host'],
                          row['title_tokens'],
                          row['body_tokens']
                         )

        session.add(article)

    session.commit()
    session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The file you want to load into the db',
                        type=str)

    args = parser.parse_args()

    main(args.filename)