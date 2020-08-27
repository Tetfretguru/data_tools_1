import argparse
import hashlib

import nltk
from nltk.corpus import stopwords

import logging
logging.basicConfig(level=logging.INFO)

from urllib.parse import urlparse
import pandas as pd
import re

logger = logging.getLogger(__name__)

def _save_data(df, filename):
    clean_filename = 'clean_{}'.format(filename)
    logger.info('Saving data at location: {}'.format(clean_filename))
    df.to_csv(clean_filename)

def _drop_rows_with_missing_values(df):
    logger.info('Dropping rows with missing data')
    return df.dropna()

def _remove_duplicated_entries(df, column_name):
    logger.info('Removing duplicated entries')
    df.drop_duplicates(subset=['title'], keep='first', inplace=True)

    return df

# Data enrichment with nltk module
def _tokenize_main_headers(df):
    stop_words = set(stopwords.words('spanish'))

    def __tokenize_column(df, column_name):
        return (df
                    .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1)
                    .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
                    .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
                    .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
                    .apply(lambda ok_words_list: len(ok_words_list))
                )

    df['title_tokens'] = __tokenize_column(df, 'title')
    df['body_tokens'] = __tokenize_column(df, 'body')

    return df 
def _body_cleanup(df):
	logger.info('Removing new lines from body')
	
	stripped_body = (df
                     .apply(lambda row: row['body'], axis=1)
                     .apply(lambda body: list(body))
                     .apply(lambda letters: list(map(lambda letter: letter.replace('\n', ''), letters)))
                     .apply(lambda letters: ''.join(letters))   
                )
	df['body'] = stripped_body
	return df

def _gen_uids_for_rows(df):
	logger.info('Generating uids for each row')
	uids = (df
				.apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
				.apply(lambda hash_object: hash_object.hexdigest())
		)
	df['uid'] = uids
	
	return df.set_index('uid')

def _fill_missing_titles(df):
	
	# En este caso, el url nos da el título

	logger.info('Filling missing titles')
	missing_titles_mask = df['title'].isna()

	missing_titles = (df[missing_titles_mask]['url']
						.str.extract(r'(?P<missing_titles>[^/]+)$')
						.applymap(lambda title: title.split('-'))
						.applymap(lambda title_word_list: ' '.join(title_word_list))
						)

	df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']

	return df

def _extract_host(df):
    logger.info('Extracting host from urls')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)

    return df

def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info('Filling newspaper_uid column with {}'.format(newspaper_uid))    
    df['newspaper_uid'] = newspaper_uid

    return df

def _extract_newspaper_uid(filename):
    logger.info('Extracting newspaper uid')
    newspaper_uid = filename.split('_')[0]

    logger.info('Newspaper uid detected: {}'.format(newspaper_uid))

    return newspaper_uid

def _read_data(filename):
    logger.info('Reading file {}'.format(filename))

    return pd.read_csv(filename)

def main(filename):
    logger.info('Starting cleaning process')

    df = _read_data(filename)
    newspaper_uid = _extract_newspaper_uid(filename)
    df = _add_newspaper_uid_column(df, newspaper_uid)
    df = _extract_host(df)
    df = _fill_missing_titles(df)
    df = _gen_uids_for_rows(df)
    df = _body_cleanup(df)
    df = _tokenize_main_headers(df)
    df = _remove_duplicated_entries(df, 'title')
    df = _drop_rows_with_missing_values(df)
    _save_data(df, filename)
    
    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='The path to the dirty data',type=str)
    args = parser.parse_args()
    filename = args.filename
    
    df = main(filename)
    print(df)