import requests  # URL and API
import boto3  # S3
#import psycopg2  # DB
import urllib.parse
from enum import Enum, auto
#from ETLUtilities.Logger import logger


class RepoType(Enum):
    S3 = auto()
    DB = auto()
    API = auto()
    URL = auto()
    # FTP = auto()
    LOCAL = auto()


class ReadType(Enum):
    ITERATOR = auto()
    BULK = auto()


class WriteType(Enum):
    APPEND = auto()
    NEW = auto()


def read(repo_type, location, read_type=None, query=None, username=None, password=None, chunk_size=1024):
    # s3://bucket/key
    if repo_type == RepoType.S3:
        bucket = location.split('/')[2]
        key = '/'.join(location.split('/')[3:])

        s3 = boto3.client('s3', aws_access_key_id=username,
                          aws_secret_access_key=password)
        obj = s3.get_object(Bucket=bucket, Key=key)
        if read_type == ReadType.ITERATOR:
            logger.info("Reading %d chunk size of data from %s" % (chunk_size, location))
            return obj['Body'].iter_chunks(chunk_size=chunk_size)
        elif read_type == ReadType.BULK:
            logger.info("Reading data in bulk from %s" % location)
            return obj['Body'].read()

    # https://
    elif repo_type == RepoType.API:
        session = requests.Session()
        session.hooks = {
            'response': lambda k, *args, **kwargs: k.raise_for_status()
        }
        # can we get away from needing this specifically for the REDDA API, or create new RepoType.REDDAAPI
        # 2019/10/14 decided to go with using reddawrapyr instead
        # r = session.get("https://staging-turbot.redda.celgene.com/api/login", auth=(username, password))

        # need to review authentication for APIs
        if username is not None and password is not None:
            r = session.get(location, auth=(username, password), stream=True)
        else:
            r = session.get(location, stream=True)
        if read_type == ReadType.ITERATOR:
            logger.info("Reading %d chunk size of data from https://%s" % (chunk_size, location))
            return r.iter_content(chunk_size=chunk_size)
        elif read_type == ReadType.BULK:
            logger.info("Reading data in bulk from https://%s" % location)
            return r.content

    # https://
    elif repo_type == RepoType.URL:
        r = requests.get(location, stream=True)
        r.raise_for_status()
        if read_type == ReadType.BULK:
            logger.info("Reading data in bulk from https://%s" % location)
            return r.content
        elif read_type == ReadType.ITERATOR:
            logger.info("Reading %d chunk size of data from https://%s" % (chunk_size, location))
            return r.iter_content(chunk_size=chunk_size)

    # file://
    elif repo_type == RepoType.LOCAL:
        location = location.split('file://')[1]
        file = open(location, 'r', encoding="ISO-8859-1")
        if read_type == ReadType.BULK:
            logger.info("Reading data in bulk from %s" % location)
            return file.read()
        elif read_type == ReadType.ITERATOR:
            def read_chunk():
                return file.read(chunk_size)
            logger.info("Reading %d chunk size of data from %s" % (chunk_size, location))
            return iter(read_chunk, '')

    # postgres://username:password@localhost/mydatabase
    elif repo_type == RepoType.DB:
        username = location.split('/')[2].split(':')[0]
        password = urllib.parse.unquote(location.split('/')[2].split(':')[1].split('@')[0])
        host = location.split('@')[1].split('/')[0]
        db_name = location.split('@')[1].split('/')[1]
        db_conn = psycopg2.connect(host=host, dbname=db_name,
                                   user=username,
                                   password=password)
        cur = db_conn.cursor()
        cur.execute(query)
        logger.info("Reading data from %s" % host)
        return cur

    # no longer doing FTP, remove code after refactor to Client objects
    # elif repo_type == RepoType.FTP:
    #    ftp = FTP(host)
    #    if username is None and password is None:
    #        ftp.login()
    #    else:
    #        ftp.login(username, password)
    #    data = []

    #    def handle_binary(more_data):
    #        data.append(more_data)

    #   if return_type == ReturnType.BULK:
    #        resp = ftp.retrbinary('RETR ' + location, handle_binary)
    #        return data
    #    elif return_type == ReturnType.ITERATOR:
    #        resp = ftp.retrbinary('RETR ' + location, handle_binary, chunk_size)
    #        return data
    return 0


def write(repo_type, location, content=None, query=None, username=None, password=None, write_type=None):
    # s3://bucket/key
    if repo_type == RepoType.S3:
        bucket = location.split('/')[2]
        key = '/'.join(location.split('/')[3:])

        client = boto3.client('s3', aws_access_key_id=username,
                              aws_secret_access_key=password)
        logger.info("Writing data to %s" % location)
        client.put_object(Body=content, Bucket=bucket, Key=key)

    elif repo_type == RepoType.DB:
        username = location.split('/')[2].split(':')[0]
        password = urllib.parse.unquote(location.split('/')[2].split(':')[1].split('@')[0])
        host = location.split('@')[1].split('/')[0]
        db_name = location.split('@')[1].split('/')[1]
        db_conn = psycopg2.connect(host=host, dbname=db_name,
                                   user=username,
                                   password=password)
        cur = db_conn.cursor()
        logger.info("Writing data to %s" % host)
        cur.execute(query)
        cur.close()
        db_conn.commit()
    # file://
    elif repo_type == RepoType.LOCAL:
        location = location.split('file://')[1]
        if write_type == WriteType.NEW:
            logger.info("Writing data to %s" % location)
            file = open(location, 'w')
        elif write_type == WriteType.APPEND:
            logger.info("Appending data to %s" % location)
            file = open(location, 'a')
        file.write(content)
        file.close()
    # TBD
    #elif repo_type == RepoType.API:
    #    return 0
    #elif repo_type == RepoType.URL:
    #    return 0
    #elif repo_type == RepoType.FTP:
    #    return 0

