import os
from os.path import abspath, dirname, join

### MYSQL DATABASE ###
MYSQL_HOST = ""
MYSQL_DB = ""
MYSQL_USER = ""
MYSQL_PASSWORD = ""

MYSQL_HOST = os.environ.get('MYSQL_HOST') or "loanproductivo-cluster.cluster-cgzlfaeupg9q.us-east-1.rds.amazonaws.com"
MYSQL_DB = os.environ.get('MYSQL_DB') or "infobcra"
MYSQL_USER = os.environ.get('MYSQL_USER') or "jvasquez"
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD') or "Boeing737"

SQLALCHEMY_DATABASE_BCRA = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}'

#SQLALCHEMY_DATABASE_BCRA = f'mysql+pymysql://{"jvasquez"}:{"Boeing737"}@{"loanproductivo-cluster.cluster-cgzlfaeupg9q.us-east-1.rds.amazonaws.com"}'

