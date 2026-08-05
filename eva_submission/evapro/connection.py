from urllib.parse import urlsplit

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_internal_pyutils.config_utils import get_metadata_creds_for_profile
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


def get_evapro_engine():
    pg_url, pg_user, pg_pass = get_metadata_creds_for_profile(cfg['maven']['environment'], cfg['maven']['settings_file'])
    dbtype, host_url, port_and_db = urlsplit(pg_url).path.split(':')
    port, db = port_and_db.split('/')
    return create_engine(URL.create(
        'postgresql+psycopg2',
        username=pg_user,
        password=pg_pass,
        host=host_url.split('/')[-1],
        database=db,
        port=int(port)
    ))
