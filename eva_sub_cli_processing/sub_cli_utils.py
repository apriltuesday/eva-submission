import requests
from ebi_eva_common_pyutils.config import cfg
from retry import retry

# Submission statuses
OPEN = 'OPEN'
UPLOADED = 'UPLOADED'
COMPLETED = 'COMPLETED'
TIMEOUT = 'TIMEOUT'
FAILED = 'FAILED'
CANCELLED = 'CANCELLED'
PROCESSING = 'PROCESSING'

# Processing steps
VALIDATION = 'VALIDATION'
BROKERING = 'BROKERING'
INGESTION = 'INGESTION'
PROCESSING_STEPS = [VALIDATION, BROKERING, INGESTION]

# Processing statuses
READY_FOR_PROCESSING = 'READY_FOR_PROCESSING'
FAILURE = 'FAILURE'
SUCCESS = 'SUCCESS'
RUNNING = 'RUNNING'
ON_HOLD = 'ON_HOLD'
PROCESSING_STATUS = [READY_FOR_PROCESSING, FAILURE, SUCCESS, RUNNING, ON_HOLD]


def sub_ws_auth():
    return (
        cfg.query('submissions', 'webservice', 'admin_username'),
        cfg.query('submissions', 'webservice', 'admin_password')
    )


def sub_ws_url_build(*args, **kwargs):
    url = cfg.query('submissions', 'webservice', 'url') + '/' + '/'.join(args)
    if kwargs:
        query_params = []
        for k, v in kwargs.items():
            if isinstance(v, list):
                query_params.extend([f'{k}={v2}' for v2 in v])
            else:
                query_params.append(f'{k}={v}')
        return url + '?' + '&'.join(query_params)
    else:
        return url


@retry(tries=5, backoff=2, jitter=.5)
def get_from_sub_ws(url):
    response = requests.get(url, auth=sub_ws_auth())
    response.raise_for_status()
    return response.json()


@retry(tries=5, backoff=2, jitter=.5)
def put_to_sub_ws(url, json_data=None):
    response = requests.put(url, auth=sub_ws_auth(), json=json_data)
    response.raise_for_status()
    if not response.text:
        return None
    return response.json()


def fetch_submission_from_eload(eload_id):
    response = get_from_sub_ws(sub_ws_url_build('admin', 'submissions', eloadId=eload_id, size=1))
    content = response.get('content', [])
    if not content:
        return None
    return content[0]


def fetch_submission(submission_id):
    response = get_from_sub_ws(sub_ws_url_build('admin', 'submissions', submissionId=submission_id, size=1))
    content = response.get('content', [])
    if not content:
        return None
    return content[0]
