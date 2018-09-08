import gettext
import yaml
import os
from celery import Celery

env = os.environ
CELERY_BROKER_URL = (env.get("CELERY_BROKER_URL", "redis://localhost:6379"),)
CELERY_RESULT_BACKEND = env.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

EXTRACT_SERVICE_LOCALE_DIR = env.get(
    "EXTRACT_SERVICE_LOCALE_DIR", "../resources/locale"
)
EXTRACT_SERVICE_LANGUAGE = env.get("EXTRACT_SERVICE_LANGUAGE", None)

EXTRACT_SERVICE_CONF = env.get("EXTRACT_SERVICE_CONF", "/etc/extract_service_conf.yml")

# Install gettext support
if EXTRACT_SERVICE_LANGUAGE:
    language = gettext.translation(
        "extractor",
        localedir=EXTRACT_SERVICE_LOCALE_DIR,
        languages=[EXTRACT_SERVICE_LANGUAGE],
    )
    language.install()
else:
    gettext.install("extractor")


def get_service_conf():
    try:
        return yaml.load(open(EXTRACT_SERVICE_CONF).read())
    except:
        print("Cannot load %s" % EXTRACT_SERVICE_CONF)
        return None


service_conf = get_service_conf()
if service_conf is not None:
    CELERY_BROKER_URL = service_conf.get("celery_broker_url", CELERY_BROKER_URL)
    CELERY_RESULT_BACKEND = service_conf.get(
        "celery_result_backend", CELERY_RESULT_BACKEND
    )

taskmanager = Celery(
    "extractions", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND
)
