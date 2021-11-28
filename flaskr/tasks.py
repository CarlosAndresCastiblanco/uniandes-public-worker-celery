from celery import Celery
from celery.utils.log import get_task_logger
from pathlib import Path
from vistas import Vistas
from models import *

appC = Celery('tasks', broker='redis://127.0.0.1:6379/0')
logger = get_task_logger(__name__)
PATH = str(Path().absolute(), )

appC.conf.beat_schedule = {
    'add-every-10-seconds': {
        'task': 'tasks.add',
        'schedule': 10,
    },
    'add-every-30-seconds': {
        'task': 'tasks.back',
        'schedule': 30,
    },
}
appC.conf.timezone = 'UTC'

@appC.task(name="tasks.add")
def test():
    vistas = Vistas()
    vistas.broker()

@appC.task(name="tasks.back")
def background():
    vistas = Vistas()
    obc = find_conversion_each_in_progress()
    if obc != None:
        vistas.background(obc)
    else:
        print('No hay objetos en background')