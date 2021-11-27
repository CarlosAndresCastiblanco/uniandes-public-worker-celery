from celery import Celery
from celery.utils.log import get_task_logger
from pathlib import Path
from storage import *
from vistas import Vistas
from models import *

appC = Celery('tasks', broker='redis://127.0.0.1:6379/0')
logger = get_task_logger(__name__)
PATH = str(Path().absolute(), )

appC.conf.beat_schedule = {
    'add-every-60-seconds': {
        'task': 'tasks.add',
        'schedule': 60,
    },
}
appC.conf.timezone = 'UTC'

@appC.task(name="tasks.add")
def test():
    vistas = Vistas()
    result = find_conversion()
    print('size query............. '+str(len(result)))
    r = vistas.get()
    print("r______"+str(r))
    update_processed(r)
    """
    for row in result:
        print('row.................... ',row)
        if row.estado == 'uploaded':
            try:
                if find_object('uniandes-bucket-s3', 'us-east-1',
                               "origin-{}-{}.{}".format(row.usuario_id, row.id, row.origen)):
                    downloading_files(
                        'originales/{}'.format("origin-{}-{}.{}".format(row.usuario_id, row.id, row.origen)),
                        'uniandes-bucket-s3',
                        "origin-{}-{}.{}".format(row.usuario_id, row.id, row.origen),
                        'us-east-1'
                    )
                    archivo = AudioSegment.from_file(
                        "originales/origin-{}-{}.{}".format(row.usuario_id, row.id, row.origen),
                        str(row.origen))
                    archivo.export(
                        "originales/destino-{}-{}.{}".format(row.usuario_id, row.id, row.destino),
                        format=row.destino)
                    print('convertido satisfactoriamente',
                          "destino-{}-{}.{}".format(row.usuario_id, row.id, row.destino))
                    upload_file("originales/destino-{}-{}.{}".format(row.usuario_id, row.id, row.destino),
                                'uniandes-bucket-s3',
                                "destino-{}-{}.{}".format(row.usuario_id, row.id, row.destino),
                                'us-east-1')
                    remove_file("originales/destino-{}-{}.{}".format(row.usuario_id, row.id, row.destino))
                    row.estado = "processed"
                    session.commit()
                else:
                    print("Archivo no encontrado en S3")
            except Exception as err:
                print('error convirtiendo')
                print(err)
                print(err.args)
    """