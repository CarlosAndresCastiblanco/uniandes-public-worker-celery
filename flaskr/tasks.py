from datetime import datetime
from celery import Celery
from celery.utils.log import get_task_logger
from pydub import AudioSegment
import os
from pathlib import Path
from celery.schedules import crontab
from datetime import timedelta
from pydub.utils import which
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.sqltypes import DateTime
import smtplib
from storage import *
from vistas import Vistas

"""
Config Enviroment
"""
uri_database = os.getenv('SQLALCHEMY_DATABASE_URI')
"""
Launch DataBase
"""
engine = create_engine(uri_database, echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()
"""
Config Entities DataBase
"""
class Usuario(Base):
    __tablename__ = 'usuario'
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True)
    email = Column(String(50), unique=True)
    password = Column(String(50))
    conversiones = relationship("Conversion")

class Conversion(Base):
    __tablename__ = 'conversion'
    id = Column(Integer, primary_key=True)
    nombre = Column(String(50))
    origen = Column(String(50))
    destino = Column(String(50))
    estado = Column(String(50))
    fecha = Column(String(50))
    usuario_id = Column(Integer, ForeignKey('usuario.id'))

AudioSegment.converter = which("ffmpeg")
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
    result = session.query(Conversion).filter(Conversion.estado.in_(['uploaded'])).all()
    print('size query............. ',len(result))
    vistas.get()
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