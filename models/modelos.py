from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship
import os
"""
Config Enviroment
"""
uri_database = os.getenv('SQLALCHEMY_DATABASE_URI')
"""
Launch DataBase
"""
engine = create_engine("mysql+pymysql://admin:12345678@converter.cd0qbrcafg8c.us-east-1.rds.amazonaws.com:3306/converter", echo=True)
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

def find_conversion():
    return session.query(Conversion).filter(Conversion.estado.in_(['uploaded'])).all()

def find_conversion_each_in_progress():
    return session.query(Conversion).filter(Conversion.estado.in_(['uploaded'])).first()

def update_processed(id_conversion):
    c = session.query(Conversion).filter_by(id=id_conversion).first()
    c.estado = "processed"
    session.commit()

