from flask_restful import Resource

from flaskr.storage.storage import *

class Vistas():
    def get(self):
        try:
            receive_and_delete_messages_queue()
            return '', 200
        except:
            return '', 404
