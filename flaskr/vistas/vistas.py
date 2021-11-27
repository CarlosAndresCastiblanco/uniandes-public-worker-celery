from storage import *

class Vistas():
    def get(self):
        try:
            receive_and_delete_messages_queue()
            print("Realizado:::::::::::::::")
            return '', 200
        except:
            print("No se encontro mensaje:::::::::::::::")
            return '', 404
