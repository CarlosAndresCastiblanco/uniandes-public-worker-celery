from storage import *

class Vistas():
    def get(self):
        try:
            result = receive_and_delete_messages_queue()
            print("Realizado:::::::::::::::")
            return result
        except:
            print("No se encontro mensaje:::::::::::::::")
            return '', 404
