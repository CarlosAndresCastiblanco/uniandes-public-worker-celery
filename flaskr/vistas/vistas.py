from storage import *

class Vistas():
    def get(self):
        try:
            receive_and_delete_messages_queue()
            print("Realizado:::::::::::::::")
        except:
            print("No se encontro mensaje:::::::::::::::")
