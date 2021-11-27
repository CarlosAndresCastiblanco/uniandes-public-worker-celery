from storage import *

class Vistas():
    def broker(self):
        try:
            receive_and_delete_messages_queue()
            print("Realizado:::::::::::::::")
        except:
            print("No se encontro mensaje:::::::::::::::")

    def background(self,conversion):
        try:
            conversion_background(conversion)
            print("Realizado:::::::::::::::")
        except:
            print("No se encontro mensaje:::::::::::::::")
