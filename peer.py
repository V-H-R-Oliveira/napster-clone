from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from twisted.python import threadable
from random import randint
from pathlib import Path
from functools import partial
from operator import itemgetter
from sys import stderr
from datetime import datetime

threadable.init()


PUBLIC_FOLDER = Path('shared_files')


class Client(DatagramProtocol):
    def __init__(self, host: str = '127.0.0.1', port: int = 5000):
        self.__id = (host, port)
        self.__server = ('127.0.0.1', 7000)
        self.__peers = set()
        self.__public_folder = PUBLIC_FOLDER
        self.__file_structure = {}

    def startProtocol(self):
        self.transport.write("REGISTER".encode('utf-8'), self.__server)

    def stopProtocol(self):
        print(
            f'\n[{self.__id[0]}:{self.__id[1]}: {str(datetime.today())}] Bye !!!')

    def connectionRefused(self):
        print(
            f'[{self.__id[0]}:{self.__id[1]}: {str(datetime.today())}] Servidor está offline', file=stderr)

    def datagramReceived(self, datagram: bytes, addr):
        datagram = datagram.decode('utf-8')

        print(
            f'[{self.__id[0]}:{self.__id[1]}: {str(datetime.today())}] Received datagram', datagram)

        if '|' in datagram:
            query, content = datagram.split('|', maxsplit=1)
        else:
            query = datagram

        query = query.upper()

        if query == 'UPDATE_TABLE':
            self.__update(content)
        elif query == 'SEARCH_FILE_REQUEST':
            self.__search_requested_file(content)
        elif query == 'SEARCH_FILE_RESPONSE':
            self.__request_file_from_peer(content)
        elif query == 'SEND_FILE':
            self.__send_file_to_peer(content, addr)
        elif query == 'DOWNLOAD_FILE':
            self.__receive_file_from_peer(content)
        elif query == 'FINISH_UPLOAD':
            self.__save_file(content, addr)
        elif query == 'DOWNLOAD_COMPLETED':
            self.__get_download_status(content, addr)

    def user_input(self):
        while True:
            try:
                file = input(
                    '::> Digite o nome do arquivo que está procurando:')
                evt = f'SEARCH_FILE|{file}'.encode('utf-8')
                self.transport.write(evt, self.__server)
            except Exception:
                self.transport.write('LEAVE'.encode('utf-8'), self.__server)
                return

    def __search_requested_file(self, message: str):
        exists = False
        origin, filename = message.split('-')

        for file in self.__public_folder.iterdir():
            if file.name == filename:
                exists = True
                break

        self.transport.write(
            f'PEER_FILE_SEARCH_RESPONSE|{origin}-{filename}-{exists}'.encode('utf-8'), self.__server)

    def __request_file_from_peer(self, content):
        filename, peer_ip = content.split('-')

        self.transport.write(
            f'SEND_FILE|{filename}'.encode('utf-8'), eval(peer_ip))

    def __send_file_to_peer(self, filename, addr):
        with open(PUBLIC_FOLDER/filename, 'rb') as f:
            for i, chunk in enumerate(iter(partial(f.read, 1024), b''), start=1):
                self.transport.write(
                    f'DOWNLOAD_FILE|{filename}-{i}-{chunk}'.encode('utf-8'), addr)

        self.transport.write(f'FINISH_UPLOAD|{filename}'.encode('utf-8'), addr)

    def __receive_file_from_peer(self, content):
        filename, idx, chunk = content.split('-', maxsplit=2)
        chunk = eval(chunk)

        if not filename in self.__file_structure:
            self.__file_structure[filename] = []

        self.__file_structure[filename].append((int(idx), chunk))

    def __save_file(self, filename, addr):
        file_chunks = sorted(self.__file_structure.get(
            filename), key=itemgetter(0))
        content = b''

        for _, chunk in file_chunks:
            content += chunk

        with open(PUBLIC_FOLDER/filename, 'wb') as f:
            f.write(content)

        del self.__file_structure[filename]

        self.transport.write(
            f'DOWNLOAD_COMPLETED|{filename}'.encode('utf-8'), addr)

    def __get_download_status(self, filename, addr):
        print(f'[{self.__id[0]}:{self.__id[1]}: {str(datetime.today())}] Arquivo {filename} recebido com sucesso em {addr}')

    def __update(self, peer_list: str):
        addrs = peer_list.splitlines()

        for addr in addrs:
            addr = eval(addr)

            if self.__id == addr:
                continue

            self.__peers.add(addr)


if __name__ == '__main__':
    if not PUBLIC_FOLDER.exists():
        print(f'Pasta compartilhada foi criada com sucesso.')
        PUBLIC_FOLDER.mkdir()

    port = randint(5000, 6000)
    client = Client(port=port)

    reactor.listenUDP(port, client)
    reactor.callInThread(client.user_input)
    print(f'Running peer at {port}')
    reactor.run()
