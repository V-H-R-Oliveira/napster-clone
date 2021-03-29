from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor


class Server(DatagramProtocol):
    def __init__(self):
        self.__peers = set()

    def datagramReceived(self, datagram: bytes, addr):
        datagram = datagram.decode('utf-8')

        print('Received datagram', datagram)

        if '|' in datagram:
            query, content = datagram.split('|')
        else:
            query = datagram

        query = query.upper()

        if query == 'REGISTER':
            self.__register(addr)
        elif query == 'LEAVE':
            self.__leave(addr)
        elif query == 'SEARCH_FILE':
            self.__search_file(content, addr)
        elif query == 'PEER_FILE_SEARCH_RESPONSE':
            self.process_search_response(content, addr)

    def __register(self, addr):
        self.__peers.add(addr)
        addrs = '\n'.join([str(peer) for peer in self.__peers])

        for peer in self.__peers:
            self.transport.write(f'UPDATE_TABLE|{addrs}'.encode('utf-8'), peer)

    def __search_file(self, filename: str, origin):
        for peer in self.__peers:
            if peer == origin:
                continue

            self.transport.write(
                f'SEARCH_FILE_REQUEST|{origin}-{filename}'.encode('utf-8'), peer)

    def process_search_response(self, content, addr):
        origin, filename, status = content.split('-')

        if status == "True":
            self.transport.write(
                f'SEARCH_FILE_RESPONSE|{filename}-{addr}'.encode('utf-8'), eval(origin))

    def __leave(self, addr):
        self.__peers.remove(addr)
        addrs = '\n'.join([str(peer) for peer in self.__peers])

        for peer in self.__peers:
            self.transport.write(f'UPDATE_TABLE|{addrs}'.encode('utf-8'), peer)


if __name__ == '__main__':
    port = 7000
    reactor.listenUDP(port, Server())
    print(f'Running at localhost:{port}')
    reactor.run()
