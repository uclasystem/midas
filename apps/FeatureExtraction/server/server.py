from PIL import Image
import socketserver
import threading

from vision_model_loader import Img2Vec

# model = Img2Vec(model='efficientnet_b5', cuda=True)
model = Img2Vec(model='efficientnet_b0', cuda=True)


def extractFeat(img_name):
    img = Image.open(img_name).convert('RGB')
    feat = model.get_vec(img)

    return feat


class TCPSocketHandler(socketserver.BaseRequestHandler):
    """
    The RequestHandler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = ""
        remainder = ""
        while str(self.data) != "end":
            self.data = self.request.recv(1024).strip()
            # print("{} wrote:".format(self.client_address[0]))
            # print(self.data)

            raw_data = remainder + self.data.decode('ascii')
            pos = raw_data.find('.jpg')
            while pos != -1:
                filename = raw_data[:pos+len('.jpg')]
                # print(filename)
                extractFeat(filename)
                # just send back the same data, but upper-cased
                self.request.send(b'feature_vec')
                raw_data = raw_data[pos+len('.jpg'):]
                pos = raw_data.find('.jpg')
            remainder = raw_data
    # def handle(self):
    #     # self.request is the TCP socket connected to the client
    #     self.data = self.request.recv(1024).strip()
    #     print("{} wrote:".format(self.client_address[0]))
    #     print(self.data)

    #     filename = self.data
    #     print(filename)
    #     extractFeat(filename)
    #     # just send back the same data, but upper-cased
    #     self.request.sendall(self.data[:1])


def SimpleServer(host="localhost", port=10080):
    server = socketserver.TCPServer((host, port), TCPSocketHandler)
    print("Server is listening at {}:{}".format(host, port))
    server.serve_forever()


def ThreadedServer(host="localhost", port=10080):
    server = socketserver.ThreadingTCPServer((host, port), TCPSocketHandler)
    print("Server is listening at {}:{}".format(host, port))
    with server:
        server.serve_forever()
        # server_thread = threading.Thread(target=server.serve_forever)
        # # Exit the server thread when the main thread terminates
        # # server_thread.daemon = True
        # server_thread.start()
        # print("Server loop running in thread:", server_thread.name)

        # server_thread.join()
        # server.shutdown()


if __name__ == '__main__':
    ThreadedServer()
