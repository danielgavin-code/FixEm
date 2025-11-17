import logging
import socket
import threading
from datetime import datetime
from emulator.messageUtils import BuildFixMessage, ParseFixMessage

SOH = '\x01'

class FixEmulatorServer:

    def __init__(self, host, port, senderCompID, targetCompID, heartBtInt=30):
        self.host = host
        self.port = port
        self.senderCompID = senderCompID
        self.targetCompID = targetCompID
        self.heartBtInt = heartBtInt
        self.serverSocket = None

    def Start(self):

        print(f"[INFO] Starting FIX Emulator on {self.host}:{self.port}")
        logging.info(f"Starting FIX Emulator on {self.host}:{self.port}") 

        self.serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serverSocket.bind((self.host, self.port))
        self.serverSocket.listen(1)
        print("[INFO] Waiting for incoming FIX connection...")
        logging.info("Waiting for incoming FIX connection...")

        while True:
            clientSocket, addr = self.serverSocket.accept()
            print(f"[INFO] Connection established from {addr}")
            print(f"Connection established from {addr}")
            thread = threading.Thread(target=self.HandleClient, args=(clientSocket,))
            thread.start()

    def HandleClient(self, clientSocket):
        buffer = ""

        while True:
            data = clientSocket.recv(4096)
            if not data:
                break

            buffer += data.decode("utf-8")

            while SOH in buffer:
                message, sep, buffer = buffer.partition(SOH * 2)  # crude message separator
                fixFields = ParseFixMessage(message + SOH)
                if not fixFields:
                    continue

                msgType = fixFields.get("35")

                if msgType == "A":

                    logging.info("--- Login request ---")
                    logging.info(f"{"> " + message.replace(SOH, '|')}")

                    response = self.BuildLogonResponse(fixFields)
                    clientSocket.sendall(response.encode("utf-8"))

                    logging.info("--- Login response ---")
                    logging.info(f"{"< " + response.replace(SOH, '|')}")

                elif msgType == "0":

                    logging.info("--- Heartbeat ---")
                    logging.info(f"{"> " + message.replace(SOH, '|')}")

                    response = self.BuildHeartbeatResponse(fixFields)
                    clientSocket.sendall(response.encode("utf-8"))

                    logging.info("--- Heartbeat ---")
                    logging.info(f"{"< " + response.replace(SOH, '|')}")

                elif msgType == "5":

                    logging.info("--- Logout request ---")
                    logging.info(f"{"> " + message.replace(SOH, '|')}")

                    response = self.BuildLogoutResponse(fixFields)
                    clientSocket.sendall(response.encode("utf-8"))

                    logging.info("--- Logout response ---")
                    logging.info(f"{"< " + response.replace(SOH, '|')}")

                    clientSocket.close()

                    logging.info("--- Connection closed by logout ---")

                    return

                else:

                    logging.info(f"--- Unsupported MsgType {msgType} ---")
                    logging.info(f"{"> " + message.replace(SOH, '|')}")

                    # Add handling for other types as needed

        clientSocket.close()
        logging.info("--- Connection closed ---")


    def BuildLogonResponse(self, incomingMsg):
        fields = {
            "35": "A",
            "34": "1",
            "49": self.senderCompID,
            "56": self.targetCompID,
            "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
            "98": "0",
            "108": str(self.heartBtInt),
        }
        return BuildFixMessage(fields)


    def BuildHeartbeatResponse(self, incomingMsg):
        fields = {
            "35": "0",
            "34": str(int(incomingMsg.get("34", "0")) + 1),
            "49": self.senderCompID,
            "56": self.targetCompID,
            "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
        }
        return BuildFixMessage(fields)


    def BuildLogoutResponse(self, incomingMsg):
        fields = {
            "35": "5",  # Logout
            "34": str(int(incomingMsg.get("34", "0")) + 1),
            "49": self.senderCompID,
            "56": self.targetCompID,
            "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
        }
        return BuildFixMessage(fields)
