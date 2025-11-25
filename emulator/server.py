import logging
import socket
import threading
from datetime import datetime
from emulator.messageUtils import BuildFixMessage, ParseFixMessage

SOH = '\x01'

class FixEmulatorServer:

    orders = {}

    def __init__(self, host, port, senderCompID, targetCompID, heartBtInt=30,
                   scenarioEngine=None, sessionConfig=None):
        self.host           = host
        self.port           = port
        self.senderCompID   = senderCompID
        self.targetCompID   = targetCompID
        self.heartBtInt     = heartBtInt
        self.serverSocket   = None
        self.scenarioEngine = scenarioEngine
        self.sessionConfig  = sessionConfig

    def Start(self):

        logging.info(f"Starting FIX Emulator on {self.host}:{self.port}") 

        self.serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serverSocket.bind((self.host, self.port))
        self.serverSocket.listen(1)

        logging.info("Waiting for incoming FIX connection...")

        while True:
            clientSocket, addr = self.serverSocket.accept()
            print(f"[INFO] Connection established from {addr}")
            print(f"Connection established from {addr}")
            thread = threading.Thread(target=self.HandleClient, args=(clientSocket,))
            thread.start()


    def StoreNewOrder(self, fixFields, orderId, execId):

        clOrdId = fixFields.get("11")

        self.orders[clOrdId] = {
            "orderId": orderId,
            "execId":  execId,
            "symbol":  fixFields.get("55"),
            "side":    fixFields.get("54"),
            "qty":     fixFields.get("38"),
            "price":   fixFields.get("44"),
            "filledQty": 0,
            "status":  "NEW",
            "timestamp": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
        }

        logging.info(f"[ORDER STORED] {clOrdId} → {self.orders[clOrdId]}")


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

                #
                # New Order Single (Tag 35=D)
                #

                elif msgType == "D":

                    logging.info("---- New Order Single (35=D) ----")
                    logging.info(f"{'> ' + message.replace(SOH, '|')}")

                    clOrdId = fixFields.get("11")
                    side    = fixFields.get("54")
                    qty     = fixFields.get("38")
                    symbol  = fixFields.get("55")
                    ordType = fixFields.get("40")
                    price   = fixFields.get("44", "0")

                    # validation - session level

                    requiredTags = {
                        "11": "ClOrdID",
                        "54": "Side",
                        "38": "OrderQty",
                        "55": "Symbol",
                        "40": "OrdType",
                    }

                    missingTags = [tag for tag in requiredTags if not fixFields.get(tag)]

                    if missingTags:

                        missing = missingTags[0]
                        logging.info(f"---- Order Reject (missing tag {missing} {requiredTags[missing]}) ----")

                        rejectFields = {
                            "35": "3",
                            "45": fixFields.get("34", "0"), 
                            "371": missing,
                            "373": "1",
                            "58": f"Required tag {missing} ({requiredTags[missing]}) missing in NewOrderSingle",
                            "49": self.senderCompID,
                            "56": self.targetCompID,
                            "34": str(int(fixFields.get('34', '0')) + 1),
                            "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info(f"{'< ' + response.replace(SOH, '|')}")
                        continue

                    # validation - session level invalid values 

                    try:
                        qtyVal = float(qty)

                        if qtyVal <= 0:
                            raise ValueError()

                    except:
                        logging.info("---- Order Reject (invalid OrderQty) ----")

                        rejectFields = {
                            "35": "3",
                            "45": fixFields.get("34", "0"),
                            "371": "38",
                            "373": "5",  # Incorrect value
                            "58": "OrderQty must be a positive number",
                            "49": self.senderCompID,
                            "56": self.targetCompID,
                            "34": str(int(fixFields.get('34', '0')) + 1),
                            "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())

                        logging.info(f"{'< ' + response.replace(SOH, '|')}")
                        continue

                    # validation - invalid order type
 
                    validOrdTypes = {"1", "2"}  # Market, Limit

                    if ordType not in validOrdTypes:
                        logging.info(f"---- Order Reject (unsupported OrdType {ordType}) ----")

                        rejectFields = {
                            "35": "3",
                            "45": fixFields.get("34", "0"),
                            "371": "40",
                            "373": "2",
                            "58": f"Unsupported OrdType {ordType}",
                            "49": self.senderCompID,
                            "56": self.targetCompID,
                            "34": str(int(fixFields.get('34', '0')) + 1),
                            "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())

                        logging.info(f"{'< ' + response.replace(SOH, '|')}")
                        continue

                    # validation - invalid price 
                    if ordType == "2":  # Limit

                        try:
                            priceVal = float(price)

                            if priceVal <= 0:
                                raise ValueError()

                        except:
                            logging.info("---- Order Reject (invalid Price) ----")

                            rejectFields = {
                                "35": "3",
                                "45": fixFields.get("34", "0"),
                                "371": "44",
                                "373": "5",
                                "58": "Price must be positive for Limit orders",
                                "49": self.senderCompID,
                                "56": self.targetCompID,
                                "34": str(int(fixFields.get('34', '0')) + 1),
                                "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                            }

                            response = BuildFixMessage(rejectFields)
                            clientSocket.sendall(response.encode())

                            logging.info(f"{'< ' + response.replace(SOH, '|')}")
                            continue

                    # validation - applicaiton level

                    if clOrdId in self.orders:
                        logging.info(f"---- Order Reject (duplicate ClOrdID {clOrdId}) ----")

                        rejectFields = {
                            "35": "8",
                            "150": "8",
                            "39":  "8",
                            "11":  clOrdId,
                            "58":  "Duplicate ClOrdID — order already exists",
                            "49":  self.senderCompID,
                            "56":  self.targetCompID,
                            "34":  str(int(fixFields.get('34', '0')) + 1),
                            "60":  datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())

                        logging.info(f"{'< ' + response.replace(SOH, '|')}")
                        continue

                    # accept and store order 

                    now     = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
                    execId  = f"EX{int(datetime.utcnow().timestamp()*1000)}"
                    orderId = f"OR{int(datetime.utcnow().timestamp()*1000)}"

                    self.orders[clOrdId] = {
                        "orderId": orderId,
                        "execId": execId,
                        "symbol": symbol,
                        "side": side,
                        "qty": qty,
                        "price": price,
                        "ordType": ordType,
                        "status": "NEW",
                        "timestamp": now,
                        "currentClOrdId": clOrdId,
                        "lastClOrdId": clOrdId,
                        "history": [clOrdId],
                    }

                    logging.info(f"[ORDER STORED] {clOrdId} → {self.orders[clOrdId]}")

                    # send ack

                    ackFields = {
                        "35": "8",
                        "150": "0",
                        "39":  "0",
                        "37":  orderId,
                        "17":  execId,
                        "11":  clOrdId,
                        "54":  side,
                        "38":  qty,
                        "55":  symbol,
                        "40":  ordType,
                        "44":  price,
                        "60":  now,
                        "49": self.senderCompID,
                        "56": self.targetCompID,
                        "34": str(int(fixFields.get("34", "0")) + 1)
                    }

                    response = BuildFixMessage(ackFields)
                    clientSocket.sendall(response.encode())

                    logging.info("---- Order Accepted (NEW) ----")
                    logging.info(f"{'< ' + response.replace(SOH, '|')}")

                #
                # Order Cancel Request (Tag 35=F)
                #

                elif msgType == "F":

                    logging.info("---- Order Cancel Request (35=F) ----")
                    logging.info(f"{'> ' + message.replace(SOH, '|')}")

                    origClOrdId = fixFields.get("41")
                    newClOrdId  = fixFields.get("11")
                    symbol      = fixFields.get("55")
                    side        = fixFields.get("54")

                    # validation - session level

                    requiredTags = {
                        "11": "ClOrdID",
                        "41": "OrigClOrdID",
                        "55": "Symbol",
                        "54": "Side",
                    }

                    missingTags = [tag for tag in requiredTags if not fixFields.get(tag)]

                    if missingTags:
                        missing = missingTags[0]
                        logging.info(f"---- Cancel Reject (missing tag {missing} {requiredTags[missing]}) ----")

                        rejectFields = {
                            "35": "3",
                            "45": fixFields.get("34", "0"),
                            "371": missing,
                            "373": "1",
                            "58": f"Required tag {missing} ({requiredTags[missing]}) missing in CancelRequest",
                            "49": self.senderCompID,
                            "56": self.targetCompID,
                            "34": str(int(fixFields.get("34","0")) + 1),
                            "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())

                        logging.info(f"{'< ' + response.replace(SOH, '|')}")
                        continue

                    # validation - order id resuse

                    if newClOrdId in self.orders:
                        logging.info(f"---- Cancel Reject (duplicate ClOrdID {newClOrdId}) ----")

                        rejectFields = {
                            "35": "8",
                            "150": "8",
                            "39":  "8",
                            "11": newClOrdId,
                            "41": origClOrdId,
                            "58": "Duplicate ClOrdID on Cancel Request",
                            "49": self.senderCompID,
                            "56": self.targetCompID,
                            "34": str(int(fixFields.get('34','0')) + 1),
                            "60": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info(f"{'< ' + response.replace(SOH, '|')}")
                        continue

                    # validation - order lookup ... does it exist

                    order = self.orders.get(origClOrdId)

                    if not order:
                        logging.info(f"---- Cancel Reject (unknown order {origClOrdId}) ----")

                        rejectFields = {
                            "35": "8",
                            "150": "8",
                            "39":  "8",
                            "11": newClOrdId,
                            "41": origClOrdId,
                            "58": "Unknown order / unable to cancel",
                            "49": self.senderCompID,
                            "56": self.targetCompID,
                            "34": str(int(fixFields.get("34","0")) + 1),
                            "60": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())

                        logging.info(f"{'< ' + response.replace(SOH, '|')}")
                        continue

                    # validation - already canceled

                    if order["status"] == "CANCELED":
                        logging.info(f"---- Cancel Reject (order already canceled {origClOrdId}) ----")

                        rejectFields = {
                            "35": "8",
                            "150": "8",
                            "39":  "8",
                            "11": newClOrdId,
                            "41": origClOrdId,
                            "58": "Order already canceled",
                            "49": self.senderCompID,
                            "56": self.targetCompID,
                            "34": str(int(fixFields.get('34','0')) + 1),
                            "60": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())

                        logging.info(f"{'< ' + response.replace(SOH, '|')}")
                        continue

                    # update order state 

                    order["status"] = "CANCELED"
                    order["lastClOrdId"] = newClOrdId
                    order["currentClOrdId"] = newClOrdId
                    order["history"].append(newClOrdId)

                    # send cancel ack

                    now     = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
                    execId  = f"EX{int(datetime.utcnow().timestamp() * 1000)}"
                    orderId = order["orderId"]

                    ackFields = {
                        "35": "8",
                        "150": "4",
                        "39":  "4",
                        "37":  orderId,
                        "17":  execId,
                        "11":  newClOrdId,
                        "41":  origClOrdId,
                        "54":  order["side"],
                        "38":  order["qty"],
                        "55":  order["symbol"],
                        "60":  now,
                        "49":  self.senderCompID,
                        "56":  self.targetCompID,
                        "34": str(int(fixFields.get("34","0")) + 1),
                    }

                    response = BuildFixMessage(ackFields)
                    clientSocket.sendall(response.encode())

                    logging.info("---- Order Cancelled ----")
                    logging.info(f"{'< ' + response.replace(SOH, '|')}")


                #
                # Order Cancel/Replace Request (Tag 35=G)
                #

                elif msgType == "G":

                    logging.info("---- Order Replace Request (35=G) ----")
                    logging.info(f"{'> ' + message.replace(SOH, '|')}")

                    origClOrdId = fixFields.get("41")
                    newClOrdId  = fixFields.get("11")
                    symbol      = fixFields.get("55")
                    side        = fixFields.get("54")
                    qty         = fixFields.get("38")
                    ordType     = fixFields.get("40")
                    price       = fixFields.get("44")

                    #
                    # validation — required tags
                    #

                    requiredTags = {
                        "11": "ClOrdID",
                        "41": "OrigClOrdID",
                        "55": "Symbol",
                        "54": "Side",
                        "38": "OrderQty",
                        "40": "OrdType",
                    }

                    missingTags = [tag for tag in requiredTags if not fixFields.get(tag)]

                    if missingTags:
                        missing = missingTags[0]

                        logging.info(f"---- Replace Reject (missing tag {missing} {requiredTags[missing]}) ----")

                        rejectFields = {
                            "35": "3",
                            "45": fixFields.get("34", "0"),
                            "371": missing,
                            "373": "1",
                            "58": f"Required tag {missing} ({requiredTags[missing]}) missing in ReplaceRequest",
                            "49": self.senderCompID,
                            "56": self.targetCompID,
                            "34": str(int(fixFields.get('34', '0')) + 1),
                            "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info(f"{'< ' + response.replace(SOH, '|')}")
                        continue

                    # validation — ClOrdID reuse

                    if newClOrdId in self.orders:
                        logging.info(f"---- Replace Reject (duplicate ClOrdID {newClOrdId}) ----")

                        rejectFields = {
                            "35": "8",
                            "150": "8",
                            "39":  "8",
                            "11":  newClOrdId,
                            "41":  origClOrdId,
                            "58":  "Duplicate ClOrdID on Replace Request",
                            "49":  self.senderCompID,
                            "56":  self.targetCompID,
                            "34": str(int(fixFields.get('34','0')) + 1),
                            "60": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info(f"{'< ' + response.replace(SOH, '|')}")
                        continue

                    # validation - lookup using OLD ClOrdID (origClOrdId)

                    order = self.orders.get(origClOrdId)

                    if not order:
                        logging.info(f"---- Replace Reject (unknown order {origClOrdId}) ----")

                        rejectFields = {
                            "35": "8",
                            "150": "8",
                            "39":  "8",
                            "11":  newClOrdId,
                            "41":  origClOrdId,
                            "58":  "Unknown order / unable to replace",
                            "49":  self.senderCompID,
                            "56":  self.targetCompID,
                            "34": str(int(fixFields.get('34','0')) + 1),
                            "60": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info(f"{'< ' + response.replace(SOH, '|')}")
                        continue

                    # validation — invalid qty

                    try:
                        if float(qty) <= 0:
                            raise ValueError()
                    except:
                        logging.info("---- Replace Reject (invalid OrderQty) ----")

                        rejectFields = {
                            "35": "3",
                            "45": fixFields.get("34", "0"),
                            "371": "38",
                            "373": "5",
                            "58": "OrderQty must be a positive number for Replace request",
                            "49": self.senderCompID,
                            "56": self.targetCompID,
                            "34": str(int(fixFields.get('34','0')) + 1),
                            "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info(f"{'< ' + response.replace(SOH, '|')}")
                        continue

                    # validation — invalid price for Limit

                    if ordType == "2":
                        try:
                            if float(price) <= 0:
                                raise ValueError()
                        except:
                            logging.info("---- Replace Reject (invalid Price) ----")

                            rejectFields = {
                                "35": "3",
                                "45": fixFields.get("34", "0"),
                                "371": "44",
                                "373": "5",
                                "58": "Price must be positive for Limit Replace",
                                "49": self.senderCompID,
                                "56": self.targetCompID,
                                "34": str(int(fixFields.get('34','0')) + 1),
                                "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                            }

                            response = BuildFixMessage(rejectFields)
                            clientSocket.sendall(response.encode())
                            logging.info(f"{'< ' + response.replace(SOH, '|')}")
                            continue

                    # apply the replace values

                    oldKey = origClOrdId

                    order["qty"]     = qty
                    order["price"]   = price
                    order["ordType"] = ordType
                    order["side"]    = side
                    order["symbol"]  = symbol

                    order["currentClOrdId"] = newClOrdId
                    order["lastClOrdId"]    = newClOrdId
                    order["history"].append(newClOrdId)

                    # update dictionary 

                    self.orders.pop(oldKey)
                    self.orders[newClOrdId] = order

                    # ack - send the ack message 

                    now     = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
                    execId  = f"EX{int(datetime.utcnow().timestamp() * 1000)}"
                    orderId = order["orderId"]

                    ackFields = {
                        "35": "8",
                        "150": "5",
                        "39":  "5",
                        "37":  orderId,
                        "17":  execId,
                        "11":  newClOrdId,
                        "41":  origClOrdId,
                        "54":  side,
                        "38":  qty,
                        "55":  symbol,
                        "40":  ordType,
                        "44":  price,
                        "60":  now,
                        "49":  self.senderCompID,
                        "56":  self.targetCompID,
                        "34": str(int(fixFields.get("34","0")) + 1),
                    }

                    response = BuildFixMessage(ackFields)
                    clientSocket.sendall(response.encode())

                    logging.info("---- Order Replaced ----")
                    logging.info(f"{'< ' + response.replace(SOH, '|')}")

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
