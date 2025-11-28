#!/usr/bin/python

#
#     Title    : server.py
#     Version  : 1.0
#     Date     : 28 November 2025
#     Author   : Daniel Gavin
#
#     Function : Core FIX Emulator server.
#              :  - Handles incoming FIX session messages.
#              :  - Processes application-layer messages.
#              :  - Store order state and lifecycle transitions.
#              :  - Integrates with ScenarioEngine for scripted executions.
#
#     Modification History
#
#     Date     : 28 November 2025
#     Author   : Daniel Gavin
#     Changes  : New file. 
#
#     Date     :
#     Author   :
#     Changes  :
#

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
        self.outSeq         = 1

        # scenario engine wiring
        self.scenarioEngine = scenarioEngine
        self.sessionConfig  = sessionConfig

        # simple seq counter for scenario-generated execs
        self.scenarioSeqNum = 100000

    #
    # core server
    #


    ###############################################################################
    #
    # Procedure   : Start()
    #
    # Description : - Initialize listener socket.
    #             : - Spawn handler threads for FIX client connections.
    #
    # Input       : -none-
    #
    # Returns     : -none-
    #
    ###############################################################################

    def Start(self):

        logging.info(f"Starting FIX Emulator on {self.host}:{self.port}")

        self.serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serverSocket.bind((self.host, self.port))
        self.serverSocket.listen(1)

        logging.info("Waiting for incoming FIX connection...")

        while True:
            clientSocket, addr = self.serverSocket.accept()
            print(f"[INFO] Connection established from {addr}")
            logging.info(f"Connection established from {addr}")
            thread = threading.Thread(target=self.HandleClient, args=(clientSocket,))
            thread.start()

    # 
    # scenario helpers
    # 


    ###############################################################################
    #
    # Procedure   : _getOrder()
    #
    # Description : Lookup and return stored order dictionary by ClOrdID.
    #
    # Input       : clOrdId - client order identifier
    #
    # Returns     : dictionary - order object or None if not found
    #
    ###############################################################################

    def _getOrder(self, clOrdId):
        return self.orders.get(clOrdId)


    ###############################################################################
    #
    # Procedure   : _getOrder()
    #
    # Description : Return and increment outbound FIX sequnce number for scenario
    #             : execs.
    # 
    # Input       : clOrdId - client order identifier
    # 
    # Returns     : dictionary - order object or None if not found
    #
    ###############################################################################

    def _nextOutboundSeq(self):
        self.outSeq += 1
        return self.outSeq


    ###############################################################################
    #
    # Procedure   : _sendScenarioExec()
    #
    # Description : Build and send scenario-driven ExecReport.
    #             : - Simulate fills, partial fills, cancels, rejects.
    #
    # Input       : order  - dictionary - stored order state 
    #             : action - string     - scenario action (partial, full_fill, etc.)
    #
    # Returns     : -none-
    #
    ###############################################################################

    def _sendScenarioExec(self, order, action):

        clientSocket = order.get("clientSocket")
        if not clientSocket:
            logging.warning(f"[SCENARIO] No client socket for order {order}")
            return

        clOrdId = order["currentClOrdId"]
        symbol  = order["symbol"]
        side    = order["side"]
        qtyStr  = order["qty"]
        price   = order["price"]
        orderId = order["orderId"]

        try:
            origQty = float(qtyStr)

        except Exception:
            origQty = 0.0

        cumQty    = float(order.get("cumQty", 0.0))
        leavesQty = float(order.get("leavesQty", origQty))

        execType  = None
        ordStatus = None
        lastQty   = 0.0

        # 
        # scenario actions
        # 

        if action == "new":
            logging.info(f"[SCENARIO] action=new for {clOrdId} (no extra ExecReport sent)")
            return

        elif action == "partial":
            execType = "1"
            fillQty = leavesQty * 0.25 if leavesQty > 0 else 0.0
            lastQty = fillQty
            cumQty += fillQty
            leavesQty -= fillQty
            ordStatus = "1" if leavesQty > 0 else "2"

        elif action in ("full_fill", "fill"):
            execType  = "2"
            lastQty   = leavesQty if leavesQty > 0 else origQty
            cumQty    = origQty
            leavesQty = 0.0
            ordStatus = "2"

        elif action == "cancel":
            execType  = "4"
            ordStatus = "4"
            lastQty   = 0.0

        elif action == "reject":
            execType  = "8"
            ordStatus = "8"
            lastQty   = 0.0

        elif action == "replace_ack":
            execType  = "5" 
            ordStatus = "5"
            lastQty   = 0.0
            logging.info(f"[SCENARIO] Replace ACK for {clOrdId}")

        else:
            logging.warning(f"[SCENARIO] Unsupported action '{action}'")
            return

        order["cumQty"]    = cumQty
        order["leavesQty"] = leavesQty
        order["status"]    = {
            "0": "NEW",
            "1": "PARTIALLY_FILLED",
            "2": "FILLED",
            "4": "CANCELED",
            "5": "REPLACED",
            "8": "REJECTED",
        }.get(ordStatus, order.get("status", "NEW"))

        now    = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
        execId = f"EX{int(datetime.utcnow().timestamp() * 1000)}"

        fields = {
            "35":  "8",
            "150": execType,
            "39":  ordStatus,
            "37":  orderId,
            "17":  execId,
            "11":  clOrdId,
            "55":  symbol,
            "54":  side,
            "38":  qtyStr,
            "44":  price,
            "60":  now,
            "32":  str(lastQty),
            "31":  price,
            "14":  str(cumQty),
            "151": str(leavesQty),
            "49":  self.senderCompID,
            "56":  self.targetCompID,
            "34":  self._nextOutboundSeq(),  # you already have this method
        }

        response = BuildFixMessage(fields)
        clientSocket.sendall(response.encode())

        logging.info(f"---- Scenario ExecReport ({action}) ----")
        logging.info("< " + response.replace(SOH, "|"))


    ###############################################################################
    #
    # Procedure   : HandleScenarioAction()
    #
    # Description : Process a scenario-driven action (fill, partial, cancel)
    #             : - Locate stored order and send ExecReport. 
    #
    # Input       : orderObj - dictionary - contains clOrdID
    #             : action   - string     - scenario action (partial, full_fill, ..)
    #
    # Returns     : -none-
    #
    ###############################################################################

    def HandleScenarioAction(self, orderObj, action):

        clOrdId = orderObj.get("clOrdID")

        if not clOrdId:
            logging.warning("[SCENARIO] orderObj missing clOrdID")
            return

        order = self._getOrder(clOrdId)

        if not order:
            logging.warning(f"[SCENARIO] No stored order for ClOrdID={clOrdId}")
            return

        logging.info(f"[SCENARIO] HandleScenarioAction: clOrdID={clOrdId}, action={action}")
        self._sendScenarioExec(order, action)


    # 
    # client loop
    #


    ###############################################################################
    #
    # Procedure   : HandleClient()
    #
    # Description : Main loop that does that does the following ... 
    #             : - Receives raw data.
    #             : - Parses MsgType.
    #             : - Message validation and business logic.
    #             : - Sends response.
    #
    # Input       : clientSocket - TCP socket connected to the FIX client
    #
    # Returns     : -none- (runs until client disconnects or Logout)
    #
    ###############################################################################

    def HandleClient(self, clientSocket):

        buffer = ""

        while True:

            data = clientSocket.recv(4096)

            if not data:
                break

            buffer += data.decode("utf-8")

            # crude message seperator
            while SOH in buffer:

                message, sep, buffer = buffer.partition(SOH * 2) 
                fixFields = ParseFixMessage(message + SOH)

                if not fixFields:
                    continue

                msgType = fixFields.get("35")

                if msgType == "A":

                    logging.info("--- Login request ---")
                    logging.info("> " + message.replace(SOH, '|'))

                    response = self.BuildLogonResponse(fixFields)
                    clientSocket.sendall(response.encode("utf-8"))

                    logging.info("--- Login response ---")
                    logging.info("< " + response.replace(SOH, '|'))

                elif msgType == "0":

                    logging.info("--- Heartbeat ---")
                    logging.info("> " + message.replace(SOH, '|'))

                    response = self.BuildHeartbeatResponse(fixFields)
                    clientSocket.sendall(response.encode("utf-8"))

                    logging.info("--- Heartbeat ---")
                    logging.info("< " + response.replace(SOH, '|'))

                elif msgType == "5":

                    logging.info("--- Logout request ---")
                    logging.info("> " + message.replace(SOH, '|'))

                    response = self.BuildLogoutResponse(fixFields)
                    clientSocket.sendall(response.encode("utf-8"))

                    logging.info("--- Logout response ---")
                    logging.info("< " + response.replace(SOH, '|'))

                    clientSocket.close()
                    logging.info("--- Connection closed by logout ---")
                    return

                #
                # New Order Single (Tag 35=D)
                #

                elif msgType == "D":

                    logging.info("---- New Order Single (35=D) ----")
                    logging.info("> " + message.replace(SOH, '|'))

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
                            "35" : "3",
                            "45" : fixFields.get("34", "0"),
                            "371": missing,
                            "373": "1",
                            "58" : f"Required tag {missing} ({requiredTags[missing]}) missing in NewOrderSingle",
                            "49" : self.senderCompID,
                            "56" : self.targetCompID,
                            "34" : str(int(fixFields.get('34', '0')) + 1),
                            "52" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info("< " + response.replace(SOH, '|'))
                        continue

                    # validation - session level invalid values

                    try:

                        qtyVal = float(qty)

                        if qtyVal <= 0:
                            raise ValueError()

                    except Exception:

                        logging.info("---- Order Reject (invalid OrderQty) ----")

                        rejectFields = {
                            "35" : "3",
                            "45" : fixFields.get("34", "0"),
                            "371": "38",
                            "373": "5", 
                            "58" : "OrderQty must be a positive number",
                            "49" : self.senderCompID,
                            "56" : self.targetCompID,
                            "34" : str(int(fixFields.get('34', '0')) + 1),
                            "52" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info("< " + response.replace(SOH, '|'))
                        continue

                    # validation - invalid order type

                    validOrdTypes = {"1", "2"} 

                    if ordType not in validOrdTypes:
                        logging.info(f"---- Order Reject (unsupported OrdType {ordType}) ----")

                        rejectFields = {
                            "35" : "3",
                            "45" : fixFields.get("34", "0"),
                            "371": "40",
                            "373": "2",
                            "58" : f"Unsupported OrdType {ordType}",
                            "49" : self.senderCompID,
                            "56" : self.targetCompID,
                            "34" : str(int(fixFields.get('34', '0')) + 1),
                            "52" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info("< " + response.replace(SOH, '|'))
                        continue

                    # validation - invalid price

                    if ordType == "2": 

                        try:
                            priceVal = float(price)

                            if priceVal <= 0:
                                raise ValueError()

                        except Exception:
                            logging.info("---- Order Reject (invalid Price) ----")

                            rejectFields = {
                                "35" : "3",
                                "45" : fixFields.get("34", "0"),
                                "371": "44",
                                "373": "5",
                                "58" : "Price must be positive for Limit orders",
                                "49" : self.senderCompID,
                                "56" : self.targetCompID,
                                "34" : str(int(fixFields.get('34', '0')) + 1),
                                "52" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                            }

                            response = BuildFixMessage(rejectFields)
                            clientSocket.sendall(response.encode())
                            logging.info("< " + response.replace(SOH, '|'))
                            continue

                    # validation - application level

                    if clOrdId in self.orders:

                        logging.info(f"---- Order Reject (duplicate ClOrdID {clOrdId}) ----")

                        rejectFields = {
                            "35" : "8",
                            "150": "8",
                            "39" : "8",
                            "11" : clOrdId,
                            "58" : "Duplicate ClOrdID — order already exists",
                            "49" : self.senderCompID,
                            "56" : self.targetCompID,
                            "34" : str(int(fixFields.get('34', '0')) + 1),
                            "60" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info("< " + response.replace(SOH, '|'))
                        continue

                    # accept and store order

                    now     = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
                    execId  = f"EX{int(datetime.utcnow().timestamp() * 1000)}"
                    orderId = f"OR{int(datetime.utcnow().timestamp() * 1000)}"

                    self.orders[clOrdId] = {
                        "orderId"  : orderId,
                        "execId"   : execId,
                        "symbol"   : symbol,
                        "side"     : side,
                        "qty"      : qty,
                        "price"    : price,
                        "ordType"  : ordType,
                        "status"   : "NEW",
                        "timestamp": now,

                        # track ids / history
                        "clOrdID"       : clOrdId,
                        "currentClOrdId": clOrdId,
                        "lastClOrdId"   : clOrdId,
                        "history"       : [clOrdId],

                        # socket needed for scenario-generated execs
                        "clientSocket": clientSocket,

                        # execution progress tracking
                        "cumQty"   : 0.0,
                        "leavesQty": float(qty) if qty else 0.0,
                    }

                    logging.info(f"[ORDER STORED] {clOrdId} → {self.orders[clOrdId]}")

                    # send ack

                    ackFields = {
                        "35" : "8",
                        "150": "0",
                        "39" :  "0",
                        "37" :  orderId,
                        "17" :  execId,
                        "11" :  clOrdId,
                        "54" :  side,
                        "38" :  qty,
                        "55" :  symbol,
                        "40" :  ordType,
                        "44" :  price,
                        "60" :  now,
                        "49" :  self.senderCompID,
                        "56" :  self.targetCompID,
                        "34" :  str(int(fixFields.get("34", "0")) + 1)
                    }

                    response = BuildFixMessage(ackFields)
                    clientSocket.sendall(response.encode())

                    logging.info("---- Order Accepted (NEW) ----")
                    logging.info("< " + response.replace(SOH, '|'))

                    #
                    # scenario engine
                    #

                    if self.scenarioEngine and self.sessionConfig:

                        orderObj = {
                            "clOrdID": clOrdId,
                            "symbol" : symbol,
                            "side"   : side,
                            "qty"    : qty,
                            "price"  : price,
                            "server" : self,
                        }

                        # choose behavior
                        chosenBehavior = self.sessionConfig["execution"]["defaultBehavior"]

                        for rule in self.sessionConfig["execution"]["rules"]:
                            if rule["matchFn"](symbol):
                                chosenBehavior = rule["behavior"]
                                break

                        logging.info(f"[SCENARIO] Symbol={symbol} → Behavior={chosenBehavior}")

                        self.scenarioEngine.runBehavior(orderObj, chosenBehavior)

                #
                # Order Cancel Request (Tag 35=F)
                #

                elif msgType == "F":

                    logging.info("---- Order Cancel Request (35=F) ----")
                    logging.info("> " + message.replace(SOH, '|'))

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
                            "35" : "3",
                            "45" : fixFields.get("34", "0"),
                            "371": missing,
                            "373": "1",
                            "58" : f"Required tag {missing} ({requiredTags[missing]}) missing in CancelRequest",
                            "49" : self.senderCompID,
                            "56" : self.targetCompID,
                            "34" : str(int(fixFields.get("34", "0")) + 1),
                            "52" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info("< " + response.replace(SOH, '|'))
                        continue

                    # validation - order id reuse

                    if newClOrdId in self.orders:
                        logging.info(f"---- Cancel Reject (duplicate ClOrdID {newClOrdId}) ----")

                        rejectFields = {
                            "35" : "8",
                            "150": "8",
                            "39" : "8",
                            "11" : newClOrdId,
                            "41" : origClOrdId,
                            "58" : "Duplicate ClOrdID on Cancel Request",
                            "49" : self.senderCompID,
                            "56" : self.targetCompID,
                            "34" : str(int(fixFields.get('34', '0')) + 1),
                            "60" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info("< " + response.replace(SOH, '|'))
                        continue

                    # validation - order lookup ... does it exist

                    order = self.orders.get(origClOrdId)

                    if not order:
                        logging.info(f"---- Cancel Reject (unknown order {origClOrdId}) ----")

                        rejectFields = {
                            "35" : "8",
                            "150": "8",
                            "39" : "8",
                            "11" : newClOrdId,
                            "41" : origClOrdId,
                            "58" : "Unknown order / unable to cancel",
                            "49" : self.senderCompID,
                            "56" : self.targetCompID,
                            "34" : str(int(fixFields.get("34", "0")) + 1),
                            "60" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info("< " + response.replace(SOH, '|'))
                        continue

                    # validation - already canceled

                    if order["status"] == "CANCELED":
                        logging.info(f"---- Cancel Reject (order already canceled {origClOrdId}) ----")

                        rejectFields = {
                            "35" : "8",
                            "150": "8",
                            "39" : "8",
                            "11" : newClOrdId,
                            "41" : origClOrdId,
                            "58" : "Order already canceled",
                            "49" : self.senderCompID,
                            "56" : self.targetCompID,
                            "34" : str(int(fixFields.get('34', '0')) + 1),
                            "60" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info("< " + response.replace(SOH, '|'))
                        continue

                    # update order state

                    order["status"]         = "CANCELED"
                    order["lastClOrdId"]    = newClOrdId
                    order["currentClOrdId"] = newClOrdId
                    order["history"].append(newClOrdId)

                    # send cancel ack

                    now     = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
                    execId  = f"EX{int(datetime.utcnow().timestamp() * 1000)}"
                    orderId = order["orderId"]

                    ackFields = {
                        "35" : "8",
                        "150": "4",
                        "39" : "4",
                        "37" : orderId,
                        "17" : execId,
                        "11" : newClOrdId,
                        "41" : origClOrdId,
                        "54" : order["side"],
                        "38" : order["qty"],
                        "55" : order["symbol"],
                        "60" : now,
                        "49" : self.senderCompID,
                        "56" : self.targetCompID,
                        "34" : str(int(fixFields.get("34", "0")) + 1),
                    }

                    response = BuildFixMessage(ackFields)
                    clientSocket.sendall(response.encode())

                    logging.info("---- Order Cancelled ----")
                    logging.info("< " + response.replace(SOH, '|'))

                #
                # Order Cancel/Replace Request (Tag 35=G)
                #

                elif msgType == "G":

                    logging.info("---- Order Replace Request (35=G) ----")
                    logging.info("> " + message.replace(SOH, '|'))

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
                            "35" : "3",
                            "45" : fixFields.get("34", "0"),
                            "371": missing,
                            "373": "1",
                            "58" : f"Required tag {missing} ({requiredTags[missing]}) missing in ReplaceRequest",
                            "49" : self.senderCompID,
                            "56" : self.targetCompID,
                            "34" : str(int(fixFields.get('34', '0')) + 1),
                            "52" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info("< " + response.replace(SOH, '|'))
                        continue

                    # validation — ClOrdID reuse

                    if newClOrdId in self.orders:
                        logging.info(f"---- Replace Reject (duplicate ClOrdID {newClOrdId}) ----")

                        rejectFields = {
                            "35" : "8",
                            "150": "8",
                            "39" : "8",
                            "11" : newClOrdId,
                            "41" : origClOrdId,
                            "58" : "Duplicate ClOrdID on Replace Request",
                            "49" : self.senderCompID,
                            "56" : self.targetCompID,
                            "34" : str(int(fixFields.get('34', '0')) + 1),
                            "60" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info("< " + response.replace(SOH, '|'))
                        continue

                    # validation - lookup using OLD ClOrdID (origClOrdId)

                    order = self.orders.get(origClOrdId)

                    if not order:

                        logging.info(f"---- Replace Reject (unknown order {origClOrdId}) ----")

                        rejectFields = {
                            "35" : "8",
                            "150": "8",
                            "39" : "8",
                            "11" : newClOrdId,
                            "41" : origClOrdId,
                            "58" : "Unknown order / unable to replace",
                            "49" : self.senderCompID,
                            "56" : self.targetCompID,
                            "34" : str(int(fixFields.get('34', '0')) + 1),
                            "60" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info("< " + response.replace(SOH, '|'))
                        continue

                    # validation — invalid qty

                    try:

                        if float(qty) <= 0:
                            raise ValueError()

                    except Exception:
                        logging.info("---- Replace Reject (invalid OrderQty) ----")

                        rejectFields = {
                            "35" : "3",
                            "45" : fixFields.get("34", "0"),
                            "371": "38",
                            "373": "5",
                            "58" : "OrderQty must be a positive number for Replace request",
                            "49" : self.senderCompID,
                            "56" : self.targetCompID,
                            "34" : str(int(fixFields.get('34', '0')) + 1),
                            "52" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                        }

                        response = BuildFixMessage(rejectFields)
                        clientSocket.sendall(response.encode())
                        logging.info("< " + response.replace(SOH, '|'))
                        continue

                    # validation — invalid price for Limit

                    if ordType == "2":

                        try:
                            if float(price) <= 0:
                                raise ValueError()

                        except Exception:

                            logging.info("---- Replace Reject (invalid Price) ----")

                            rejectFields = {
                                "35" : "3",
                                "45" : fixFields.get("34", "0"),
                                "371": "44",
                                "373": "5",
                                "58" : "Price must be positive for Limit Replace",
                                "49" : self.senderCompID,
                                "56" : self.targetCompID,
                                "34" : str(int(fixFields.get('34', '0')) + 1),
                                "52" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
                            }

                            response = BuildFixMessage(rejectFields)
                            clientSocket.sendall(response.encode())
                            logging.info("< " + response.replace(SOH, '|'))
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
                        "35" : "8",
                        "150": "5",
                        "39" : "5",
                        "37" : orderId,
                        "17" : execId,
                        "11" : newClOrdId,
                        "41" : origClOrdId,
                        "54" : side,
                        "38" : qty,
                        "55" : symbol,
                        "40" : ordType,
                        "44" : price,
                        "60" : now,
                        "49" : self.senderCompID,
                        "56" : self.targetCompID,
                        "34" : str(int(fixFields.get("34", "0")) + 1),
                    }

                    response = BuildFixMessage(ackFields)
                    clientSocket.sendall(response.encode())

                    logging.info("---- Order Replaced ----")
                    logging.info("< " + response.replace(SOH, '|'))

                    if self.scenarioEngine and self.sessionConfig:

                        orderObj = {
                            "clOrdID": newClOrdId,
                            "symbol" : symbol,
                            "side"   : side,
                            "qty"    : qty,
                            "price"  : price,
                            "server" : self,
                        }

                        chosenBehavior = self.sessionConfig["execution"]["defaultBehavior"]

                        for rule in self.sessionConfig["execution"]["rules"]:
                            if rule["matchFn"](symbol):
                                chosenBehavior = rule["behavior"]
                                break

                        logging.info(f"[SCENARIO] (REPLACE) Symbol={symbol} → Behavior={chosenBehavior}")

                        self.scenarioEngine.runBehavior(orderObj, chosenBehavior)

                else:

                    logging.info(f"--- Unsupported MsgType {msgType} ---")
                    logging.info("> " + message.replace(SOH, '|'))

        clientSocket.close()
        logging.info("--- Connection closed ---")

    #
    # session messages
    #


    ###############################################################################
    #
    # Procedure   : BuildLogonResponse()
    #
    # Description : Build and return FIX Logon message. (35=A)
    #
    # Input       : incomingMsg - dictionary of parsed FIX fields from client
    #
    # Returns     : string - Encoded FIX Logon message (35=A)
    #
    ###############################################################################

    def BuildLogonResponse(self, incomingMsg):

        fields = {
            "35" : "A",
            "34" : "1",
            "49" : self.senderCompID,
            "56" : self.targetCompID,
            "52" : datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
            "98" : "0",
            "108": str(self.heartBtInt),
        }

        return BuildFixMessage(fields)


    ###############################################################################
    #
    # Procedure   : BuildHeartbeatResponse()
    #
    # Description : Build and return FIX Heartbeat response. (35=0)
    #
    # Input       : incomingMsg - dictionary of parsed FIX fields from client
    #
    # Returns     : string - Encoded FIX Heartbeat message (35=0)
    #
    ###############################################################################

    def BuildHeartbeatResponse(self, incomingMsg):

        fields = {
            "35": "0",
            "34": str(int(incomingMsg.get("34", "0")) + 1),
            "49": self.senderCompID,
            "56": self.targetCompID,
            "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
        }

        return BuildFixMessage(fields)


    ###############################################################################
    #
    # Procedure   : BuildLogoutResponse()
    #
    # Description : Build and return FIX Logout. (35=5) 
    #
    # Input       : incomingMsg - dictionary of parsed FIX fields from client
    #
    # Returns     : string - Encoded FIX Logout message (35=5)
    #
    ###############################################################################

    def BuildLogoutResponse(self, incomingMsg):

        fields = {
            "35": "5", 
            "34": str(int(incomingMsg.get("34", "0")) + 1),
            "49": self.senderCompID,
            "56": self.targetCompID,
            "52": datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
        }

        return BuildFixMessage(fields)
