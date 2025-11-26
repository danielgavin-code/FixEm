#       
#     Title    : ScenarioEngine.py
#     Version  : 1.0
#     Date     : 25 November 2025
#     Author   : Daniel Gavin
#
#     Function : Execute scripted behaviors defined in behaviors.yaml.
#              :   - send
#              :   - delay
#              :   - wait_for
#
#     Modification History
#
#     Date     : 26 November 2025
#     Author   : Daniel Gavin
#     Changes  : New file.
#
#     Date     :
#     Author   :
#     Changes  :
#

import time

class ScenarioEngine:

    def __init__(self, behaviorsDict):
        """
        behaviorsDict: full behaviors.yaml dictionary loaded from ConfigLoader
        """
        self.behaviors = behaviorsDict

    #
    # entry point - run a behavior by name on an order object.
    #

    def runBehavior(self, orderObj, behaviorName):
        if behaviorName not in self.behaviors:
            raise Exception(f"[ERROR] Behavior '{behaviorName}' not found.")

        scenarioSteps = self.behaviors[behaviorName].get("scenario", [])

        print(f"[scenario] Starting behavior '{behaviorName}' for order {orderObj['clOrdID']}")

        for idx, step in enumerate(scenarioSteps, start=1):
            self.executeStep(idx, step, orderObj)

        print(f"[scenario] Completed behavior '{behaviorName}' for order {orderObj['clOrdID']}'")

    #
    # execute a single scenario step 
    #

    def executeStep(self, stepNo, step, orderObj):

        # send: something
        if "send" in step:
            msgType = step["send"]
            print(f"[scenario] step {stepNo}: send '{msgType}'")
            self.handleSend(msgType, orderObj)
            return

        # delay: ms
        if "delay" in step:
            ms = step["delay"]
            print(f"[scenario] step {stepNo}: delay {ms} ms")
            time.sleep(ms / 1000.0)
            return

        # wait_for: event-name
        if "wait_for" in step:
            event = step["wait_for"]
            print(f"[scenario] step {stepNo}: wait_for '{event}'")
            self.handleWaitFor(event, orderObj)
            return

        # end: true
        if "end" in step:
            print(f"[scenario] step {stepNo}: end of scenario")
            return

        # unknown step type
        raise Exception(f"[ERROR] Unsupported scenario step: {step}")

    #
    # roiute scenario actions back into emulator. 
    #
    def handleSend(self, msgType, orderObj):

        server = orderObj.get("server")

        if server is None:
            print(f"[exec] ERROR: no server reference on order object → cannot send '{msgType}' FIX exec")
            return

        # call back into emulator to generate and send exec report
        server.HandleScenarioAction(orderObj, msgType)

    #
    # Placeholder: wait for cancel/replace/etc.
    # Will integrate with server message router later.
    #
    def handleWaitFor(self, eventName, orderObj):
        print(f"[wait] Would block until event '{eventName}' occurs on order {orderObj['clOrdID']}")
        # TODO: integrate with async event queue when server supports it
