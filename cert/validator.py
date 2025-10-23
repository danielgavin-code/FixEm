import os

#
#     Title    : validator.py
#     Version  : 1.0
#     Date     : 23 October 2025
#     Author   : Daniel Gavin
#
#     Function : FIX message certification validator for the FixEm tool.
#              : - Validates required, optional, and conditional tag presence.
#              : - Supports FIX 4.2 specification.
#              : - Customizable
#
#     Modification History
#
#     Date     : 23 October 2025
#     Author   : Daniel Gavin
#     Changes  : New file. 
#
#     Date     :
#     Author   :
#     Changes  :
#


###############################################################################
#
# Class       : CertificationValidator
#
# Description : Validates FIX messages in log against certification rules.
#
# Input       : logFile (str) - path to the FIX log file
#
###############################################################################

class CertificationValidator:

    def __init__(self, logFile):
        self.logFile = logFile
        self.rawLines = []
        self.parsedMessages = []
        self.results = []

        # custom tags by message type
        self.customTagsByType = {
            "A": [],
            "D": ["44", "9140"],
            "8": ["20"],
            "5": [],
        }

    def LoadLog(self):
        if not os.path.isfile(self.logFile):
            raise FileNotFoundError(f"[ERROR] Log file not found: {self.logFile}")

        with open(self.logFile, 'r') as file:
            self.rawLines = [line.strip() for line in file if line.strip()]


    def ParseMessages(self):

        for line in self.rawLines:

            delimiter = "|" if "|" in line else "\x01"
            fields    = line.split(delimiter)

            msg = {}

            for field in fields:

                if "=" in field:
                    tag, value = field.split("=", 1)
                    msg[tag] = value

            if "35" in msg:
                self.parsedMessages.append(msg)
            else:
                self.results.append(("❌", "Message skipped: Missing tag 35 (MsgType)"))


    def ValidateMessages(self):

        for idx, msg in enumerate(self.parsedMessages, start=1):

            msgType = msg.get("35")
            result  = self.ValidateMsgType(msgType, msg)
            self.results.append((f"Line {idx}", result))

        return self.results


    def ValidateMsgType(self, msgType, msg):

        if msgType == "A":
            return self.ValidateLogon(msg)

        elif msgType == "D":
            return self.ValidateNewOrder(msg)

        elif msgType == "8":
            return self.ValidateExecutionReport(msg)

        elif msgType == "5":
            return self.ValidateLogout(msg)

        else:
            return f"⚠️  Unknown MsgType: {msgType} — Skipped structural validation"


    def ValidateLogon(self, msg):
    
        required     = ["8", "9", "35", "49", "56", "34", "52", "98", "108", "10"]
        optional     = ["95", "96", "141", "553", "554", "1137"]
        conditionals = [("95", "96")] 
        return self.CheckFields("Logon", required, optional, conditionals, msg)


    def ValidateLogout(self, msg):
    
        required     = ["8", "9", "35", "49", "56", "34", "52", "10"]
        optional     = ["58"]
        conditionals = []
        return self.CheckFields("Logout", required, optional, conditionals, msg)


    def ValidateNewOrder(self, msg):

        required = ["8", "9", "35", "49", "56", "34", "52", "11", "21", "55", "54", "38", "40", "60", "10"]
        optional = ["59", "47", "58", "18", "44", "15", "100", "207", "848", "849", "99", "110", "111"]
    
        conditionals = [
            ("48", "22"),
            ("95", "96"),
        ]

        return self.CheckFields("NewOrderSingle", required, optional, conditionals, msg)


    def ValidateExecutionReport(self, msg):

        required = ["8", "9", "35", "49", "56", "34", "52", "11", "17", "150", "39", "55", "54", "38", "40", "44", "14", "6", "10"]
        optional = ["32", "31", "29", "37", "198", "75", "105", "60", "151", "100", "207", "848", "849", "15"]

        conditionals = [
            ("48", "22"),
            ("95", "96"),
        ]

        return self.CheckFields("ExecutionReport", required, optional, conditionals, msg)


    def CheckFields(self, label, requiredFields, optionalFields, conditionalPairs, msg):

        msgType    = msg.get("35", "")
        customTags = self.customTagsByType.get(msgType, [])

        # tag enforcement
        missing = [tag for tag in requiredFields if tag not in msg]

        # allowed tags 
        allowed = set(requiredFields + optionalFields + customTags)

        # non-standard/approved tags 
        unexpected = [tag for tag in msg if tag not in allowed]

        # conditional tag enforcement 

        conditionalErrors = []

        for tagA, tagB in conditionalPairs:

            aIsPresent = tagA in msg
            bIsPresent = tagB in msg

            if aIsPresent ^ bIsPresent:  #fancy way of saying ... one is present, other isn't 
                conditionalErrors.append(f"{tagA}/{tagB} must both be present")

        # results 
        errors = []

        if missing:
            errors.append(f"missing required tag(s): {', '.join(missing)}")

        if unexpected:
            errors.append(f"unexpected tag(s): {', '.join(unexpected)}")

        if conditionalErrors:
            errors.extend(conditionalErrors)

        if errors:
            return f"❌ {label} " + "; ".join(errors)

        return f"✅ Valid {label}"

