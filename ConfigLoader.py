#       
#     Title    : FixEm.py
#     Version  : 1.0
#     Date     : 25 November 2025 
#     Author   : Daniel Gavin
#   
#     Function : Entry point for FixEm, does the following.
#              : - Loads session profiles.
#              : - Handles startup.
#              : - Scheduling
#              : - Runtime behavior
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

import yaml
import fnmatch
import os

class ConfigLoader:

    def __init__(self, configPath="configs"):
        self.configPath = configPath

    #
    # load engine.yaml
    #

    def loadEngineConfig(self):

        path = os.path.join(self.configPath, "engine.yaml")
        with open(path, "r") as f:
            data = yaml.safe_load(f)

        if "engine" not in data or "sessions" not in data["engine"]:
            raise Exception("[ERROR] engine.yaml is missing required 'engine.sessions' block")

        return data["engine"]

    #
    # load behaviors.yaml
    #

    def loadBehaviors(self):

        path = os.path.join(self.configPath, "behaviors.yaml")
        with open(path, "r") as f:
            data = yaml.safe_load(f)

        if "behaviors" not in data:
            raise Exception("[ERROR] behaviors.yaml missing top-level 'behaviors:' block")

        return data["behaviors"]

    #
    # load single session profile (i.e. equities.yaml)
    #

    def loadSessionProfile(self, fileName):

        path = os.path.join(self.configPath, fileName)

        if not os.path.exists(path):
            raise FileNotFoundError(f"Session config '{fileName}' not found")

        with open(path, "r") as f:
            data = yaml.safe_load(f)

        if "session" not in data:
            raise Exception(f"[ERROR] {fileName} missing required 'session:' block")

        return data["session"]

    #
    # Compile symbol rules
    #

    def compileRules(self, ruleList, behaviorsDict):

        compiled = []

        for rule in ruleList:
            if "match" not in rule or "behavior" not in rule:
                raise Exception("[ERROR] rule must contain 'match' and 'behavior' fields")

            behaviorName = rule["behavior"]
            if behaviorName not in behaviorsDict:
                raise Exception(f"[ERROR] behavior '{behaviorName}' not found in behaviors.yaml")

            matchPattern = rule["match"]

            # pre-compile match function
            def matcher(symbol, pat=matchPattern):
                return fnmatch.fnmatch(symbol, pat)

            compiled.append({
                "matchFn" : matcher,
                "behavior": behaviorName,
                "pattern" : matchPattern
            })

        return compiled

    #
    # build config bundle at startup
    #

    def loadAll(self):

        engineCfg = self.loadEngineConfig()
        behaviors = self.loadBehaviors()

        sessionBundle = {}

        for entry in engineCfg["sessions"]:

            name     = entry.get("name")
            fileName = entry.get("file")
            enabled  = entry.get("enabled", False)

            if not name or not fileName:
                raise Exception("[ERROR] each session entry must have 'name' and 'file'")

            # skip disabled sessions
            if not enabled:
                continue 

            profile = self.loadSessionProfile(fileName)

            # load the full yaml 
            fullPath = os.path.join(self.configPath, fileName)

            with open(fullPath, "r") as f:
                fullYaml = yaml.safe_load(f)

            if "execution" not in fullYaml or "rules" not in fullYaml["execution"]:
                raise Exception(f"[ERROR] {fileName} missing execution.rules block")

            compiledRules = self.compileRules(
                fullYaml["execution"]["rules"],
                behaviors
            )

            sessionBundle[name] = {
                "profileName": profile.get("name", name),
                "role": profile.get("role", "initiator"),

                # schedule *IS* inside session:
                "schedule": profile.get("schedule", {}),

                # connection is TOP-LEVEL:
                "connection": fullYaml.get("connection", {}),

                # execution is TOP-LEVEL:
                "execution": {
                    "defaultBehavior": fullYaml["execution"].get("default_behavior"),
                    "rules": compiledRules
                },

                # keep the full YAML if needed later
                "rawProfile": fullYaml
            }

        return {
            "engine"    : engineCfg,
            "behaviors" : behaviors,
            "sessions"  : sessionBundle
        }
