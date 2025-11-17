#!/usr/bin/python

#
#     Title    : FixEm.py 
#     Version  : 1.0
#     Date     : 23 October 2025 
#     Author   : Daniel Gavin
#
#     Function : FIX Emulator and certification tool. 
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

import os
import sys
import yaml
import logging
import argparse

from datetime        import datetime
from emulator.server import FixEmulatorServer
from cert.validator  import CertificationValidator

TODAY = datetime.now().strftime("%Y%m%d")

# let's add some color
BLUE   = '\033[94m'
GREEN  = '\033[92m'
YELLOW = '\033[93m'
RESET  = '\033[0m'


def InitializeLogging():

    os.makedirs("logs", exist_ok=True)

    logfile = f"logs/{TODAY}.txt"

    logging.basicConfig(

        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',

        handlers=[
            logging.FileHandler(logfile, mode='a'),  # Append mode
            logging.StreamHandler()  # Also log to console
        ]

    )


###############################################################################
#
# Procedure   : ParseArgs() 
#
# Description : Parse command-line arguments. 
#
# Input       : -none-   
#
# Returns     : args (Namespace) - parsed command line arguments 
#
###############################################################################

def ParseArgs():

    parser = argparse.ArgumentParser(

        prog="FixEm",

        description=(
            f"{GREEN}FixEm ⚙️  FIX Emulator & Certification Tool{RESET}\n\n"
            f"{YELLOW}Emulate executions, certify logs, control your session.{RESET}"
        ),

        epilog=(
            f"{BLUE}Examples:{RESET}\n"
            f"  python FixEm.py --mode emulate --config configs/equities.yaml\n"
            f"  python FixEm.py --mode certify --log logs/session.log\n\n"
            f"{GREEN}Note:{RESET} emulate mode requires --config; certify mode requires --log."
        ),

        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--mode",
        choices=["emulate", "certify"],
        required=True,
        help="🧭 Mode to run: 'emulate' for mock FIX session, 'certify' to analyze a FIX log.",
    )

    parser.add_argument(
        "--config",
        type=str,
        help="📁 Path to FIX session config file (required for emulate mode).",
    )

    parser.add_argument(
        "--log",
        type=str,
        help="📄 Path to FIX log file for certification (required for certify mode).",
    )

    return parser.parse_args()


###############################################################################
#             
# Procedure   : Main()
#
# Description : Entry point.
#     
# Input       : -none-    
#              
# Returns     : -none-
#     
###############################################################################

def Main():

    args = ParseArgs()

    InitializeLogging()

    #
    # emulate
    #

    if args.mode == "emulate":

        if not args.config:
            print("[ERROR] --config is required for emulate mode")
            sys.exit(1)

        print(f"[INFO] Loading emulator config: {args.config}")

        try:
            with open(args.config, 'r') as file:
                config = yaml.safe_load(file)

            host = config.get("host", "127.0.0.1")
            port = config.get("port", 9898)
            senderCompID = config.get("sender_comp_id", "FIXEM")
            targetCompID = config.get("target_comp_id", "CLIENT1")
            heartBtInt = config.get("heartbtint", 30)

            emulator = FixEmulatorServer(
                host=host,
                port=port,
                senderCompID=senderCompID,
                targetCompID=targetCompID,
                heartBtInt=heartBtInt
            )

            emulator.Start()

        except Exception as e:
            print(f"[ERROR] Failed to start emulator: {str(e)}")
            sys.exit(2)

    #
    # certify
    #
    elif args.mode == "certify":

        if not args.log:
            print("[ERROR] --log is required for certify mode")
            sys.exit(1)

        print(f"[INFO] Certifying FIX Log: {args.log}")

        try:
            validator = CertificationValidator(args.log)
            validator.LoadLog()
            validator.ParseMessages()
            results = validator.ValidateMessages()

            print("--- Certification Results ---")
            for label, message in results:
                print(f"  {label:8} {message}")
            print("")

        except Exception as e:
            print(f"[ERROR] Certification failed: {str(e)}")
            sys.exit(2)

    else:
        print("[ERROR] Invalid mode selected")
        sys.exit(1)


if __name__ == "__main__":
    Main()
