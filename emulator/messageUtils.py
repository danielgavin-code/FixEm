import zlib

SOH = '\x01'

def BuildFixMessage(fields):
    body = SOH.join([f"{tag}={value}" for tag, value in fields.items() if tag != "8" and tag != "9" and tag != "10"]) + SOH
    bodyLength = len(body)
    header = f"8=FIX.4.2{SOH}9={bodyLength}{SOH}"
    msgWithoutChecksum = header + body
    checksum = CalculateChecksum(msgWithoutChecksum)
    fullMessage = msgWithoutChecksum + f"10={checksum:03}{SOH}"
    return fullMessage


def CalculateChecksum(message):
    return sum(bytearray(message, "utf-8")) % 256


def ParseFixMessage(raw):
    try:
        fields = {}
        parts = raw.strip().split(SOH)
        for field in parts:
            if "=" in field:
                tag, value = field.split("=", 1)
                fields[tag] = value
        return fields
    except Exception as e:
        print(f"[ERROR] Failed to parse FIX message: {e}")
        return None

