"""Keep application failures visible while recognizing one Windows asyncio close bug."""
import re


def application_logs(output):
    # Python's Proactor logs WinError 10054 while closing an already-reset socket.
    # Match only that stdlib callback and its two stdlib frames. Retain raw evidence.
    pattern = r"Exception in callback _ProactorBasePipeTransport\._call_connection_lost\(\)\r?\nhandle: <Handle _ProactorBasePipeTransport\._call_connection_lost\(\)>\r?\nTraceback \(most recent call last\):\r?\n.*?ConnectionResetError: \[WinError 10054\] An existing connection was forcibly closed by the remote host\r?\n"
    def classify(match):
        block = match.group()
        frames = re.findall(r'File "([^"\n]+)"',block)
        only_stdlib = len(frames)==2 and all(re.search(r"[/\\]asyncio[/\\](?:events|proactor_events)\.py$",f) for f in frames)
        if only_stdlib and block.count("Traceback")==1 and "ERROR:" not in block:
            return "[Known Windows asyncio transport close: WinError 10054]\n"
        return block
    return re.sub(pattern,classify,output,flags=re.DOTALL)
