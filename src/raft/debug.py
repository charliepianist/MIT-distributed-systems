from rich import print
from rich.columns import Columns
from rich.console import Console
from datetime import time, datetime, date
import sys

skip_lock = False
skip_trace = True
for arg in sys.argv:
    if arg == 'skip-lock':
        skip_lock = True
    if arg == 'show-trace':
        skip_trace = False

file = open('out')
raw_output = file.read()
file.close()
lines = [line for line in raw_output.split('\n') if len(line) > 0 and line[0] == '[']

colors = {
    "CNDT": "#00aa00",
    "VOTE": "#bbbb00",
    "HTBT": "#888888",
    "STCH": "#cccccc",
    "STRT": "#00ff00",
    "KILL": "#ff0000",
    "STTS": "#00ffff",
    "LOCK": "#444444",
    "CMMT": "#FF69B4",
    "CLNT": "#0000FF",
    "LOGS": "#3948a1",
    "TRCE": "#ffffff",
    "DBUG": "#ffc0cb",
}

# Parse file
annot = []
numServers = 0
start_time = None
for line in lines:
    parts = line.split(' ')
    info = {
        'topic': parts[0][1:-1],
        'time': parts[1],
        'server': parts[3],
        'role': parts[4][1:-1],
        'term': parts[6][:-1],
        'msg': ' '.join(parts[8:])
    }
    if info['topic'] == 'LOCK' and skip_lock:
        continue
    if info['topic'] == 'TRCE' and skip_trace:
        continue
    if start_time is None:
        start_time = datetime.combine(date.today(), time.fromisoformat(info['time']))
    td = (datetime.combine(date.today(), time.fromisoformat(info['time'])) - start_time)
    info['time'] = td.microseconds // 1000 / 1000 + td.seconds
    annot.append(info)
    if int(info['server']) + 1 > numServers:
        numServers = int(info['server']) + 1

# Get layout info
console = Console()
width = console.size.width

# Print
for info in annot:
    cols = ["" for _ in range(numServers)]
    msg = info['msg']
    topic = info['topic']
    server = info['server']
    role = info['role']
    term = info['term']
    t = info['time']
    msg = f'[{colors[topic]}][{t:.3f}] S{server} T{term} ({role}) {msg}[/{colors[topic]}]'

    cols[int(info['server'])] = msg
    col_width = int(width / numServers)
    cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
    print(cols)
if raw_output.split('\n')[-2][:2] == 'ok':
    print('ok')
else:
    print(raw_output.split('\n')[-6])
    print(raw_output.split('\n')[-5])
    print(raw_output.split('\n')[-4])