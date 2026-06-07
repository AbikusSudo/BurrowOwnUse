import argparse, asyncio, json, os, signal, socket, struct, time, uuid, zlib, sys
import urllib.request, urllib.parse, requests, threading, traceback, ssl, re
from aioice import Candidate, Connection
from aioice import stun, turn
from aioice.stun import TransactionFailed
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich import box
from http.server import HTTPServer, BaseHTTPRequestHandler

console = Console()

if "DATA" not in stun.ATTRIBUTES_BY_NAME:
    stun.ATTRIBUTES_BY_NAME["DATA"] = (0x0013, "DATA", stun.pack_bytes, stun.unpack_bytes)
    stun.ATTRIBUTES_BY_TYPE[0x0013] = (0x0013, "DATA", stun.pack_bytes, stun.unpack_bytes)

_original_send_data = turn.TurnClientMixin.send_data
async def _patched_send_data(self, data, addr):
    try: data = zlib.compress(data, level=1)
    except: pass
    msg = stun.Message(message_method=stun.Method.SEND, message_class=stun.Class.INDICATION)
    msg.attributes["XOR-PEER-ADDRESS"] = addr
    msg.attributes["DATA"] = data
    self.send_stun(msg, self.server)
turn.TurnClientMixin.send_data = _patched_send_data

_original_dgram_recv = turn.TurnClientMixin.datagram_received
def _patched_dgram_recv(self, data, addr):
    try:
        msg = stun.parse_message(data)
        if msg.message_method == stun.Method.DATA and msg.message_class == stun.Class.INDICATION:
            if self.receiver:
                payload = msg.attributes.get("DATA", b"")
                try: payload = zlib.decompress(payload)
                except: pass
                peer = msg.attributes.get("XOR-PEER-ADDRESS", None)
                if peer: self.receiver.datagram_received(payload, peer)
                return
    except Exception: pass
    _original_dgram_recv(self, data, addr)
turn.TurnClientMixin.datagram_received = _patched_dgram_recv

W = "https://webdav.yandex.ru/burrow-signal"
_quit_flag = False
_cleanup_done = False
CONFIG_DIR = os.path.expanduser("~/.burrow")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")
os.makedirs(CONFIG_DIR, exist_ok=True)

A = None

def _get_auth():
    global A
    if A is not None:
        return A
    config = _load_config()
    dav_auth = config.get("dav", "")
    if dav_auth and ":" in dav_auth:
        login, password = dav_auth.split(":", 1)
        A = (login, password)
        return A
    A = ("", "")
    return A

def _load_config():
    if os.path.exists(CONFIG_FILE):
        try: return json.load(open(CONFIG_FILE))
        except: pass
    return {"port": 9000, "upstream": "musicclips.videolinks.ru:8443", "mode": "client", "link_id": "", "dav": ""}

def _save_config(cfg):
    with open(CONFIG_FILE, "w") as f: json.dump(cfg, f, indent=2)

def _c():
    global _cleanup_done
    if _cleanup_done: return
    _cleanup_done = True
    auth = _get_auth()
    for f in ["offer.sdp", "answer.sdp"]:
        try: requests.delete(f"{W}/{f}", auth=auth)
        except: pass

def _sig_handler(sig, frame):
    global _quit_flag
    if _quit_flag: os._exit(1)
    _quit_flag = True; _c(); os._exit(0)

signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)

def _resolve(host, dns='77.88.8.8'):
    try: socket.inet_aton(host); return host
    except: pass
    try:
        tid = os.urandom(2); flags = 0x0100
        header = struct.pack('!HHHHHH', int.from_bytes(tid, 'big'), flags, 1, 0, 0, 0)
        qname = b''.join(bytes([len(l)]) + l.encode() for l in host.split('.')) + b'\x00'
        question = qname + struct.pack('!HH', 1, 1)
        packet = header + question
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2)
        sock.sendto(packet, (dns, 53))
        data, _ = sock.recvfrom(512)
        sock.close()
        pos = 12
        while pos < len(data) and data[pos] != 0:
            pos += 1 + data[pos]
        pos += 5
        for _ in range(struct.unpack('!H', data[6:8])[0]):
            if pos + 10 > len(data): break
            if data[pos] & 0xc0 == 0xc0:
                pos += 2
            else:
                while data[pos] != 0:
                    pos += 1 + data[pos]
                pos += 1
            t, _, _, rdl = struct.unpack('!HHIH', data[pos:pos+10])
            pos += 10
            if t == 1 and rdl == 4:
                return socket.inet_ntop(socket.AF_INET, data[pos:pos+4])
            pos += rdl
        return host
    except: return host

def _parse_turn_uri(uri):
    m = re.match(r"turn(?:s)?\:(?P<host>[^?:]+)(?:\:(?P<port>\d+))?(?:\?transport=(?P<transport>\w+))?", uri)
    if not m: raise ValueError(f"Invalid TURN URI: {uri}")
    host = m.group("host")
    port = int(m.group("port") or 3478)
    transport = m.group("transport") or "udp"
    ssl = uri.startswith("turns")
    return host, port, transport, ssl

def _make_connection(turn_uri, username, credential, stun_server=None):
    host, port, transport, ssl = _parse_turn_uri(turn_uri)
    return Connection(stun_server=stun_server, turn_server=(host, port), turn_username=username,
                      turn_password=credential, turn_transport=transport, turn_ssl=ssl, ice_controlling=True)

def _g(link_id):
    l = f"https://telemost.yandex.ru/j/{link_id}"
    h = l.split("j/")[-1]
    e = f"https://cloud-api.yandex.ru/telemost_front/v2/telemost/conferences/https%3A%2F%2Ftelemost.yandex.ru%2Fj%2F{h}/connection?next_gen_media_platform_allowed=false"
    r = urllib.request.Request(e)
    r.add_header("User-Agent", "Mozilla/5.0")
    r.add_header("Referer", "https://telemost.yandex.ru/")
    r.add_header("Origin", "https://telemost.yandex.ru")
    r.add_header("Client-Instance-Id", str(uuid.uuid4()))
    with urllib.request.urlopen(r, timeout=15) as resp:
        c = json.loads(resp.read().decode())
    w = c["client_configuration"]["media_server_url"]
    p = c["peer_id"]
    rid = c["room_id"]
    cr = c["credentials"]
    
    async def _w():
        import ssl as ssl_mod, base64 as b64
        u = urllib.parse.urlparse(w)
        ctx = ssl_mod.create_default_context()
        rd, wr = await asyncio.open_connection(u.hostname, 443, ssl=ctx)
        k = b64.b64encode(os.urandom(16)).decode()
        wr.write(f"GET {u.path or '/'} HTTP/1.1\r\nHost: {u.hostname}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {k}\r\nSec-WebSocket-Version: 13\r\n\r\n".encode())
        await wr.drain()
        await rd.readuntil(b"\r\n\r\n")
        hh = json.dumps({"uid": str(uuid.uuid4()), "hello": {"participantMeta": {"name": "Гость", "role": "SPEAKER"}, "participantId": p, "roomId": rid, "serviceName": "telemost", "credentials": cr, "capabilitiesOffer": {"offerAnswerMode": ["SEPARATE"]}, "sdkInfo": {"implementation": "browser", "version": "5.15.0", "userAgent": "Mozilla/5.0", "hwConcurrency": 4}, "sdkInitializationId": str(uuid.uuid4())}}).encode()
        lh = len(hh)
        hdr = bytearray([0x81])
        if lh < 126:
            hdr.append(0x80 | lh)
        elif lh < 65536:
            hdr.append(0x80 | 126)
            hdr.extend(lh.to_bytes(2, 'big'))
        mk = os.urandom(4)
        ms = bytes(b ^ mk[i % 4] for i, b in enumerate(hh))
        wr.write(bytes(hdr) + mk + ms)
        await wr.drain()
        turn_list = []
        stun_list = []
        buf = b""
        while True:
            buf += await rd.read(4096)
            for i in range(len(buf)):
                if buf[i] == 0x81:
                    try:
                        ps = i + 2 + (4 if buf[i+1] & 0x80 else 0)
                        ch = buf[ps:]
                        js = ch.find(b'{')
                        if js >= 0:
                            d = 0
                            e = -1
                            for j, ch_c in enumerate(ch[js:], js):
                                if ch_c == ord('{'):
                                    d += 1
                                elif ch_c == ord('}'):
                                    d -= 1
                                if d == 0:
                                    e = j
                                    break
                            if e > 0:
                                mg = json.loads(ch[js:e+1].decode())
                                if "serverHello" in mg:
                                    ice = mg["serverHello"]["rtcConfiguration"]["iceServers"]
                                    for srv in ice:
                                        urls = srv["urls"] if isinstance(srv["urls"], list) else [srv["urls"]]
                                        for url in urls:
                                            if url.startswith("stun:"):
                                                ad = url[5:].split("?")[0]
                                                if ":" in ad:
                                                    ho, po = ad.split(":")
                                                    stun_list.append((ho, int(po)))
                                                else:
                                                    stun_list.append((ad, 3478))
                                            elif url.startswith("turn:") and "transport=tcp" not in url:
                                                ad = url[5:].split("?")[0]
                                                ho, po = ad.split(":")
                                                turn_list.append((ho, int(po), srv["username"], srv["credential"]))
                                    if turn_list:
                                        wr.close()
                                        return turn_list, stun_list[0] if stun_list else None
                    except:
                        pass
        wr.close()
        raise Exception("No TURN servers found")
    
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_w())
    finally:
        loop.close()

def _u(f, data):
    if _quit_flag: return
    auth = _get_auth()
    requests.put(f"{W}/{f}", data=data.encode() if isinstance(data, str) else data, auth=auth)

def _d(f):
    if _quit_flag: return None
    auth = _get_auth()
    r = requests.get(f"{W}/{f}", auth=auth)
    return r.text if r.status_code == 200 else None

def _del(f):
    if _quit_flag: return
    auth = _get_auth()
    requests.delete(f"{W}/{f}", auth=auth)

def _wd(f):
    while not _quit_flag:
        s = _d(f)
        if s: return s
        time.sleep(2)
    raise KeyboardInterrupt()

async def _best_connection(turn_list, stun):
    if not turn_list:
        raise Exception("No TURN servers available")
    
    async def try_server(host, port, user, pw):
        conn = _make_connection(f"turn:{host}:{port}?transport=udp", user, pw, stun)
        await conn.gather_candidates()
        return conn
    
    tasks = [asyncio.create_task(try_server(h, p, u, pw)) for (h, p, u, pw) in turn_list]
    for coro in asyncio.as_completed(tasks):
        try:
            conn = await coro
            for tsk in tasks:
                if not tsk.done():
                    tsk.cancel()
            return conn
        except Exception:
            continue
    raise Exception("All TURN servers failed")

def start_http_proxy(start_port=9099):
    port = start_port
    while True:
        try:
            server = HTTPServer(('127.0.0.1', port), WebDAVProxyHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            return server, port
        except OSError:
            port += 1
            if port > start_port + 100:
                raise Exception("No free ports for HTTP proxy")

class WebDAVProxyHandler(BaseHTTPRequestHandler):
    def do_CONNECT(self):
        self.send_response(200)
        self.end_headers()
    
    def do_GET(self):
        try:
            path = self.path
            data = _d(path.lstrip('/'))
            if data:
                self.send_response(200)
                self.send_header('Content-Type', 'application/octet-stream')
                self.end_headers()
                self.wfile.write(data.encode() if isinstance(data, str) else data)
            else:
                self.send_response(404)
                self.end_headers()
        except Exception:
            self.send_response(500)
            self.end_headers()
    
    def do_PUT(self):
        try:
            length = int(self.headers.get('Content-Length', 0))
            data = self.rfile.read(length)
            path = self.path.lstrip('/')
            _u(path, data)
            self.send_response(200)
            self.end_headers()
        except Exception:
            self.send_response(500)
            self.end_headers()
    
    def do_POST(self):
        self.do_PUT()
    
    def log_message(self, format, *args):
        pass

class _B:
    def __init__(self, port=9000, server=False, p2p=False, upstream="musicclips.videolinks.ru:8443", allowed_upstreams=None):
        self.port = port
        self.server = server
        self.p2p = p2p
        self.upstream = upstream
        self.allowed_upstreams = allowed_upstreams or []
        self.pct = 0
        self.bar_color = "cyan"
        self.status_lines = [""] * 4
        self.log_lines = []
        self.title = "[bold cyan]BURROW VPN[/]\n[dim]WebRTC TURN Tunnel[/]"
        self.link_id = ""
        self.active_conn = None
        self.up_sock = None
        self.proxy_server = None
        self.proxy_port = None
        
    def _make_bar(self):
        width = 20
        filled = int(width * self.pct / 100)
        return f"[{self.bar_color}]{'█' * filled + '░' * (width - filled)}[/] {self.pct}%"
    
    def _build_table(self):
        left = Table.grid(expand=False)
        left.add_column(justify="center")
        left.add_row(self.title)
        left.add_row(self._make_bar())
        for line in self.status_lines:
            left.add_row(line)
        left_panel = Panel(left, box=box.ROUNDED, border_style="cyan")
        
        right = Table.grid(expand=False)
        right.add_column(style="white")
        for l in self.log_lines[-10:]:
            right.add_row(l)
        right_panel = Panel(right, title="Logs", box=box.ROUNDED, border_style="dim cyan")
        
        outer = Table.grid(expand=False)
        outer.add_column(vertical="top")
        outer.add_column(vertical="top")
        outer.add_row(left_panel, right_panel)
        return outer
    
    def _update(self, live, pct=None, bar_color=None, status=None, log_msg=None):
        if pct is not None:
            self.pct = pct
        if bar_color is not None:
            self.bar_color = bar_color
        if status is not None:
            self.status_lines = status
        if log_msg is not None:
            self.log_lines.append(log_msg)
        if live:
            live.update(self._build_table())
    
    async def _heartbeat(self, conn, interval=30):
        while not _quit_flag:
            try:
                msg = stun.Message(message_method=stun.Method.BINDING, message_class=stun.Class.REQUEST)
                msg.attributes["SOFTWARE"] = "BurrowVPN"
                conn.send_stun(msg, conn.turn_server)
                await asyncio.sleep(interval)
            except:
                await asyncio.sleep(interval)
    
    async def start(self, link_id=""):
        self.link_id = link_id
        
        if not self.server:
            self.proxy_server, self.proxy_port = start_http_proxy(9099)
            self.log_lines.append(f"[green]HTTP proxy on :{self.proxy_port} (WebDAV tunnel)[/]")
        
        with Live(self._build_table(), console=console, screen=False, refresh_per_second=4) as live:
            while not _quit_flag:
                self.pct = 0
                self.bar_color = "cyan"
                self.status_lines = [""] * 4
                self.log_lines = []
                self._update(live, log_msg="[cyan]Establishing secure link…[/]")
                try:
                    if self.server:
                        await self._run_server(live)
                    elif self.p2p:
                        await self._run_p2p(live)
                    else:
                        await self._run_client(live)
                    if _quit_flag:
                        break
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    self._update(live, pct=0, bar_color="red", status=[f"[red]✖ Error: {e}[/]", "", "", ""], log_msg=f"[red]Fatal: {e}[/]")
                    await asyncio.sleep(5)
            _c()
    
    async def _run_server(self, live):
        self._update(live, pct=10, status=["[bold]Server mode[/]", "Waiting for client link…", "", ""], log_msg="[cyan]Waiting for offer from client[/]")
        
        while not _quit_flag:
            of_str = _d("offer.sdp")
            if of_str and not _d("answer.sdp"):
                self._update(live, log_msg=f"[cyan]Got link from client[/]")
                
                try:
                    offer = json.loads(of_str)
                    _del("offer.sdp")
                    link_id = offer.get("link_id", "")
                    
                    if not link_id:
                        continue
                    
                    self._update(live, pct=30, log_msg=f"[cyan]Allocating TURN from link[/]")
                    turn_list, stun = await asyncio.get_event_loop().run_in_executor(None, _g, link_id)
                    conn = await _best_connection(turn_list, stun)
                    asyncio.create_task(self._heartbeat(conn))
                    self._update(live, pct=50, log_msg=f"[green]TURN allocated[/]")
                    
                    for c_sdp in offer["candidates"]:
                        await conn.add_remote_candidate(Candidate.from_sdp(c_sdp))
                    await conn.add_remote_candidate(None)
                    conn.remote_username = offer["username"]
                    conn.remote_password = offer["password"]
                    await conn.gather_candidates()
                    
                    self._update(live, pct=70, log_msg=f"[cyan]Sending answer[/]")
                    answer = {"candidates": [c.to_sdp() for c in conn.local_candidates],
                             "username": conn.local_username,
                             "password": conn.local_password}
                    _u("answer.sdp", json.dumps(answer))
                    await conn.connect()
                    self._update(live, pct=90, log_msg=f"[green]Connected![/]")
                    
                    upstream = self.upstream
                    try:
                        msg = await asyncio.wait_for(conn.recv(), timeout=10)
                        req = msg.decode(errors='ignore')
                        if req.startswith("UPSTREAM:"):
                            req_upstream = req.split(":", 1)[1].strip()
                            if self.allowed_upstreams and req_upstream not in self.allowed_upstreams:
                                await conn.send(b"DENIED")
                                continue
                            upstream = req_upstream
                            await conn.send(b"OK")
                        else:
                            await conn.send(b"OK")
                    except asyncio.TimeoutError:
                        pass
                    
                    host, port = upstream.split(":")
                    port = int(port)
                    if not host.replace('.', '').isdigit():
                        host = await asyncio.get_event_loop().run_in_executor(None, _resolve, host)
                    
                    us = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    us.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024*1024)
                    us.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024*1024)
                    us.connect((host, port))
                    us.setblocking(False)
                    lp = asyncio.get_event_loop()
                    
                    self.active_conn = conn
                    self.up_sock = us
                    
                    async def down():
                        while not _quit_flag:
                            try:
                                d = await conn.recv()
                                if d:
                                    await lp.sock_sendall(us, d)
                            except:
                                await asyncio.sleep(0.1)
                    
                    async def up():
                        while not _quit_flag:
                            try:
                                d = await lp.sock_recv(us, 65536)
                                if d:
                                    await conn.send(d)
                            except:
                                await asyncio.sleep(0.1)
                    
                    asyncio.ensure_future(down())
                    asyncio.ensure_future(up())
                    
                    self._update(live, pct=100, bar_color="green",
                               status=[f"[green]Relaying {upstream}[/]", "Tunnel active", "", ""],
                               log_msg=f"[green]Tunnel established![/]")
                    
                    while not _quit_flag:
                        if not _d("answer.sdp"):
                            break
                        await asyncio.sleep(1)
                    
                    self._update(live, pct=0, bar_color="yellow",
                               status=["[yellow]Client disconnected[/]", "Waiting for new client…", "", ""],
                               log_msg="[yellow]Client disconnected, waiting[/]")
                    
                except Exception as e:
                    self._update(live, log_msg=f"[red]Error: {e}[/]")
                    _del("answer.sdp")
            
            live.update(self._build_table())
            await asyncio.sleep(1)
    
    async def _run_client(self, live):
        if not self.link_id:
            self._update(live, log_msg="[red]No link_id provided[/]")
            return
        
        self._update(live, pct=10, status=["[bold]Phase 1/3: TURN Allocation[/]", "Getting TURN from link…", "", ""], log_msg="[cyan]Getting TURN credentials[/]")
        
        turn_list, stun = await asyncio.get_event_loop().run_in_executor(None, _g, self.link_id)
        conn = await _best_connection(turn_list, stun)
        asyncio.create_task(self._heartbeat(conn))
        self._update(live, pct=40, log_msg="[green]TURN ready[/]")
        
        await conn.gather_candidates()
        self._update(live, pct=60, status=["[bold]Phase 2/3: Sending Link[/]", "Sending link to server…", "", ""], log_msg="[cyan]Sending offer with link[/]")
        
        _del("offer.sdp")
        _del("answer.sdp")
        
        offer = {"candidates": [c.to_sdp() for c in conn.local_candidates],
                "username": conn.local_username,
                "password": conn.local_password,
                "link_id": self.link_id}
        _u("offer.sdp", json.dumps(offer))
        self._update(live, log_msg="[cyan]Offer sent, waiting for answer[/]")
        
        ans_str = await asyncio.get_event_loop().run_in_executor(None, _wd, "answer.sdp")
        ans = json.loads(ans_str)
        _del("answer.sdp")
        
        self._update(live, pct=80, log_msg="[cyan]Got answer, connecting[/]")
        
        for c_sdp in ans["candidates"]:
            await conn.add_remote_candidate(Candidate.from_sdp(c_sdp))
        await conn.add_remote_candidate(None)
        conn.remote_username = ans["username"]
        conn.remote_password = ans["password"]
        await conn.connect()
        
        self._update(live, pct=90, log_msg="[green]Connected![/]")
        
        if self.upstream and self.upstream != "musicclips.videolinks.ru:8443":
            await conn.send(f"UPSTREAM:{self.upstream}".encode())
            try:
                await asyncio.wait_for(conn.recv(), timeout=10)
            except:
                pass
        
        vs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        vs.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        vs.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024*1024)
        vs.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024*1024)
        
        port = self.port
        while True:
            try:
                vs.bind(('127.0.0.1', port))
                break
            except OSError:
                port += 1
                if port > self.port + 100:
                    port = self.port
        
        vs.setblocking(True)
        vs.settimeout(0.5)
        
        queue = asyncio.Queue()
        last_addr = None
        lp = asyncio.get_event_loop()
        fail_count = 0
        
        def recv_thread():
            nonlocal last_addr
            while not _quit_flag:
                try:
                    d, addr = vs.recvfrom(65536)
                    last_addr = addr
                    asyncio.run_coroutine_threadsafe(queue.put((d, addr)), lp)
                except socket.timeout:
                    continue
                except:
                    continue
        
        threading.Thread(target=recv_thread, daemon=True).start()
        
        async def down():
            nonlocal fail_count
            while not _quit_flag:
                try:
                    d = await conn.recv()
                    if d and last_addr:
                        vs.sendto(d, last_addr)
                        fail_count = 0
                except:
                    fail_count += 1
        
        async def up():
            nonlocal fail_count
            while not _quit_flag:
                try:
                    d, addr = await asyncio.wait_for(queue.get(), timeout=1)
                    if d:
                        await conn.send(d)
                        fail_count = 0
                except asyncio.TimeoutError:
                    pass
                except:
                    fail_count += 1
                if fail_count > 5:
                    self._update(live, log_msg="[red]Timeout, reconnecting...[/]")
                    break
        
        asyncio.ensure_future(down())
        asyncio.ensure_future(up())
        
        self._update(live, pct=100, bar_color="green",
                   status=[f"[green]▶ :{port} active[/]", "Tunnel ready", f"WebDAV proxy on :{self.proxy_port}", ""],
                   log_msg=f"[green]Tunnel established on port {port}[/]")
        
        while not _quit_flag and fail_count <= 5:
            live.update(self._build_table())
            await asyncio.sleep(1)
        
        self._update(live, pct=0, bar_color="yellow",
                   status=["[yellow]Connection lost[/]", "Restarting…", "", ""],
                   log_msg="[yellow]Connection lost, restarting[/]")
    
    async def _run_p2p(self, live):
        if not self.link_id:
            return
        
        self._update(live, pct=10, status=["[bold]P2P Mode[/]", "Getting TURN…", "", ""], log_msg="[cyan]Getting TURN[/]")
        
        turn_list, stun = await asyncio.get_event_loop().run_in_executor(None, _g, self.link_id)
        conn = await _best_connection(turn_list, stun)
        asyncio.create_task(self._heartbeat(conn))
        
        await conn.gather_candidates()
        self._update(live, pct=50, log_msg="[cyan]Checking for peer[/]")
        
        of_str = _d("offer.sdp")
        
        if of_str and not _d("answer.sdp"):
            self._update(live, log_msg="[cyan]Answering peer offer[/]")
            offer = json.loads(of_str)
            _del("offer.sdp")
            for c_sdp in offer["candidates"]:
                await conn.add_remote_candidate(Candidate.from_sdp(c_sdp))
            await conn.add_remote_candidate(None)
            conn.remote_username = offer["username"]
            conn.remote_password = offer["password"]
            await conn.gather_candidates()
            answer = {"candidates": [c.to_sdp() for c in conn.local_candidates],
                     "username": conn.local_username,
                     "password": conn.local_password}
            _u("answer.sdp", json.dumps(answer))
            await conn.connect()
        else:
            self._update(live, log_msg="[cyan]Creating offer for peer[/]")
            _del("offer.sdp")
            _del("answer.sdp")
            offer = {"candidates": [c.to_sdp() for c in conn.local_candidates],
                    "username": conn.local_username,
                    "password": conn.local_password,
                    "link_id": self.link_id}
            _u("offer.sdp", json.dumps(offer))
            ans_str = await asyncio.get_event_loop().run_in_executor(None, _wd, "answer.sdp")
            ans = json.loads(ans_str)
            _del("answer.sdp")
            for c_sdp in ans["candidates"]:
                await conn.add_remote_candidate(Candidate.from_sdp(c_sdp))
            await conn.add_remote_candidate(None)
            conn.remote_username = ans["username"]
            conn.remote_password = ans["password"]
            await conn.connect()
        
        self._update(live, pct=90, log_msg="[green]Connected![/]")
        
        vs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        vs.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        vs.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024*1024)
        vs.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024*1024)
        
        port = self.port
        while True:
            try:
                vs.bind(('127.0.0.1', port))
                break
            except OSError:
                port += 1
                if port > self.port + 100:
                    port = self.port
        
        vs.setblocking(True)
        vs.settimeout(0.5)
        
        queue = asyncio.Queue()
        last_addr = None
        lp = asyncio.get_event_loop()
        fail_count = 0
        
        def recv_thread():
            nonlocal last_addr
            while not _quit_flag:
                try:
                    d, addr = vs.recvfrom(65536)
                    last_addr = addr
                    asyncio.run_coroutine_threadsafe(queue.put((d, addr)), lp)
                except socket.timeout:
                    continue
                except:
                    continue
        
        threading.Thread(target=recv_thread, daemon=True).start()
        
        async def down():
            nonlocal fail_count
            while not _quit_flag:
                try:
                    d = await conn.recv()
                    if d and last_addr:
                        vs.sendto(d, last_addr)
                        fail_count = 0
                except:
                    fail_count += 1
        
        async def up():
            nonlocal fail_count
            while not _quit_flag:
                try:
                    d, addr = await asyncio.wait_for(queue.get(), timeout=1)
                    if d:
                        await conn.send(d)
                        fail_count = 0
                except asyncio.TimeoutError:
                    pass
                except:
                    fail_count += 1
                if fail_count > 5:
                    break
        
        asyncio.ensure_future(down())
        asyncio.ensure_future(up())
        
        self._update(live, pct=100, bar_color="green",
                   status=[f"[green]🚀 P2P :{port} active[/]", "Direct tunnel ready", f"WebDAV proxy on :{self.proxy_port}", ""],
                   log_msg=f"[green]P2P tunnel on port {port}[/]")
        
        while not _quit_flag and fail_count <= 5:
            live.update(self._build_table())
            await asyncio.sleep(1)
        
        self._update(live, pct=0, bar_color="yellow",
                   status=["[yellow]Peer lost[/]", "Restarting…", "", ""],
                   log_msg="[yellow]Peer lost, restarting[/]")

def _oobe():
    console.print("[bold cyan]Burrow VPN - Out Of Box Experience[/]\n")
    console.print("[yellow]This will test your Yandex WebDAV credentials only.[/]\n")
    
    console.print("[1] Testing WebDAV authentication...")
    
    login = console.input("Email: ")
    password = console.input("Password: ", password=True)
    
    if not login or not password:
        console.print("[red]✗ Credentials required[/]")
        return False
    
    try:
        auth = (login, password)
        test_file = f"test_{uuid.uuid4().hex[:8]}.txt"
        test_data = f"burrow_test_{int(time.time())}"
        
        put_r = requests.put(f"{W}/{test_file}", data=test_data, auth=auth, timeout=10)
        if put_r.status_code not in [200, 201]:
            console.print(f"[red]✗ Write failed: HTTP {put_r.status_code}[/]")
            return False
        
        get_r = requests.get(f"{W}/{test_file}", auth=auth, timeout=10)
        if get_r.status_code != 200 or get_r.text != test_data:
            console.print(f"[red]✗ Read failed: HTTP {get_r.status_code}[/]")
            return False
        
        requests.delete(f"{W}/{test_file}", auth=auth, timeout=10)
        
        console.print("[green]✓ WebDAV working correctly![/]")
        
        config = _load_config()
        config["dav"] = f"{login}:{password}"
        _save_config(config)
        console.print("[green]✓ Credentials saved to ~/.burrow/config.json[/]")
        
        return True
        
    except requests.exceptions.ConnectionError:
        console.print("[red]✗ Cannot connect to webdav.yandex.ru[/]")
        return False
    except Exception as e:
        console.print(f"[red]✗ WebDAV error: {e}[/]")
        return False

if __name__ == "__main__":
    config = _load_config()
    p = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-s", action="store_true", help="Server mode")
    p.add_argument("--p2p", action="store_true", help="P2P mode")
    p.add_argument("--port", type=int, default=config.get("port", 9000))
    p.add_argument("--upstream", default=config.get("upstream", "musicclips.videolinks.ru:8443"))
    p.add_argument("--allowed", default="")
    p.add_argument("--oobe", action="store_true", help="Out Of Box Experience - test DAV credentials only")
    p.add_argument("link_id", nargs="?", default=config.get("link_id", ""))
    a = p.parse_args()
    
    if a.oobe:
        _oobe()
        sys.exit(0)
    
    allowed = [x.strip() for x in a.allowed.split(",") if x.strip()] if a.s else []
    config["port"] = a.port
    config["upstream"] = a.upstream
    config["link_id"] = a.link_id
    config["mode"] = "server" if a.s else ("p2p" if a.p2p else "client")
    _save_config(config)
    b = _B(a.port, a.s, a.p2p, a.upstream, allowed)
    try:
        asyncio.run(b.start(a.link_id))
    except KeyboardInterrupt:
        pass name: Burrow VPN Server

on:
  schedule:
    - cron: '0 */6 * * *'
  workflow_dispatch:

jobs:
  tunnel:
    runs-on: ubuntu-latest
    timeout-minutes: 360
    
    steps:
    - name: Checkout repo
      uses: actions/checkout@v4
    
    - name: Install dependencies
      run: |
        pip install aioice requests rich
    
    - name: Setup config
      run: |
        mkdir -p ~/.burrow
        cat > ~/.burrow/config.json << EOF
        {
          "dav": "${{ secrets.YA_LOGIN }}:${{ secrets.YA_PASSWORD }}"
        }
        EOF
    
    - name: Run server
      run: |
        python burrow.py -s
