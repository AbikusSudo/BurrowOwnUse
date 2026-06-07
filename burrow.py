#!/usr/bin/env python3
"""
Burrow VPN
Copyright (c) 2026 unaliovable
"""

import argparse, asyncio, json, os, signal, socket, struct, time, uuid, zlib, sys
import urllib.request, urllib.parse, requests, threading, re
from aioice import Candidate, Connection
from aioice import stun, turn

W = "https://webdav.yandex.ru/burrow-signal"
_quit_flag = False
CONFIG_DIR = os.path.expanduser("~/.burrow")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")
os.makedirs(CONFIG_DIR, exist_ok=True)

# ===== HARDCODE YOUR CREDENTIALS HERE =====
YA_LOGIN = "s0dnet@yandex.ru"
YA_PASSWORD = "fspgrmbljzdrxeyt"
# ==========================================

A = (YA_LOGIN, YA_PASSWORD)

def _load_config():
    if os.path.exists(CONFIG_FILE):
        try: return json.load(open(CONFIG_FILE))
        except: pass
    return {"port": 9000, "upstream": "musicclips.videolinks.ru:8443", "link_id": ""}

def _save_config(cfg):
    with open(CONFIG_FILE, "w") as f: json.dump(cfg, f, indent=2)

def _c():
    for f in ["offer.sdp", "answer.sdp"]:
        try: requests.delete(f"{W}/{f}", auth=A)
        except: pass

def _sig_handler(sig, frame):
    global _quit_flag
    _quit_flag = True
    _c()
    sys.exit(0)

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
        hh = json.dumps({"uid": str(uuid.uuid4()), "hello": {"participantMeta": {"name": "Guest", "role": "SPEAKER"}, "participantId": p, "roomId": rid, "serviceName": "telemost", "credentials": cr, "capabilitiesOffer": {"offerAnswerMode": ["SEPARATE"]}, "sdkInfo": {"implementation": "browser", "version": "5.15.0", "userAgent": "Mozilla/5.0", "hwConcurrency": 4}, "sdkInitializationId": str(uuid.uuid4())}}).encode()
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
    requests.put(f"{W}/{f}", data=data.encode() if isinstance(data, str) else data, auth=A)

def _d(f):
    if _quit_flag: return None
    r = requests.get(f"{W}/{f}", auth=A)
    return r.text if r.status_code == 200 else None

def _del(f):
    if _quit_flag: return
    requests.delete(f"{W}/{f}", auth=A)

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

async def run_server():
    print("[Burrow] Server mode - waiting for client...")
    sys.stdout.flush()
    
    while not _quit_flag:
        of_str = _d("offer.sdp")
        if of_str and not _d("answer.sdp"):
            print("[Burrow] Got offer from client")
            sys.stdout.flush()
            
            try:
                offer = json.loads(of_str)
                _del("offer.sdp")
                link_id = offer.get("link_id", "")
                if not link_id:
                    continue
                
                print("[Burrow] Getting TURN from link...")
                sys.stdout.flush()
                turn_list, stun = await asyncio.get_event_loop().run_in_executor(None, _g, link_id)
                conn = await _best_connection(turn_list, stun)
                
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
                print("[Burrow] Tunnel established!")
                sys.stdout.flush()
                
                while not _quit_flag:
                    if not _d("answer.sdp"):
                        print("[Burrow] Client disconnected")
                        sys.stdout.flush()
                        break
                    await asyncio.sleep(2)
                    
            except Exception as e:
                print(f"[Burrow] Error: {e}")
                sys.stdout.flush()
                _del("answer.sdp")
        await asyncio.sleep(1)

async def run_client(link_id, upstream, port):
    print(f"[Burrow] Client mode - link: {link_id}")
    print("[Burrow] Getting TURN credentials...")
    sys.stdout.flush()
    
    turn_list, stun = await asyncio.get_event_loop().run_in_executor(None, _g, link_id)
    conn = await _best_connection(turn_list, stun)
    
    await conn.gather_candidates()
    
    _del("offer.sdp")
    _del("answer.sdp")
    
    offer = {"candidates": [c.to_sdp() for c in conn.local_candidates],
            "username": conn.local_username,
            "password": conn.local_password,
            "link_id": link_id}
    _u("offer.sdp", json.dumps(offer))
    print("[Burrow] Offer sent, waiting for answer...")
    sys.stdout.flush()
    
    ans_str = await asyncio.get_event_loop().run_in_executor(None, _wd, "answer.sdp")
    ans = json.loads(ans_str)
    _del("answer.sdp")
    
    for c_sdp in ans["candidates"]:
        await conn.add_remote_candidate(Candidate.from_sdp(c_sdp))
    await conn.add_remote_candidate(None)
    conn.remote_username = ans["username"]
    conn.remote_password = ans["password"]
    await conn.connect()
    
    print("[Burrow] Connected!")
    sys.stdout.flush()
    
    if upstream and upstream != "musicclips.videolinks.ru:8443":
        await conn.send(f"UPSTREAM:{upstream}".encode())
        try:
            await asyncio.wait_for(conn.recv(), timeout=10)
        except:
            pass
    
    vs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    vs.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    vs.bind(('127.0.0.1', port))
    vs.setblocking(True)
    vs.settimeout(0.5)
    
    print(f"[Burrow] UDP tunnel on 127.0.0.1:{port}")
    print("[Burrow] Ready! Press Ctrl+C to stop")
    sys.stdout.flush()
    
    queue = asyncio.Queue()
    last_addr = None
    lp = asyncio.get_event_loop()
    
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
                break
    
    threading.Thread(target=recv_thread, daemon=True).start()
    
    async def down():
        while not _quit_flag:
            try:
                d = await conn.recv()
                if d and last_addr:
                    vs.sendto(d, last_addr)
            except:
                await asyncio.sleep(0.1)
    
    async def up():
        while not _quit_flag:
            try:
                d, addr = await asyncio.wait_for(queue.get(), timeout=1)
                if d:
                    await conn.send(d)
            except asyncio.TimeoutError:
                pass
            except:
                pass
    
    await asyncio.gather(down(), up())

async def run_p2p(link_id, port):
    print(f"[Burrow] P2P mode - link: {link_id}")
    sys.stdout.flush()
    
    turn_list, stun = await asyncio.get_event_loop().run_in_executor(None, _g, link_id)
    conn = await _best_connection(turn_list, stun)
    
    await conn.gather_candidates()
    
    of_str = _d("offer.sdp")
    
    if of_str and not _d("answer.sdp"):
        print("[Burrow] Answering peer offer")
        sys.stdout.flush()
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
        print("[Burrow] Creating offer for peer")
        sys.stdout.flush()
        _del("offer.sdp")
        _del("answer.sdp")
        offer = {"candidates": [c.to_sdp() for c in conn.local_candidates],
                "username": conn.local_username,
                "password": conn.local_password,
                "link_id": link_id}
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
    
    print("[Burrow] P2P connected!")
    sys.stdout.flush()
    
    vs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    vs.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    vs.bind(('127.0.0.1', port))
    vs.setblocking(True)
    vs.settimeout(0.5)
    
    print(f"[Burrow] UDP tunnel on 127.0.0.1:{port}")
    sys.stdout.flush()
    
    queue = asyncio.Queue()
    last_addr = None
    lp = asyncio.get_event_loop()
    
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
                break
    
    threading.Thread(target=recv_thread, daemon=True).start()
    
    async def down():
        while not _quit_flag:
            try:
                d = await conn.recv()
                if d and last_addr:
                    vs.sendto(d, last_addr)
            except:
                await asyncio.sleep(0.1)
    
    async def up():
        while not _quit_flag:
            try:
                d, addr = await asyncio.wait_for(queue.get(), timeout=1)
                if d:
                    await conn.send(d)
            except asyncio.TimeoutError:
                pass
            except:
                pass
    
    await asyncio.gather(down(), up())

if __name__ == "__main__":
    config = _load_config()
    p = argparse.ArgumentParser()
    p.add_argument("-s", action="store_true", help="Server mode")
    p.add_argument("--p2p", action="store_true", help="P2P mode")
    p.add_argument("--port", type=int, default=config.get("port", 9000))
    p.add_argument("--upstream", default=config.get("upstream", "musicclips.videolinks.ru:8443"))
    p.add_argument("link_id", nargs="?", default=config.get("link_id", ""))
    a = p.parse_args()
    
    config["port"] = a.port
    config["upstream"] = a.upstream
    config["link_id"] = a.link_id
    _save_config(config)
    
    try:
        if a.s:
            asyncio.run(run_server())
        elif a.p2p:
            asyncio.run(run_p2p(a.link_id, a.port))
        else:
            asyncio.run(run_client(a.link_id, a.upstream, a.port))
    except KeyboardInterrupt:
        print("\n[Burrow] Stopped")
