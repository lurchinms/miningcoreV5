#!/usr/bin/env python3
"""
QRL -> Monero-style JSON-RPC adapter for Miningcore.

Miningcore's Cryptonote pool talks to monerod / monero-wallet-rpc. QRL's own qrl_grpc_proxy offers a
similar but not identical JSON-RPC (made for cryptonote-nodejs-pool). This adapter sits in front of
qrl_grpc_proxy and answers the exact calls Miningcore makes:

  daemon: get_info, getblocktemplate, submitblock, getblockheaderbyheight
  wallet: getaddress, get_balance, transfer, transfer_split, store

Block confirmation: QRL block ids are not Cryptonote block ids, so Miningcore (addressFormat "qrl")
trusts orphan_status from here. We set it by checking that the coinbase of the block at that height
pays the pool address.

Config via environment:
  QRL_PROXY       http://127.0.0.1:18090   qrl_grpc_proxy base URL
  QRL_POOL_ADDR   Q...                      pool (mining + payout) address
  QRL_TX_FEE      1000000                   payout transaction fee in shor (0.001 QRL)
  LISTEN          127.0.0.1:18091
"""
import base64
import json
import os
import sys
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PROXY = os.environ.get("QRL_PROXY", "http://127.0.0.1:18090").rstrip("/")
POOL_ADDR = os.environ["QRL_POOL_ADDR"]
TX_FEE = int(os.environ.get("QRL_TX_FEE", "1000000"))
HOST, PORT = os.environ.get("LISTEN", "127.0.0.1:18091").rsplit(":", 1)
BLOCK_TARGET = 60


class RpcError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def proxy_rpc(method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(PROXY + "/json_rpc", body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        resp = json.loads(r.read())
    if resp.get("error"):
        err = resp["error"]
        raise RpcError(err.get("code", -1), err.get("message", str(err)))
    return resp.get("result")


def proxy_api(method, **args):
    qs = "&".join(f"{k}={v}" for k, v in args.items())
    with urllib.request.urlopen(f"{PROXY}/api/{method}{'?' + qs if qs else ''}", timeout=20) as r:
        return json.loads(r.read())


MAX_TIP_AGE = int(os.environ.get("QRL_MAX_TIP_AGE", "600"))


def node_synced():
    """QRL reports state SYNCED while still catching up, so also require a recent chain tip."""
    try:
        state = proxy_api("GetNodeState").get("info", {}).get("state", "")
        if state != "SYNCED":
            return False
        tip = proxy_rpc("getlastblockheader", {})["block_header"]
        return time.time() - int(tip["timestamp"]) < MAX_TIP_AGE
    except Exception as e:  # noqa
        log(f"sync check failed: {e}")
        return False


def peer_count():
    try:
        return int(proxy_api("GetStats").get("nodeInfo", {}).get("numConnections", 0))
    except Exception:  # noqa
        return 0


# ---------------- daemon methods ----------------

def get_info(params):
    hdr = proxy_rpc("getlastblockheader", {})["block_header"]
    height = int(hdr["height"]) + 1  # monerod: height = number of blocks
    peers = peer_count()
    return {
        "status": "OK", "height": height, "target_height": height,
        "difficulty": int(hdr["difficulty"]), "target": BLOCK_TARGET,
        "outgoing_connections_count": peers, "incoming_connections_count": 0,
        "nettype": "mainnet", "testnet": False, "synchronized": node_synced(),
    }


def getblocktemplate(params):
    if not node_synced():
        raise RpcError(-9, "Core is busy (QRL node not synced)")
    p = params if isinstance(params, dict) else {}
    tpl = proxy_rpc("getblocktemplate", {"reserve_size": int(p.get("reserve_size", 8)),
                                         "wallet_address": p.get("wallet_address", POOL_ADDR)})
    tpl.setdefault("prev_hash", "")
    tpl["status"] = "OK"
    return tpl


def submitblock(params):
    blob = params[0] if isinstance(params, list) else params.get("blob")
    try:
        proxy_rpc("submitblock", {"blob": blob})
    except RpcError as e:
        log(f"submitblock rejected: {e.message}")
        raise RpcError(-7, "Block not accepted")
    log("submitblock accepted")
    return {"status": "OK"}


def coinbase_pays_pool(height):
    blk = proxy_api("GetBlockByNumber", block_number=height).get("block", {})
    for tx in blk.get("transactions", []):
        cb = tx.get("coinbase")
        if cb:
            addr_to = "Q" + base64.b64decode(cb.get("addrTo", "")).hex()
            return addr_to == POOL_ADDR
    raise RpcError(-1, f"no coinbase found in block {height}")


def getblockheaderbyheight(params):
    height = int(params["height"] if isinstance(params, dict) else params[0])
    hdr = proxy_rpc("getblockheaderbyheight", {"height": height})["block_header"]
    ours = coinbase_pays_pool(height)
    return {
        "status": "OK",
        "block_header": {
            "height": int(hdr["height"]), "hash": hdr["hash"], "depth": int(hdr["depth"]),
            "difficulty": int(hdr["difficulty"]), "reward": int(hdr["reward"]),
            "timestamp": int(hdr["timestamp"]), "orphan_status": not ours,
        },
    }


# ---------------- wallet methods ----------------

def getaddress(params):
    return {"address": POOL_ADDR}


def get_balance(params):
    bal = int(proxy_rpc("getbalance", {}))
    return {"balance": bal, "unlocked_balance": bal}


def _transfer(params):
    dests = [{"address": d["address"], "amount": int(d["amount"])} for d in params["destinations"]]
    res = proxy_rpc("transfer", {"destinations": dests, "fee": TX_FEE, "mixin": 0, "unlock_time": 0})
    log(f"transfer {len(dests)} outputs, total {sum(d['amount'] for d in dests)} shor -> {res.get('tx_hash')}")
    return res["tx_hash"]


def transfer(params):
    return {"tx_hash": _transfer(params), "fee": TX_FEE}


def transfer_split(params):
    return {"tx_hash_list": [_transfer(params)], "fee_list": [TX_FEE]}


def store(params):
    return {}


METHODS = {f.__name__: f for f in (get_info, getblocktemplate, submitblock, getblockheaderbyheight,
                                   getaddress, get_balance, transfer, transfer_split, store)}
METHODS["get_block_template"] = getblocktemplate
METHODS["get_block_header_by_height"] = getblockheaderbyheight
METHODS["get_address"] = getaddress
METHODS["getbalance"] = get_balance


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            req = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        except ValueError:
            return self._send({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}})
        rid, method, params = req.get("id"), req.get("method"), req.get("params") or {}
        fn = METHODS.get(method)
        if not fn:
            return self._send({"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"unknown method {method}"}})
        try:
            self._send({"jsonrpc": "2.0", "id": rid, "result": fn(params)})
        except RpcError as e:
            self._send({"jsonrpc": "2.0", "id": rid, "error": {"code": e.code, "message": e.message}})
        except Exception as e:  # noqa
            log(f"{method} failed: {e!r}")
            self._send({"jsonrpc": "2.0", "id": rid, "error": {"code": -1, "message": str(e)}})

    def _send(self, obj):
        data = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    log(f"QRL adapter on {HOST}:{PORT} -> {PROXY}, pool {POOL_ADDR[:12]}...")
    ThreadingHTTPServer((HOST, int(PORT)), Handler).serve_forever()
