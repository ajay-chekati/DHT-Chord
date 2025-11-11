# config.py
M: int = 32                # small for local tests; use 160 for real runs (sha1)
REPLICATION: int = 3       # store metadata on primary + next r-1 successors
RPC_TIMEOUT: float = 2.0
STABILIZE_INTERVAL: float = 1.0
FIX_FINGERS_INTERVAL: float = 1.0
CHECK_PREDECESSOR_INTERVAL: float = 1.0
MAX_RPC_RETRIES: int = 2
REPAIR_REPLICAS_INTERVAL: float = 5.0
REPUBLISH_INTERVAL: float = 30.0