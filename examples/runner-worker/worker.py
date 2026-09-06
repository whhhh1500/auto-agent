"""stdlib private-runner worker; protocol matches main.go and logs no payload."""
import json, os, time, urllib.request
BASE=os.environ["HARNESS_RUNNER_URL"].rstrip("/"); TOKEN=os.environ["HARNESS_RUNNER_TOKEN"]
CAPS=[x.strip() for x in os.getenv("HARNESS_RUNNER_CAPABILITIES", "").split(",") if x.strip()]
def post(path, body):
    req=urllib.request.Request(BASE+path, json.dumps(body).encode(), {"Authorization":"Bearer "+TOKEN,"Content-Type":"application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as r: return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e: return e.code, {}
delay=1
while True:
    try:
        status, task=post("/v1/runners/claim", {"capabilities":CAPS})
        if status==204: time.sleep(1); continue
        if status!=200: raise RuntimeError(status)
        ok=not task.get("cancel_requested",False)
        post("/v1/runners/tasks/%s/complete"%task["id"], {"generation":task["generation"],"ok":ok,"content":"completed "+task["capability"]})
        delay=1
    except KeyboardInterrupt: break
    except Exception: time.sleep(delay); delay=min(delay*2,15)
