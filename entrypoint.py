import os
import sys
import threading

import page_watcher
import metadata_fetcher

def _flag(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")

API_SERVER = _flag("API_SERVER")
WEB_UI = _flag("WEB_UI")
WATCHER = _flag("WATCHER")


def _validate():
    errors = []
    warnings = []

    if not WATCHER and not API_SERVER and not WEB_UI:
        errors.append(
            "No components enabled — set at least one of: WATCHER, API_SERVER, WEB_UI"
        )

    if WATCHER or API_SERVER:
        db_required = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
        missing_db = [v for v in db_required if not os.environ.get(v)]
        if missing_db:
            consumers = " and ".join(c for c, on in [("WATCHER", WATCHER), ("API_SERVER", API_SERVER)] if on)
            for v in missing_db:
                errors.append(f"  {v} is not set (required by {consumers})")

    if WEB_UI and not API_SERVER and not os.environ.get("API_URL"):
        api_port = os.environ.get("API_PORT", "5001")
        warnings.append(
            f"  WEB_UI is enabled but API_SERVER is not and API_URL is not set.\n"
            f"  The web UI will call http://localhost:{api_port} — "
            f"set API_URL if the API server runs on a different host."
        )

    if warnings:
        print("WARNING:")
        for w in warnings:
            print(w)

    if errors:
        print("ERROR: missing required configuration:")
        for e in errors:
            print(e)
        sys.exit(1)


_validate()

if WATCHER:
    watcher_thread = threading.Thread(target=page_watcher.run, name="watcher", daemon=True)
    watcher_thread.start()

    if os.environ.get("PORNDB_API_KEY"):
        threading.Thread(target=metadata_fetcher.run_loop, name="metadata", daemon=True).start()
    else:
        print("PORNDB_API_KEY not set; metadata fetcher disabled.")

import uvicorn

api_port = int(os.environ.get("API_PORT", 5001))
web_port = int(os.environ.get("PORT") or os.environ.get("WEB_PORT", 5000))

if API_SERVER and WEB_UI:
    from api import app as api_app
    from web_ui import app as web_app

    threading.Thread(
        target=uvicorn.run,
        kwargs={"app": api_app, "host": "0.0.0.0", "port": api_port},
        name="api",
        daemon=True,
    ).start()
    uvicorn.run(web_app, host="0.0.0.0", port=web_port)

elif API_SERVER:
    from api import app as api_app
    uvicorn.run(api_app, host="0.0.0.0", port=api_port)

elif WEB_UI:
    from web_ui import app as web_app
    uvicorn.run(web_app, host="0.0.0.0", port=web_port)

else:
    if WATCHER:
        watcher_thread.join()
    else:
        print("No components enabled. Set WATCHER, API_SERVER, or WEB_UI.")
