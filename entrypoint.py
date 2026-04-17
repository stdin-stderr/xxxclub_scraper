import os
import threading

import page_watcher
import metadata_fetcher

WEB_UI = os.environ.get("WEB_UI", "").lower() in ("1", "true", "yes")

watcher_thread = threading.Thread(target=page_watcher.run, name="watcher", daemon=True)
watcher_thread.start()

if os.environ.get("PORNDB_API_KEY"):
    metadata_thread = threading.Thread(target=metadata_fetcher.run_loop, name="metadata", daemon=True)
    metadata_thread.start()
else:
    print("PORNDB_API_KEY not set; metadata fetcher disabled.")

if WEB_UI:
    import uvicorn
    from web_ui import app

    port = int(os.environ.get("WEB_PORT", 5000))
    uvicorn.run(app, host="0.0.0.0", port=port)
else:
    watcher_thread.join()
