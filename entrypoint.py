import os
import threading

import page_watcher

WEB_UI = os.environ.get("WEB_UI", "").lower() in ("1", "true", "yes")

thread = threading.Thread(target=page_watcher.run, name="watcher", daemon=True)
thread.start()

if WEB_UI:
    import uvicorn
    from web_ui import app

    port = int(os.environ.get("WEB_PORT", 5000))
    uvicorn.run(app, host="0.0.0.0", port=port)
else:
    thread.join()
