import webbrowser
from threading import Timer

import uvicorn


def open_browser() -> None:
    webbrowser.open("http://127.0.0.1:8000")


if __name__ == "__main__":
    Timer(1.2, open_browser).start()
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000)
