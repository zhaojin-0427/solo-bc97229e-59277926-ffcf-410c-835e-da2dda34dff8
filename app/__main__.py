"""Local development entry point: ``python -m app``.

The threaded dev server keeps responding while reduction jobs run in
background threads. Production uses gunicorn (see Dockerfile).
"""

from .web import create_app

if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=8000, threaded=True, debug=False)
