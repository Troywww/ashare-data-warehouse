"""Web 控制面板 — Flask 应用工厂."""

from flask import Flask


def create_app() -> Flask:
    app = Flask(__name__)

    from . import routes
    app.register_blueprint(routes.bp)
    routes.init_app(app)

    return app
