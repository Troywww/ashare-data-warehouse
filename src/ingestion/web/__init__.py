"""Web 控制面板 — Flask 应用工厂."""

import math

from flask import Flask
from flask.json.provider import DefaultJSONProvider


class SanitizedProvider(DefaultJSONProvider):
    """不允许 JSON 中出现 NaN/Inf."""

    def dumps(self, obj, **kwargs):
        return super().dumps(self._clean(obj), allow_nan=False, **kwargs)

    @staticmethod
    def _clean(obj):
        if isinstance(obj, dict):
            return {k: SanitizedProvider._clean(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [SanitizedProvider._clean(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj


def create_app() -> Flask:
    app = Flask(__name__)
    app.json_provider_class = SanitizedProvider
    app.json = SanitizedProvider(app)

    from . import routes
    app.register_blueprint(routes.bp)
    routes.init_app(app)

    return app
