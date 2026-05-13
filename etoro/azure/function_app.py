import azure.functions as func
from azure.functions import WsgiMiddleware
from lambda_app import app as flask_app

app = func.FunctionApp()


@app.route(
    route="{*route}",
    auth_level=func.AuthLevel.ANONYMOUS,
    methods=["GET", "POST", "DELETE", "OPTIONS", "PUT"],
)
def etoro_api(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    return WsgiMiddleware(flask_app).handle(req, context)
