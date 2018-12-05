from pecan import make_app
from cs739devicefailureprediction import model


def setup_app(config):
    model.init_model(config)
    app_conf = dict(config.app)

    return make_app(
        app_conf.pop('root'),
        logging=getattr(config, 'logging', {}),
        **app_conf
    )
