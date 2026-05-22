from configs.settings import AppSettings


def test_default_app_settings() -> None:
    settings = AppSettings()

    assert settings.app_name == "aegisflow-quant"
    assert settings.environment == "development"
    assert settings.log_level == "INFO"
    assert settings.ws_connect_timeout_seconds == 10
