from dotenv import load_dotenv
import os
from typing import Optional

load_dotenv(".env")


class BaseConfig:
    PERCENT_IN: float = float(os.getenv("PERCENT_IN", "0.0"))
    PERCENT_OUT: float = float(os.getenv("PERCENT_OUT", "0.0"))
    LIFETIME: int = int(os.getenv("LIFETIME", "0"))
    LEVERAGE: int = int(os.getenv("LEVERAGE", "1"))
    MAX_VOLUME_FOR_TRADE: float = float(os.getenv("MAX_VOLUME_FOR_TRADE", "0.0"))
    MIN_VOLUME_24H: float = float(os.getenv("MIN_VOLUME_24H", "0.0"))
    MAX_TRADES_COUNT: int = int(os.getenv("MAX_TRADES_COUNT", "3"))

    CHAT_ID: Optional[str] = os.getenv("CHAT_ID")

    REAL_TRADE: int = int(os.getenv("REAL_TRADE", 0))
    


class ProdConfig(BaseConfig):
    MEXC_WS: Optional[str] = os.getenv("MEXC_WS_PROD")
    MEXC_REST_API: Optional[str] = os.getenv("MEXC_REST_API_PROD")
    MEXC_API_KEY: Optional[str] = os.getenv("MEXC_API_KEY_PROD")
    MEXC_SECRET_KEY: Optional[str] = os.getenv("MEXC_SECRET_KEY_PROD")
    MEXC_AUTHORIZATION_KEY: Optional[str] = os.getenv("MEXC_AUTHORIZATION_KEY_PROD")
    
    BYBIT_WS: Optional[str] = os.getenv("BYBIT_WS_PROD")
    BYBIT_REST_API: Optional[str] = os.getenv("BYBIT_REST_API_PROD")
    BYBIT_API_KEY: Optional[str] = os.getenv("BYBIT_API_KEY_PROD")
    BYBIT_SECRET_KEY: Optional[str] = os.getenv("BYBIT_SECRET_KEY_PROD")

    GATE_WS: Optional[str] = os.getenv("GATE_WS_PROD")
    GATE_REST_API: Optional[str] = os.getenv("GATE_REST_API_PROD")
    GATE_API_KEY: Optional[str] = os.getenv("GATE_API_KEY_PROD")
    GATE_SECRET_KEY: Optional[str] = os.getenv("GATE_SECRET_KEY_PROD")


class DevConfig(BaseConfig):
    MEXC_WS: Optional[str] = os.getenv("MEXC_WS")
    MEXC_REST_API: Optional[str] = os.getenv("MEXC_REST_API")
    MEXC_API_KEY: Optional[str] = os.getenv("MEXC_API_KEY")
    MEXC_SECRET_KEY: Optional[str] = os.getenv("MEXC_SECRET_KEY")
    MEXC_AUTHORIZATION_KEY: Optional[str] = os.getenv("MEXC_AUTHORIZATION_KEY")

    BYBIT_WS: Optional[str] = os.getenv("BYBIT_WS")
    BYBIT_REST_API: Optional[str] = os.getenv("BYBIT_REST_API")
    BYBIT_API_KEY: Optional[str] = os.getenv("BYBIT_API_KEY")
    BYBIT_SECRET_KEY: Optional[str] = os.getenv("BYBIT_SECRET_KEY")

    GATE_WS: Optional[str] = os.getenv("GATE_WS")
    GATE_REST_API: Optional[str] = os.getenv("GATE_REST_API")
    GATE_API_KEY: Optional[str] = os.getenv("GATE_API_KEY")
    GATE_SECRET_KEY: Optional[str] = os.getenv("GATE_SECRET_KEY")


config = DevConfig() if os.getenv("APP_ENV", "dev") == "dev" else ProdConfig()
