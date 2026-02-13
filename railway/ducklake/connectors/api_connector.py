"""Conector para APIs externas (dolar blue, feriados, etc)."""

from datetime import datetime

import pandas as pd
import requests

from .base import BaseConnector


class APIConnector(BaseConnector):
    """Extrae datos desde APIs REST publicas."""

    source_name = "api"

    def __init__(self, sources: dict):
        self.sources = sources

    def extract(self) -> dict[str, pd.DataFrame]:
        results = {}
        for name, cfg in self.sources.items():
            try:
                df = self._fetch(name, cfg)
                if df is not None:
                    results[name] = df
            except Exception as e:
                print(f"  WARN: API {name} no disponible ({e}), skipping")
        return results

    def _fetch(self, name: str, cfg: dict) -> pd.DataFrame | None:
        url = cfg["url"].format(year=datetime.now().year)
        resp = requests.get(url, timeout=cfg.get("timeout", 10))
        resp.raise_for_status()
        data = resp.json()

        if name == "dolar":
            return self._parse_dolar(data)
        elif name == "feriados":
            return pd.DataFrame(data)
        else:
            return pd.DataFrame(data if isinstance(data, list) else [data])

    def _parse_dolar(self, data: dict) -> pd.DataFrame:
        return pd.DataFrame([{
            "fecha": datetime.now().strftime("%Y-%m-%d"),
            "dolar_blue_compra": data["blue"]["value_buy"],
            "dolar_blue_venta": data["blue"]["value_sell"],
            "dolar_oficial_compra": data["oficial"]["value_buy"],
            "dolar_oficial_venta": data["oficial"]["value_sell"],
        }])
