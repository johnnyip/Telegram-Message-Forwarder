import json
from typing import Any, Dict, List

from ..core.utils import parse_int_or_str


def load_routes(routes_json: str) -> List[Dict[str, Any]]:
    routes_json = routes_json.strip()
    if not routes_json:
        raise RuntimeError("ROUTES_JSON is required")

    raw_routes = json.loads(routes_json)
    if not isinstance(raw_routes, list):
        raise RuntimeError("ROUTES_JSON must be a JSON array")

    routes: List[Dict[str, Any]] = []
    for idx, raw in enumerate(raw_routes, start=1):
        if not isinstance(raw, dict):
            raise RuntimeError(f"Route #{idx} must be an object")

        sources = [parse_int_or_str(x) for x in raw.get("sources", [])]
        targets = [parse_int_or_str(x) for x in raw.get("targets", [])]

        if not sources:
            raise RuntimeError(f"Route #{idx} has no sources")
        if not targets:
            raise RuntimeError(f"Route #{idx} has no targets")

        routes.append({
            "name": str(raw.get("name", f"route_{idx}")),
            "sources": set(sources),
            "targets": targets,
        })
    return routes


def find_matching_routes(chat_id: Any, routes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [route for route in routes if chat_id in route["sources"]]


def route_names(routes: List[Dict[str, Any]]) -> List[str]:
    return [r["name"] for r in routes]


__all__ = [
    "find_matching_routes",
    "load_routes",
    "route_names",
]
