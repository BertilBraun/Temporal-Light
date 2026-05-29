"""Tests for JSON-safe payload serialization and typed restoration."""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel

from temporal_light.serialization import from_json_safe, to_json_safe


class LineItem(BaseModel):
    sku: str
    quantity: int


@dataclass(frozen=True)
class Shipment:
    tracking_id: str
    items: list[LineItem]


def test_pydantic_model_serializes_to_plain_json_dict() -> None:
    item = LineItem(sku='book', quantity=2)

    assert to_json_safe(item) == {'sku': 'book', 'quantity': 2}


def test_nested_dataclass_restores_from_annotation() -> None:
    payload = {
        'tracking_id': 'trk-123',
        'items': [{'sku': 'book', 'quantity': 2}],
    }

    result = from_json_safe(payload, Shipment)

    assert result == Shipment(
        tracking_id='trk-123',
        items=[LineItem(sku='book', quantity=2)],
    )


def test_union_restoration_uses_annotation_order() -> None:
    result = from_json_safe({'sku': 'book', 'quantity': 2}, LineItem | Shipment)

    assert result == LineItem(sku='book', quantity=2)
