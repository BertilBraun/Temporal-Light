"""JSON-safe payload conversion and annotation-guided restoration."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from types import NoneType, UnionType
from typing import Any, Union, get_args, get_origin, get_type_hints

from pydantic import BaseModel


def to_json_safe(value: Any) -> Any:
    """Convert supported Python values into plain JSON-compatible structures."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode='json')

    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: to_json_safe(getattr(value, field.name)) for field in dataclasses.fields(value)}

    if isinstance(value, list):
        return [to_json_safe(item) for item in value]

    if isinstance(value, tuple):
        return [to_json_safe(item) for item in value]

    if isinstance(value, Mapping):
        return {str(key): to_json_safe(item) for key, item in value.items()}

    return value


def from_json_safe(value: Any, annotation: Any | None) -> Any:
    """Restore a JSON value using the provided type annotation when supported."""
    if annotation is None or annotation is Any:
        return value

    if annotation is NoneType:
        return None if value is None else value

    origin = get_origin(annotation)
    if origin in (UnionType, None) and isinstance(annotation, UnionType):
        return _restore_union(value, get_args(annotation))

    if origin is Union:
        return _restore_union(value, get_args(annotation))

    if _is_base_model_type(annotation) and isinstance(value, dict):
        return annotation.model_validate(value)

    if dataclasses.is_dataclass(annotation) and isinstance(value, dict):
        return _restore_dataclass(value, annotation)

    if origin is list:
        item_annotation = _single_type_argument(annotation)
        if not isinstance(value, list):
            return value
        return [from_json_safe(item, item_annotation) for item in value]

    if origin is dict:
        key_annotation, value_annotation = _dict_type_arguments(annotation)
        if not isinstance(value, dict):
            return value
        return {
            _restore_dict_key(key, key_annotation): from_json_safe(item, value_annotation)
            for key, item in value.items()
        }

    if origin is tuple:
        if not isinstance(value, list):
            return value
        return tuple(_restore_tuple_items(value, get_args(annotation)))

    return value


def _restore_union(value: Any, member_annotations: tuple[Any, ...]) -> Any:
    if value is None and NoneType in member_annotations:
        return None

    last_error: Exception | None = None
    for member_annotation in member_annotations:
        if member_annotation is NoneType:
            continue
        try:
            restored = from_json_safe(value, member_annotation)
        except Exception as error:
            last_error = error
            continue
        if _value_matches_annotation(restored, member_annotation):
            return restored

    if last_error is not None:
        raise last_error
    return value


def _restore_dataclass(value: dict[Any, Any], annotation: type[Any]) -> Any:
    field_annotations = get_type_hints(annotation)
    restored_fields = {
        str(key): from_json_safe(field_value, field_annotations.get(str(key))) for key, field_value in value.items()
    }
    return annotation(**restored_fields)


def _restore_tuple_items(value: list[Any], item_annotations: tuple[Any, ...]) -> list[Any]:
    if not item_annotations:
        return value

    if len(item_annotations) == 2 and item_annotations[1] is Ellipsis:
        return [from_json_safe(item, item_annotations[0]) for item in value]

    return [
        from_json_safe(item, item_annotations[index] if index < len(item_annotations) else Any)
        for index, item in enumerate(value)
    ]


def _restore_dict_key(key: Any, annotation: Any | None) -> Any:
    if annotation in (None, Any, str):
        return key
    if annotation in (int, float, bool):
        return annotation(key)
    return key


def _value_matches_annotation(value: Any, annotation: Any) -> bool:
    origin = get_origin(annotation)
    if origin is not None:
        origin_type = list if origin is list else dict if origin is dict else tuple if origin is tuple else None
        return origin_type is None or isinstance(value, origin_type)
    if _is_base_model_type(annotation):
        return isinstance(value, annotation)
    if dataclasses.is_dataclass(annotation):
        return isinstance(value, annotation)
    if isinstance(annotation, type):
        return isinstance(value, annotation)
    return True


def _is_base_model_type(annotation: Any) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, BaseModel)


def _single_type_argument(annotation: Any) -> Any:
    arguments = get_args(annotation)
    return arguments[0] if arguments else Any


def _dict_type_arguments(annotation: Any) -> tuple[Any, Any]:
    arguments = get_args(annotation)
    if len(arguments) != 2:
        return Any, Any
    return arguments[0], arguments[1]
