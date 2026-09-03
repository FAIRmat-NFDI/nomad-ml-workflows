from __future__ import annotations

import logging
from collections.abc import Mapping
from functools import lru_cache

from huggingface_hub import HfApi, ModelCard

logger = logging.getLogger(__name__)


def validate_hf_model_card(content: str) -> ModelCard:
    """Parse a model card and require valid Hugging Face metadata."""
    try:
        return ModelCard(content, ignore_metadata_errors=False)
    except Exception as error:
        raise ValueError(f'Could not parse the model card: {error}') from error


def _get_tag_ids(model_tags: Mapping[str, object], category: str) -> list[str]:
    """Return unique, non-empty tag IDs while preserving the Hub order."""
    tags = model_tags.get(category, [])
    if not isinstance(tags, list):
        return []

    tag_ids: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        if not isinstance(tag, Mapping):
            continue
        tag_id = tag.get('id')
        if not isinstance(tag_id, str) or not (tag_id := tag_id.strip()):
            continue
        if tag_id not in seen:
            seen.add(tag_id)
            tag_ids.append(tag_id)

    return tag_ids


def _get_model_tags(api: HfApi) -> Mapping[str, object]:
    model_tags = api.get_model_tags()
    return model_tags if isinstance(model_tags, Mapping) else {}


@lru_cache(maxsize=1)
def _get_default_model_tags() -> Mapping[str, object]:
    """Fetch the catalog once per process without making schema loading mandatory."""
    try:
        return _get_model_tags(HfApi())
    except Exception as exc:
        # Suggestions are optional UI metadata. A temporary Hub/network failure must
        # not prevent NOMAD from loading the schema package.
        logger.warning('Could not load Hugging Face model-tag suggestions: %s', exc)
        return {}


def get_hf_pipeline_tag_ids(api: HfApi | None = None) -> list[str]:
    """Get Hub pipeline-tag IDs suitable for NOMAD task suggestions."""
    model_tags = _get_default_model_tags() if api is None else _get_model_tags(api)
    return _get_tag_ids(model_tags, 'pipeline_tag')


def get_hf_library_ids(api: HfApi | None = None) -> list[str]:
    """Get Hub library-tag IDs suitable for NOMAD library suggestions."""
    model_tags = _get_default_model_tags() if api is None else _get_model_tags(api)
    return _get_tag_ids(model_tags, 'library')


tasks = get_hf_pipeline_tag_ids()
libraries = get_hf_library_ids()
