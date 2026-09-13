"""Versioned run provenance snapshots; the launcher remains the registry owner."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ArtifactReference(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    name: str = Field(pattern=r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$')
    version: str = Field(pattern=r'^sha256:[a-f0-9]{64}$')


class RunArtifacts(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    schema_version: Literal[1]
    request_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    source_commit: str = Field(pattern=r'^[a-f0-9]{40}$')
    inputs: list[ArtifactReference] = Field(default_factory=list, max_length=100)
    outputs: list[ArtifactReference] = Field(default_factory=list, max_length=100)

    @model_validator(mode='after')
    def unique_names(self) -> RunArtifacts:
        for references in (self.inputs, self.outputs):
            if len({r.name for r in references}) != len(references):
                raise ValueError('Artifact reference names must be unique within each role')
        return self


def snapshot(value: Any) -> dict[str, Any] | None:
    return None if value is None else RunArtifacts.model_validate(value).model_dump()
