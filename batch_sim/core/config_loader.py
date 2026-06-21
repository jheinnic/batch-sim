"""Config loader: reads YAML files and validates them into Pydantic models."""
from __future__ import annotations
from pathlib import Path
from typing import TypeVar, Type
import yaml
from pydantic import BaseModel
from batch_sim.core.schemas import (
    SimulationConfig, InstanceRegistryConfig, SchedulerConfig, ExperimentConfig,
)

M = TypeVar("M", bound=BaseModel)

def _load_yaml(path: str | Path) -> dict:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as f:
        return yaml.safe_load(f)

def load_config(path: str | Path, model: Type[M]) -> M:
    return model.model_validate(_load_yaml(path))

def load_simulation_config(path: str | Path) -> SimulationConfig:
    return load_config(path, SimulationConfig)

def load_instance_registry_config(path: str | Path) -> InstanceRegistryConfig:
    return load_config(path, InstanceRegistryConfig)

def load_scheduler_config(path: str | Path):
    # BSIM-109: SchedulerConfig is a discriminated union (BatchConfig | K8SConfig |
    # K8SPlusConfig), not a single model — validate via TypeAdapter, which returns
    # the concrete subclass keyed on scheduler_type.
    from pydantic import TypeAdapter
    return TypeAdapter(SchedulerConfig).validate_python(_load_yaml(path))

def load_experiment_config(path: str | Path) -> ExperimentConfig:
    return load_config(path, ExperimentConfig)
