"""Distributed algorithm implementations.

This package contains implementations of various distributed algorithms.
It automatically scans for algorithm modules and registers them.
"""

import importlib
import inspect
import pkgutil
from typing import TYPE_CHECKING

from distbench.algorithm import Algorithm

if TYPE_CHECKING:
    from distbench.algorithm import Algorithm

ALGORITHM_REGISTRY: dict[str, type["Algorithm"]] = {}
__all__ = []


def _register_algorithms() -> None:
    """Find and import all algorithm modules in this package."""

    package_path = __path__
    package_name = __name__

    for _, module_name, _ in pkgutil.iter_modules(package_path):
        if module_name.startswith("_"):
            continue

        try:
            module = importlib.import_module(f"{package_name}.{module_name}")

            for name, obj in inspect.getmembers(module):
                if (
                    inspect.isclass(obj)
                    and issubclass(obj, Algorithm)
                    and obj is not Algorithm
                    and obj.__module__ == module.__name__
                ):
                    ALGORITHM_REGISTRY[module_name] = obj

                    if name not in __all__:
                        __all__.append(name)

        except Exception as e:
            print(f"Warning: Failed to import algorithm '{module_name}': {e}")


_register_algorithms()

__all__.append("ALGORITHM_REGISTRY")
