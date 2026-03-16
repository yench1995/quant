import importlib
import pkgutil
from pathlib import Path
from .base import AbstractStrategy

class StrategyRegistry:
    _strategies: dict[str, type[AbstractStrategy]] = {}

    @classmethod
    def register(cls, strategy_class: type[AbstractStrategy]):
        cls._strategies[strategy_class.STRATEGY_ID] = strategy_class

    @classmethod
    def get(cls, strategy_id: str) -> type[AbstractStrategy] | None:
        return cls._strategies.get(strategy_id)

    @classmethod
    def all(cls) -> dict[str, type[AbstractStrategy]]:
        return dict(cls._strategies)

    @classmethod
    def discover(cls):
        """Auto-discover all AbstractStrategy subclasses in the strategies package."""
        package_dir = Path(__file__).parent
        for _, module_name, _ in pkgutil.iter_modules([str(package_dir)]):
            if module_name in ("base", "registry"):
                continue
            try:
                module = importlib.import_module(f".{module_name}", package="app.strategies")
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (
                        isinstance(attr, type)
                        and issubclass(attr, AbstractStrategy)
                        and attr is not AbstractStrategy
                        and hasattr(attr, "STRATEGY_ID")
                    ):
                        cls.register(attr)
            except Exception as e:
                print(f"Warning: could not load strategy module {module_name}: {e}")

registry = StrategyRegistry()
