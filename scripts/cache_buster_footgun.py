from pydantic import BaseModel
from functools import cached_property

class Catalog(BaseModel):
    entities: list[Entity] = []
    limit: int | None = None

    def __setattr__(self, name, value):
        """Catches top-level attribute changes and busts the caches."""
        if name in self.__dict__:
            self._bust_caches()
        super().__setattr__(name, value)

    def _bust_caches(self):
        """Silently drops the cached properties so they recalculate on next access."""
        self.__dict__.pop('arrow_schema', None)
        self.__dict__.pop('shaped_schema', None)

    # --- Controlled Mutation Methods for Deep Objects ---
    def add_entity(self, entity: Entity):
        """
        If you append to the list directly (catalog.entities.append), the cache SURVIVES (Danger!).
        You MUST use this method to mutate deep objects safely.
        """
        self.entities.append(entity)
        self._bust_caches()

    @cached_property
    def shaped_schema(self) -> pa.Schema:
        print("Calculating heavy schema...")
        return pa.schema([]) # Heavy logic here