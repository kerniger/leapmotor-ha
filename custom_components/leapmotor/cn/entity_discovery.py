"""Discover late-arriving CN signals without duplicating entities."""
from homeassistant.core import callback


def register_discovery(coordinator, entry, async_add_entities, descriptions, factory):
    seen = set()

    @callback
    def discover():
        entities = []
        for vin, data in (coordinator.data or {}).get("vehicles", {}).items():
            for description in descriptions(data):
                identity = (vin, description.key)
                if identity not in seen:
                    seen.add(identity)
                    entities.append(factory(coordinator, entry.entry_id, vin, description))
        if entities:
            async_add_entities(entities)

    entry.async_on_unload(coordinator.async_add_listener(discover))
    discover()
