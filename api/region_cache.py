# This class encapsulates region queries and caches results, so that the database it not contacted over and over again
# each time a region is required

from typing import Dict, Set, List
from db.sql.dal import Region
from db.sql import dal

class _RegionCache:
    # Region storage
    _regions: Dict[str, Region]   # qnode to region mapping
    _regions_by_name: Dict[str, Set[Region]] # region name (lowercase) to region mapping. There can be more than one region per name

    def __init__(self):
        self._regions = {}
        self._regions_by_name = {}
        self._regions_by_type = {}

    def _add_regions(self, regions: List[Region]):
        for region in regions:
            self._regions[region.admin_id] = region

            name = region.admin.lower()
            if not name in self._regions_by_name:
                self._regions_by_name[name] = set()
            self._regions_by_name[name].add(region)

    def _load_from_db(self, names: Set[str]=set(), ids: Set[str]=set()) -> None:
        if not names and not ids:
            return

        regions = dal.query_admins(admins = list(names), admin_ids = list(ids))
        self._add_regions(regions)

    def get_regions(self, region_names: List[str]=[], region_ids: List[str]=[], region_type=None) -> Dict[str, Region]:
        # Find missing names and ids
        region_names = [name.lower() for name in region_names]
        missing_names = set(region_names) - set(self._regions_by_name.keys())
        missing_ids = set(region_ids) - set(self._regions.keys())

         # Query database for the rest
        self._load_from_db(names=missing_names, ids=missing_ids)

        # Now return everything we have, first ids
        regions = {}
        for id in region_ids:
            region = self._regions.get(id)
            if region and (region_type is None or region.region_type==region_type):
                regions[region.admin_id] = region

        # Now names
        for name in region_names:
            name_regions = self._regions_by_name.get(name.lower(), set())
            for region in name_regions:
                if region_type is None or region.region_type == region_type:
                    regions[region.admin_id] = region

        return regions

region_cache = _RegionCache()  # Public, process-wide
