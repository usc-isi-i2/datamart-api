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

class UnknownSubjectError(Exception):
    def __init__(self, *errors):
        super().__init__()
        self.errors = errors

    def get_error_dict(self):
        return {'Error': self.errors}

def get_query_region_ids(request_args) -> Dict[str, List[str]]:
    # Returns all ids pf regions specifed in the URL in a dictionary based on region_type:
    # { country_id: [all countries in the query],
    #   admin1_id: [all admin1s in the query],
    #   admin2_id: [all admin2s in the query],
    #   admin3_id: [all admin3s in the query] }
    # Raises an exception if non-existing regions are specified (by name or by ID)

    # Consolidate all names and ids into two lists
    args = {
        'country': Region.COUNTRY,
        'admin1': Region.ADMIN1,
        'admin2': Region.ADMIN2,
        'admin3': Region.ADMIN3,
    }
    arg_names = []
    arg_ids = []
    for arg in args.keys():
        arg_id = f'{arg}_id'
        arg_names += request_args.getlist(arg)
        arg_ids += request_args.getlist(arg_id)

    # Query those regions
    found_regions_by_id = region_cache.get_regions(region_names=arg_names, region_ids=arg_ids)
    found_regions_by_name: Dict[
        str, Set[Region]] = {}  # Organize by name for easy lookup, there can be numerous regions per name
    for region in found_regions_by_id.values():
        name = region.admin.lower()
        if not name in found_regions_by_name:
            found_regions_by_name[name] = {region}
        else:
            found_regions_by_name[name].add(region)

    # Now go over the queried regions and make sure we have everything we asked for
    errors = []
    result_regions: Dict[str, List[str]] = {}
    for arg, arg_type in args.items():
        result_regions[arg] = []

        arg_names = [name for name in request_args.getlist(arg)]
        for name in arg_names:
            found = False
            for candidate in found_regions_by_name.get(name.lower(), set()):
                if candidate.region_type == arg_type:
                    result_regions[arg].append(candidate.admin_id)
                    found = True
            if not found:
                errors.append(f'No {arg} {name}')

        arg_id = f'{arg}_id'
        arg_ids = request_args.getlist(arg_id) or []
        for arg_id in arg_ids:
            c = found_regions_by_id.get(arg_id)
            if c and c.region_type == arg_type:
                result_regions[arg].append(c.admin_id)
            else:
                errors.append(f'No {arg}_id {arg_id}')

    if errors:
        raise UnknownSubjectError(errors)

    return result_regions
