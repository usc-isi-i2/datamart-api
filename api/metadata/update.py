import datetime

import pandas as pd

from api.metadata.metadata import DatasetMetadata
from db.sql import dal
from db.sql.kgtk import import_kgtk_dataframe


class DatasetMetadataUpdater():

    def create_dataset(self, metadata: DatasetMetadata, *, create: bool = True):
        # Create qnode
        dataset_id = f'Q{metadata.dataset_id}'
        edges = None
        if dal.get_dataset_id(metadata.dataset_id) is None:
            metadata._dataset_id = dataset_id

            if not metadata.last_update:
                metadata.last_update = datetime.datetime.now().isoformat().split('.')[0]
                metadata.last_update_precision = 14  # second

            edges = pd.DataFrame(metadata.to_kgtk_edges(dataset_id))

            if create:
                import_kgtk_dataframe(edges)

        return dataset_id, edges


    def update(self, dataset_id: str, *, last_update: str = None) -> dict:
        '''update dataset metadata last_updated field'''
        if not last_update:
            last_update = datetime.datetime.now().isoformat().split('.')[0]

        dataset_metadata = dal.query_dataset_metadata(dataset_id, include_dataset_qnode=True)
        if not dataset_metadata:
            raise Exception(f"No such dataset {dataset_id}")

        dataset_metadata = dataset_metadata[0]
        dataset_qnode = dataset_metadata.pop('dataset_qnode')
        dal.delete_dataset_metadata(dataset_qnode, debug=True)

        dataset_metadata['last_update'] = last_update
        dataset_metadata['last_update_precision'] = 14  # second

        self.create_dataset(DatasetMetadata().from_dict(dataset_metadata))
        return dataset_metadata
