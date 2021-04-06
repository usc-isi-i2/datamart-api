# This file contains functions that wrap access to kgtk
import csv
import tempfile
from typing import Tuple
from pathlib import Path
from kgtk.exceptions import KGTKException
import kgtk.cli.validate
import kgtk.cli.explode
import kgtk.cli.add_id
import shutil
from kgtk.io.kgtkwriter import KgtkWriter
from kgtk.kgtkformat import KgtkFormat
from kgtk.value.kgtkvalue import KgtkValueFields
from pandas.core.frame import DataFrame
import pandas as pd

class KGTKPipeline:
    def __init__(self, frame: DataFrame):
        self._dir = tempfile.mkdtemp()
        frame.to_csv(self.input, sep='\t', index=False, quoting=csv.QUOTE_NONE, quotechar='')

    @property
    def dir(self):
        return self._dir

    @property
    def input(self) -> Path:
        return Path(self._dir, 'input.tsv')

    def get_file(self, name: str) -> Path:
        return Path(self._dir, name)

    def read_csv(self, name: str) -> DataFrame:
        return pd.read_csv(self.get_file(name), sep='\t', quoting=csv.QUOTE_NONE, dtype=object).fillna('')

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if not self._dir:
            return
        try:
            shutil.rmtree(self._dir)
        except:
            pass

# We have not yet found a way to validate kgtk files with kgtk.
# Running kgtk validate form the shell does not return a different exit code in case of an error.
# Running kgtk.cli.validate.run doesn't validate the file
# def validate(ctx: KGTKPipeline, filename='input.tsv') -> Tuple[bool, str]:
#     try:
#         kgtk.cli.validate.run(ctx.get_file(filename), verbose=True, very_verbose=True, errors_to_stdout=True, validate_by_default=True)
#         return True, ''
#     except KGTKException as e:
#         return False, str(e)

def explode(ctx: KGTKPipeline, infile: str = 'input.tsv', outfile: str = 'exploded.tsv') -> None:
    kgtk.cli.explode.run(ctx.get_file(infile),
                         ctx.get_file(outfile),
                         KgtkFormat.NODE2,
                         KgtkFormat.DataType.choices(),
                         KgtkValueFields.FIELD_NAMES,
                         KgtkFormat.NODE2 + ";" + KgtkFormat.KGTK_NAMESPACE,
                         False,
                         False,
                         False,
                         KgtkWriter.OUTPUT_FORMAT_KGTK,
                         allow_lax_nodes=True)

def add_ids(ctx: KGTKPipeline, infile: str = 'input.tsv', outfile: str = 'with-ids.tsv') -> None:
    kgtk.cli.add_id.run(ctx.get_file(infile), ctx.get_file(outfile), id_style='node1-label-node2-num')
