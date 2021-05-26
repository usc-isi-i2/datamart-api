from datetime import date
import pandas as pd
import ast
import tempfile
from enum import Enum
import hashlib
import shutil
import csv
from pathlib import Path
import regex as re
import math

from .regex_defination import lax_number_or_quantity_pat, lax_date_and_times_pat


def build_wikidata_id(row, node1_column_idx, node2_column_idx, label_column_idx, value_hash_width=6, id_separator="-"):
    node2_value = row[node2_column_idx]
    if value_hash_width > 0 and node2_value.startswith(('L', 'P', 'Q')):
        return row[node1_column_idx] + id_separator + row[label_column_idx] + id_separator + row[node2_column_idx]
    else:
        return row[node1_column_idx] + id_separator + row[label_column_idx] + id_separator + \
            hashlib.sha256(node2_value.encode('utf-8')).hexdigest()[:value_hash_width]

def add_ids2(df, overwrite_id=False):
    ''''This version assume the "id" column exists. By default it does not overide existind ids'''
    def build_wikidata_id2(row, value_hash_width=6, id_separator="-"):
        node2_value = row['node2']
        if value_hash_width > 0 and node2_value.startswith(('L', 'P', 'Q')):
            return row['node1'] + id_separator + row['label'] + id_separator + row['node2']
        else:
            return row['node1'] + id_separator + row['label'] + id_separator + \
                hashlib.sha256(node2_value.encode('utf-8')).hexdigest()[:value_hash_width]

    def create_id(row):
        if overwrite_id or not row['id']:
            new_id = build_wikidata_id2(row)
        else:
            new_id = row['id']
        return new_id
    id_col = df.apply(create_id, axis=1)
    new_df = df.copy()
    new_df['id'] = id_col
    return new_df

def add_ids(df):
    column_names = df.columns.copy()
    node1_column_idx = column_names.get_loc("node1")
    label_column_idx = column_names.get_loc("label")
    node2_column_idx = column_names.get_loc("node2")
    # id_column_idx = -1 # default

    # id_style='wikidata'
    if node1_column_idx < 0:
        raise ValueError("No node1 column index")
    if label_column_idx < 0:
        raise ValueError("No label column index")
    if node2_column_idx < 0:
        raise ValueError("No node2 column index")

    # if id_column_idx >= 0:
    #     # The input file has an ID column.  Use it.
    #     # old_id_column_name = column_names[id_column_idx]
    #     old_id_column_idx = id_column_idx
    # else:
    #     # There is not old ID column index.
    #     old_id_column_idx = -1
    #     # old_id_column_name = ""


    # # The new ID column was not explicitly named.
    # if id_column_idx >= 0:
    #     # The input file has an ID column.  Use it.
    #     new_id_column_name = column_names[id_column_idx]
    #     new_id_column_idx = id_column_idx
    #     add_new_id_column = False
    # else:
    #     # Create a new ID column.
    #     new_id_column_idx = len(column_names)
    #     new_id_column_name = "id"
    #     column_names.append(new_id_column_name)
    #     add_new_id_column = True

    new_id_column_idx = len(column_names)
    new_id_column_name = "id"

    # claim_id_column_name = "claim_id"
    # # claim_id_column_idx = column_name_map.get(claim_id_column_name, -1)
    # initial_id = -1

    df[new_id_column_name] = None # add a new column for the ids.
    for i, row in df.iterrows():
        # if add_new_id_column:
        #     row.append("")
        # elif old_id_column_idx >= 0:
        #     if row[old_id_column_idx] != "":
        #         if new_id_column_idx != old_id_column_idx:
        #             row[new_id_column_idx] = row[old_id_column_idx]
        #         continue
        new_id = build_wikidata_id(row, node1_column_idx, node2_column_idx, label_column_idx)
        row[new_id_column_idx] = new_id

    return df

class DataType(Enum):
    EMPTY = 0
    LIST = 1
    NUMBER = 2
    QUANTITY = 3
    STRING = 4
    LANGUAGE_QUALIFIED_STRING = 5
    LOCATION_COORDINATES = 6
    DATE_AND_TIMES = 7
    EXTENSION = 8
    BOOLEAN = 9
    SYMBOL = 10

    def lower(self)->str:
        return self.name.lower()

    @classmethod
    def choices(cls):
        results = [ ]
        for name in cls.__members__.keys():
            results.append(name.lower())
        return results


DATA_TYPE_FIELD_NAME = "data_type"
DATE_AND_TIMES_FIELD_NAME = "date_and_time"
DECODED_TEXT_FIELD_NAME = "decoded_text"
HIGH_TOLERANCE_FIELD_NAME = "high_tolerance"
LANGUAGE_FIELD_NAME = "language"
LANGUAGE_SUFFIX_FIELD_NAME = "language_suffix"
LATITUDE_FIELD_NAME = "latitude"
LIST_LEN_FIELD_NAME = "list_len"
LONGITUDE_FIELD_NAME = "longitude"
LOW_TOLERANCE_FIELD_NAME = "low_tolerance"
NUMBER_FIELD_NAME = "number"
PRECISION_FIELD_NAME = "precision"
SI_UNITS_FIELD_NAME = "si_units"
SYMBOL_FIELD_NAME = "symbol"
TEXT_FIELD_NAME = "text"
TRUTH_FIELD_NAME = "truth"
UNITS_NODE_FIELD_NAME = "units_node"
VALID_FIELD_NAME = "valid"


FIELD_NAMES = [
        LIST_LEN_FIELD_NAME,
        DATA_TYPE_FIELD_NAME,
        VALID_FIELD_NAME,
        TEXT_FIELD_NAME,
        DECODED_TEXT_FIELD_NAME,
        LANGUAGE_FIELD_NAME,
        LANGUAGE_SUFFIX_FIELD_NAME,
        "numberstr",
        NUMBER_FIELD_NAME,
        "low_tolerancestr",
        LOW_TOLERANCE_FIELD_NAME,
        "high_tolerancestr",
        HIGH_TOLERANCE_FIELD_NAME,
        SI_UNITS_FIELD_NAME,
        UNITS_NODE_FIELD_NAME,
        "latitudestr",
        LATITUDE_FIELD_NAME,
        "longitudestr",
        LONGITUDE_FIELD_NAME,
        "date",
        "time",
        DATE_AND_TIMES_FIELD_NAME,
        "yearstr",
        "year",
        "monthstr",
        "month",
        "daystr",
        "day",
        "hourstr",
        "hour",
        "minutesstr",
        "minutes",
        "secondsstr",
        "seconds",
        "zonestr",
        "precisionstr",
        PRECISION_FIELD_NAME,
        "iso8601extended",
        TRUTH_FIELD_NAME,
        SYMBOL_FIELD_NAME
        ]

FIELD_NAME_FORMATS = {
        LIST_LEN_FIELD_NAME: "int",
        DATA_TYPE_FIELD_NAME: "sym",
        VALID_FIELD_NAME: "bool",
        TEXT_FIELD_NAME: "str",
        DECODED_TEXT_FIELD_NAME: "str",
        LANGUAGE_FIELD_NAME: "sym",
        LANGUAGE_SUFFIX_FIELD_NAME: "sym",
        "numberstr": "str",
        NUMBER_FIELD_NAME: "num",
        "low_tolerancestr": "str",
        LOW_TOLERANCE_FIELD_NAME: "num",
        "high_tolerancestr": "str",
        HIGH_TOLERANCE_FIELD_NAME: "num",
        SI_UNITS_FIELD_NAME: "sym",
        UNITS_NODE_FIELD_NAME: "sym",
        "latitudestr": "str",
        LATITUDE_FIELD_NAME: "num",
        "longitudestr": "str",
        LONGITUDE_FIELD_NAME: "num",
        "date": "str",
        "time": "str",
        DATE_AND_TIMES_FIELD_NAME: "str",
        "yearstr": "str",
        "year": "int",
        "monthstr": "str",
        "month": "int",
        "daystr": "str",
        "day": "int",
        "hourstr": "str",
        "hour": "int",
        "minutesstr": "str",
        "minutes": "int",
        "secondsstr": "str",
        "seconds": "int",
        "zonestr": "str",
        "precisionstr": "str",
        PRECISION_FIELD_NAME: "int",
        "iso8601extended": "bool",
        TRUTH_FIELD_NAME: "bool",
        SYMBOL_FIELD_NAME: "sym",
    }

KGTK_NAMESPACE = "kgtk:"

DEFAULT_DATA_TYPE_FIELDS = {
        DataType.EMPTY.lower(): [ DATA_TYPE_FIELD_NAME, VALID_FIELD_NAME ],
        DataType.LIST.lower(): [ DATA_TYPE_FIELD_NAME, VALID_FIELD_NAME, LIST_LEN_FIELD_NAME ],
        DataType.NUMBER.lower(): [ DATA_TYPE_FIELD_NAME, VALID_FIELD_NAME, NUMBER_FIELD_NAME ],
        DataType.QUANTITY.lower(): [ DATA_TYPE_FIELD_NAME, VALID_FIELD_NAME,
                                                NUMBER_FIELD_NAME,
                                                LOW_TOLERANCE_FIELD_NAME,
                                                HIGH_TOLERANCE_FIELD_NAME,
                                                SI_UNITS_FIELD_NAME,
                                                UNITS_NODE_FIELD_NAME,
        ],
        DataType.STRING.lower(): [ DATA_TYPE_FIELD_NAME, VALID_FIELD_NAME, TEXT_FIELD_NAME ],
        DataType.LANGUAGE_QUALIFIED_STRING.lower(): [ DATA_TYPE_FIELD_NAME, VALID_FIELD_NAME, TEXT_FIELD_NAME, LANGUAGE_FIELD_NAME, LANGUAGE_SUFFIX_FIELD_NAME ],
        DataType.LOCATION_COORDINATES.lower(): [ DATA_TYPE_FIELD_NAME, VALID_FIELD_NAME,
                                                            LATITUDE_FIELD_NAME,
                                                            LONGITUDE_FIELD_NAME,
        ],
        DataType.DATE_AND_TIMES.lower(): [ DATA_TYPE_FIELD_NAME, VALID_FIELD_NAME,
                                                      DATE_AND_TIMES_FIELD_NAME,
                                                      PRECISION_FIELD_NAME,
        ],
        DataType.EXTENSION.lower(): [ ],
        DataType.BOOLEAN.lower(): [ DATA_TYPE_FIELD_NAME, VALID_FIELD_NAME, TRUTH_FIELD_NAME ],
        DataType.SYMBOL.lower(): [ DATA_TYPE_FIELD_NAME, VALID_FIELD_NAME, SYMBOL_FIELD_NAME ],
    }

def explode(df, verbose=1, ctxPipeline=None):
    column_name= "node2"
    column_names =  list(df.columns.values) # df.columns.copy() #
    prefix = "node2;" + KGTK_NAMESPACE
    selected_field_names = FIELD_NAMES

    explosion = { }
    new_column_count = 0
    for field_name in selected_field_names:
        exploded_name = prefix + field_name
        if verbose:
            print("Field '%s' becomes '%s'" % (field_name, exploded_name), flush=True) # file=self.error_file
        if exploded_name in explosion:
            raise ValueError("Field name '%s' is duplicated in the field list." % exploded_name)
        if exploded_name in column_names:
            raise ValueError("Exploded column '%s' already exists and not allowed to overwrite" % exploded_name)
        else:
            column_names.append(exploded_name)
            exploded_idx = len(column_names) - 1
            explosion[field_name] = exploded_idx
            if verbose:
                print("Field '%s' becomes new column '%s' (idx=%d)" % (field_name, exploded_name, exploded_idx), flush=True) # file=self.error_file
            new_column_count += 1
    if verbose:
        # print("%d columns + %d columns = %d columns" % (column_count, new_column_count, len(column_names)))
        print("Explosion length: %d" % len(explosion))

    for new_column_name in explosion:
        df[prefix + new_column_name] = None

    for _, row in df.iterrows():
        item_to_explode = row[column_name]
        field_map = get_field_map(item_to_explode)
        for field_name, idx in explosion.items():
            if field_name in field_map:
                if FIELD_NAME_FORMATS[field_name] == "str":
                    # Format this as a KGTK string.
                    newvalue = '"' + str(field_map[field_name]) + '"'
                else:
                    # Convert everything else to a KGTK number or symbol
                    newvalue = str(field_map[field_name])
                row[idx] = newvalue
            else:
                row[idx] = "" # In case we are overwriting an existing column.
        # print(row)

    if ctxPipeline:
        ctxPipeline.create_output(df)
    return df

def unstringify(s, unescape_pipe = True):
    """Convert a KGTK formatted string into an internal string.  The language
    code and suffix are not returned.
    """
    if s.startswith("'"):
        s, language = s.rsplit("@", 1)
    if unescape_pipe:
        s = s.replace('\\|', '|')
    return ast.literal_eval(s)


def is_empty(value):
    if not value:
        return True

def is_string(value, verbose=False):
    """
        Return True if the first character  is '"'.

        Strings begin and end with double quote (").  Any internal double
        quotes must be escaped with backslash (\").  Triple-double quoted
    """
    if not value.startswith('"'):
        return False
    strict_string_re = re.compile(r'^"(?P<text>(?:[^"\\]|\\.)*)"$')
    m = strict_string_re.match(value)
    if m is None:
        if verbose:
            print("KgtkValue.strict_string_re.match failed for %s" % value, flush=True) # file=self.error_file
        return False

    return {"data_type": DataType.STRING.lower(),
            "text": m.group("text"),
            "decoded_text": unstringify('"' + m.group("text") + '"')}

def validate_lang(lang, verbose=False):
    return True
    import pycountry
    import iso639
    save_lang = lang
    country_or_dialect = ""
    if "-" in lang:
        (lang, country_or_dialect) = lang.split("-", 1)
        if verbose:
            print("'%s' split into '%s' and '%s'" % (save_lang, lang, country_or_dialect))
    if len(lang) == 2:
        # Two-character language codes.
        if pycountry.languages.get(alpha_2=lang) is not None:
            if verbose:
                print("pycountry.languages.get(alpha_2=lang) succeeded")
            return True

    elif len(lang) == 3:
        # Three-character language codes.
        if pycountry.languages.get(alpha_3=lang) is not None:
            if verbose:
                print("pycountry.languages.get(alpha_3=lang) succeeded")
            return True
    # Perhaps this is a collective (language family) code from ISO 639-5?
    try:
        iso639.languages.get(part5=lang)
        if verbose:
            print("iso639.languages.get(part5=lang) succeeded")
        return True
    except KeyError:
        pass


    # If there's a table of additional language codes, check there:
    if verbose:
        print("Using the default list of additional language codes.")
    additional_language_codes = [
        # New codes:
        "cnr", # Montenegrin.  Added 21-Dec-2017. https://iso639-3.sil.org/code/cnr
        "hyw", # Western Armenian.  Added 23-Jan-2018. https://iso639-3.sil.org/code/hyw
        "szy", # Sakizawa.  Added 25-Jan-2019. https://iso639-3.sil.org/code/szy

        # Obsolete codes:
        "bh", # Bihari lanuages, apparently replaced by "bih".
        "mo", # Moldavian. Retired 3-Nov-2008. Replaced by the codes for Romanian.
              # http://www.personal.psu.edu/ejp10/blogs/gotunicode/2008/11/language-tage-mo-for-moldovan.html
        "eml", # Emiliano-Romagnolo. Split and retired 16-Jan-2009. https://iso639-3.sil.org/code/eml
    ]
    if lang in additional_language_codes:
        if verbose:
            print("found in the table of additional languages.")
        return True

    if verbose:
        print("Not found.")
    return False

def is_language_qualified_string(value, verbose=False):
    if not value.startswith("'"):
        return False
    # We are certain that this is a language qualified string, although we haven't checked validity.
    # Validate the language qualified string.
    strict_language_qualified_string_re = re.compile(r"^'(?P<text>(?:[^'\\]|\\.)*)'@(?P<lang_suffix>(?P<lang>[a-zA-Z]{2,3})(?P<suffix>-[a-zA-Z0-9]+)?)$")
    m = strict_language_qualified_string_re.match(value)
    if m is None:
        if verbose:
            print("KgtkValue.strict_language_qualified_string_re.match failed for %s" % value, flush=True) # file=error_file,
        return False
    # Extract the combined lang and suffix for use by the LanguageValidator.
    lang_and_suffix = m.group("lang_suffix")
    if not validate_lang(lang_and_suffix.lower()):
        return False
    return {
        "data_type": DataType.LANGUAGE_QUALIFIED_STRING.lower(),
        "valid": True,
        "text": m.group("text"),
        "decoded_text": unstringify('"' + m.group("text") + '"'),
        "language": m.group("lang"),
        "language_suffix": m.group("suffix")
    }

def is_number_or_quantity(value, verbose=False):
    if not value.startswith(("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "+", "-", ".")):
        return False
        # Numeric literals.
    # Use lax pattern
    number_or_quantity_re = re.compile(r'^' + lax_number_or_quantity_pat + r'$')
    m = number_or_quantity_re.match(value)
    if m is None:
        if verbose:
            print("KgtkValue.is_number_or_quantity.number_or_quantity_re.match failed for %s" % (repr(value)), flush=True) # file=self.error_file
        return False
    # Extract the number or quantity components:
    numberstr = m.group("number")
    low_tolerancestr = m.group("low_tolerance")
    high_tolerancestr = m.group("high_tolerance")
    si_units = m.group("si_units")
    units_node = m.group("units_node")
    if low_tolerancestr is None:
        low_tolerance = None
    else:
        try:
            low_tolerance = float(low_tolerancestr)
        except ValueError:
            if verbose:
                print("KgtkValue.is_number_or_quantity: low tolerance is not float for %s" % (repr(value)),
                         flush=True) # file=self.error_file
            return False
    if high_tolerancestr is None:
        high_tolerance = None
    else:
        try:
            high_tolerance = float(high_tolerancestr)
        except ValueError:
            if verbose:
                print("KgtkValue.is_number_or_quantity: high tolerance is not float for %s" % (repr(value)),
                        flush=True) # file=self.error_file,
            return False
    # For convenience, convert the numeric part to int or float:
    if numberstr is None:
        raise ValueError("Missing numeric part")
    n = numberstr.lower()
    if "." in n or ("e" in n and not n.startswith("0x")):
        number = float(n)
    else:
        number = int(n)
    data_type =  DataType.QUANTITY.lower()
    # if low_tolerancestr is not None or high_tolerancestr is not None or si_units is not None or units_node is not None:
    #     # We can be certain that this is a quantity.
    #     data_type =  DataType.QUANTITY.lower()
    # else:
    #     # We can be certain that this is a number
    #     data_type = DataType.NUMBER.lower()
    return {
        "data_type": data_type,
        "valid": True,
        "numberstr": numberstr,
        "number": number,
        "low_tolerancestr": low_tolerancestr,
        "low_tolerance": low_tolerance,
        "high_tolerancestr": high_tolerancestr,
        "high_tolerance": high_tolerance,
        "si_units": si_units,
        "units_node": units_node
    }


def is_location_coordinates(value, verbose=False):
    """
        Return False if this value is a list and idx is None.
        Otherwise, return True if the value looks like valid location coordinates.

        @043.26193/010.92708
    """
    if not value.startswith("@"):
        return False
    # We are certain that this is location coordinates, although we haven't checked validity.
    degrees_pat: str = r'(?:[-+]?(?:\d+(?:\.\d*)?)|(?:\.\d+))'
    location_coordinates_re = re.compile(r'^@(?P<lat>{degrees})/(?P<lon>{degrees})$'.format(degrees=degrees_pat))
    m = location_coordinates_re.match(value)
    if m is None:
        if verbose:
            print("KgtkValue.location_coordinates_re.match failed for %s" % value, flush=True) # file=self.error_file,
            return False

    latstr: str = m.group("lat")
    lonstr: str = m.group("lon")

    # Latitude normally runs from -90 to +90:
    minimum_valid_lat = -90
    maximum_valid_lat = 90
    try:
        lat = float(latstr)
        if lat < minimum_valid_lat:
            if verbose:
                print("KgtkValue.is_location_coordinates: lat less than minimum %f for %s" % (minimum_valid_lat, repr(value)),
                     flush=True) # file=self.error_file
            return False
        elif lat > maximum_valid_lat:
            if verbose:
                print("KgtkValue.is_location_coordinates: lat greater than maximum %f for %s" % (maximum_valid_lat, repr(value)),
                         flush=True) # file=self.error_file,
            return False
    except ValueError:
        if verbose:
            print("KgtkValue.is_location_coordinates: lat is not float for %s" % (repr(value)),
                    flush=True) #  file=self.error_file,
        return False

    # Longitude normally runs from -180 to +180:
    minimum_valid_lon = -180
    maximum_valid_lon = 180
    try:
        lon: float = float(lonstr)
        if  lon < minimum_valid_lon:
            if verbose:
                print("KgtkValue.is_location_coordinates: lon less than minimum %f for %s" % (minimum_valid_lon, repr(value)),
                             flush=True) # file=self.error_file,
            return False
        elif lon > maximum_valid_lon:
            if verbose:
                print("KgtkValue.is_location_coordinates: lon greater than maximum %f for %s" % (maximum_valid_lon, repr(value)),
                             flush=True) #  file=self.error_file,
            return False
    except ValueError:
        if verbose:
            print("KgtkValue.is_location_coordinates: lon is not float for %s" % (repr(value)),
                     flush=True) # file=self.error_file,
        return False

    return {
        "data_type": DataType.LOCATION_COORDINATES.lower(),
        "valid": True,
        "latitudestr": latstr,
        "latitude": lat,
        "longitudestr": lonstr,
        "longitude": lon
    }

def is_date_and_times(value, verbose=False):
    """
        Return True if the value looks like valid date and times
        literal based on ISO-8601.

        Valid date formats:
        YYYY
        YYYY-MM
        YYYYMMDD
        YYYY-MM-DD

        Valid date and time formats
        YYYYMMDDTHH
        YYYY-MM-DDTHH
        YYMMDDTHHMM
        YYYY-MM-DDTHH:MM
        YYMMDDTHHMMSS
        YYYY-MM-DDTHH:MM:SS

        Optional Time Zone suffix for date and time:
        Z
        +HH
        -HH
        +HHMM
        -HHMM
        +HH:MM
        -HH:MM

        NOTE: This code also accepts the following, which are disallowed by the standard:
        YYYY-
        YYYYT...
        YYYYMM
        YYYYMMT...
        YYYY-MMT...

        Note:  IS0-8601 disallows 0 for month or day, e.g.:
        Invalid                   Correct
        1960-00-00T00:00:00Z/9    1960-01-01T00:00:00Z/9
    """
    if not value.startswith("^"):
        return False
    lax_date_and_times_re = re.compile(r'^{date_and_times}$'.format(date_and_times=lax_date_and_times_pat))
    m = lax_date_and_times_re.match(value)
    if m is None:
        if verbose:
            print("KgtkValue.lax_date_and_times_re.match(%s) failed." % repr(value), flush=True) # file=self.error_file,
        return False

    date = m.group("date")
    time = m.group("time")
    date_and_time = m.group("date_and_time")

    yearstr = m.group("year")
    monthstr = m.group("month")
    daystr = m.group("day")
    hourstr = m.group("hour")
    minutesstr = m.group("minutes")
    secondsstr = m.group("seconds")
    zonestr = m.group("zone")
    precisionstr = m.group("precision")
    iso8601extended = m.group("hyphen") is not None

    minimum_valid_year = 1583
    maximum_valid_year = 2100

    # Validate the year:
    if yearstr is None or len(yearstr) == 0:
        if verbose:
            print("KgtkValue.is_date_and_times: no year in %s." % repr(value),  flush=True) # file=self.error_file,
        return False # Years are mandatory
    try:
        year = int(yearstr)
    except ValueError:
        if verbose:
            print("KgtkValue.is_date_and_times: year not int in %s." % repr(value), flush=True) # file=self.error_file,
        return False
    if year < minimum_valid_year:
        if verbose:
            print("KgtkValue.is_date_and_times: year less than minimum %d: %s." % (minimum_valid_year, repr(value)),
                     flush=True) # file=self.error_file,
        return False
    elif year > maximum_valid_year:
        if verbose:
            print("KgtkValue.is_date_and_times: year greater than maximum %d: %s." % (maximum_valid_year, repr(value)),
                        flush=True) # file=self.error_file,
        return False

    if monthstr is None:
        month = None
    else:
        try:
            month = int(monthstr)
        except ValueError:
            if verbose:
                print("KgtkValue.is_date_and_times: month not int in %s." % repr(value), flush=True)
            return False # shouldn't happen
        if month == 0:
            if verbose:
                print("KgtkValue.is_date_and_times: month 0 disallowed in %s." % repr(value), flush=True)
            return False # month 0 was disallowed.

    if daystr is None:
        day = None
    else:
        try:
            day = int(daystr)
        except ValueError:
            if verbose:
                print("KgtkValue.is_date_and_times: day not int in %s." % repr(value), flush=True)
            return False # shouldn't happen
        if day == 0:
            if verbose:
                print("KgtkValue.is_date_and_times: day 0 disallowed in %s." % repr(value), flush=True)
            return False # day 0 was disallowed.

    # Convert the time fields to ints:
    if hourstr is None:
        hour = None
    else:
        try:
            hour = int(hourstr)
        except ValueError:
            if verbose:
                print("KgtkValue.is_date_and_times: hour not int in %s." % repr(value), flush=True)
            return False # shouldn't happen

    if minutesstr is None:
        minutes = None
    else:
        try:
            minutes = int(minutesstr)
        except ValueError:
            if verbose:
                print("KgtkValue.is_date_and_times: minutes not int in %s." % repr(value), flush=True) # , file=self.error_file
            return False # shouldn't happen

    if secondsstr is None:
        seconds = None
    else:
        try:
            seconds = int(secondsstr)
        except ValueError:
            if verbose:
                print("KgtkValue.is_date_and_times: seconds not int in %s." % repr(value), flush=True) # , file=self.error_file
            return False # shouldn't happen

    if hour is not None and hour == 24:
        if ((minutes is not None and minutes > 0) or (seconds is not None and seconds > 0)):
            if verbose:
                print("KgtkValue.is_date_and_times: hour 24 and minutes or seconds not zero in %s." % repr(value), flush=True) # , file=self.error_file
            return False # An invalid time
        if verbose:
            print("KgtkValue.is_date_and_times: end-of-day value disallowed in %s." % repr(value), flush=True) # , file=self.error_file
        return False

    if precisionstr is None:
        precision = None
    else:
        try:
            precision = int(precisionstr)
        except ValueError:
            if verbose:
                print("KgtkValue.is_date_and_times: precision not int in %s." % repr(value), flush=True) # , file=self.error_file
            return False # shouldn't happen

    # We are fairly certain that this is a valid date and times.
    return {
        "data_type": DataType.DATE_AND_TIMES.lower(),
        "valid": True,
        "date": date,
        "time": time,
        "date_and_time": date_and_time,
        "yearstr": yearstr,
        "monthstr": monthstr,
        "daystr": daystr,
        "hourstr": hourstr,
        "minutesstr": minutesstr,
        "secondsstr": secondsstr,
        "year": year,
        "month": month,
        "day": day,
        "hour": hour,
        "minutes": minutes,
        "seconds": seconds,
        'zonestr': zonestr,
        'precisionstr': precisionstr,
        'precision': precision,
        'iso8601extended': iso8601extended,
    }

def is_extension(value):
    """
        Return True if the first character is !

        Although we refer to the validate parameter in the code below, we
        force self.valid to False.

    """
    if not value.startswith("!"):
        return False
    return True

def is_boolean(value):
    if value != "True" and value != "False":
        return False
    return True

def get_field_map(value): # classify
    if is_empty(value):
        raise ValueError("Cannot find node2.")
        return {"data_type": DataType.EMPTY.lower()}
    # is_list():

    value = str(value) # Convert everything to string.
    string = is_string(value)
    if string: return string

    language_qualified_string = is_language_qualified_string(value)
    if language_qualified_string: return language_qualified_string

    number_or_quantity = is_number_or_quantity(value)
    if number_or_quantity: return number_or_quantity

    location_coordinates = is_location_coordinates(value)
    if location_coordinates: return location_coordinates

    date_and_times = is_date_and_times(value)
    if date_and_times: return date_and_times

    # elif is_extension(value):
    #     return DataType.EXTENSION.lower()

    # elif is_boolean(value):
    #     return DataType.BOOLEAN.lower()
    else:
        # is_symbol:
        return {
            "data_type": DataType.SYMBOL.lower(),
            "symbol": value
        }


def implode_node2(row):
    if row['node2;kgtk:data_type'] == 'quantity':
        if isinstance(row['node2;kgtk:units_node'], str):
            node2 = row['node2'] + row['node2;kgtk:units_node']
        else:
            node2 = row['node2']
    elif row['node2;kgtk:data_type'] == 'date_and_times':
        node2 = '^' + row['node2'] + '/' + str(row['node2;kgtk:precision'])
    elif row['node2;kgtk:data_type'] == 'symbol':
        node2 = row['node2']
    elif row['node2;kgtk:data_type'] == 'location_coordinates':
        node2 = '@' + str(row['node2;kgtk:latitude']) + '/' + str(row['node2;kgtk:longitude'])
    elif row['node2;kgtk:data_type'] == 'string':
        node2 = row['node2;kgtk:text']
    else:
        raise ValueError(f"Data type not recognize: {row['node2;kgtk:data_type']}")
    return node2


def implode(df):
    imploded = df[['id', 'node1', 'label']].copy()
    imploded['node2'] = df.apply(implode_node2, axis=1)
    return imploded


class ExplodePipeline:
    def __init__(self, frame):
        self._dir = tempfile.mkdtemp()
        frame.to_csv(self.input, sep='\t', index=False, quoting=csv.QUOTE_NONE, quotechar='')

    @property
    def dir(self):
        return self._dir

    @property
    def input(self):
        return Path(self._dir, 'input.tsv')

    @property
    def output(self):
        return Path(self._dir, 'exploded.tsv')

    def create_output(self, output_frame):
        output_frame.to_csv(self.output, sep='\t', index=False, quoting=csv.QUOTE_NONE, quotechar='')

    def get_file(self, name):
        return Path(self._dir, name)

    def read_csv(self, name):
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
