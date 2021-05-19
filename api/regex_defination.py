

# The following lexical analysis is based on:
# https://docs.python.org/3/reference/lexical_analysis.html

# The long integer suffix was part of Python 2.  It was dropped in Python 3.
long_suffix_pat = r'[lL]'

plus_or_minus_pat = r'[-+]'

# Integer literals.
#
# Decimal integers, allowing leading zeros.
digit_pat = r'[0-9]'
decinteger_pat = r'(?:{digit}(?:_?{digit})*{long_suffix}?)'.format(digit=digit_pat,
                                                                        long_suffix=long_suffix_pat)
bindigit_pat = r'[01]'
bininteger_pat = r'(?:0[bB](":_?{bindigit})+{long_suffix})'.format(bindigit=bindigit_pat,
                                                                        long_suffix=long_suffix_pat)
octdigit_pat = r'[0-7]'
octinteger_pat = r'(?:0[oO](":_?{octdigit})+{long_suffix})'.format(octdigit=octdigit_pat,
                                                                        long_suffix=long_suffix_pat)
hexdigit_pat = r'[0-7a-fA-F]'
hexinteger_pat = r'(?:0[xX](":_?{hexdigit})+{long_suffix})'.format(hexdigit=hexdigit_pat,
                                                                        long_suffix=long_suffix_pat)
    
integer_pat = r'(?:{decinteger}|{bininteger}|{octinteger}|{hexinteger})'.format(decinteger=decinteger_pat,
                                                                                        bininteger=bininteger_pat,
                                                                                        octinteger=octinteger_pat,
                                                                                        hexinteger=hexinteger_pat)

# Floating point literals.
digitpart_pat = r'(?:{digit}(?:_?{digit})*)'.format(digit=digit_pat)
fraction_pat = r'(?:\.{digitpart})'.format(digitpart=digitpart_pat)
pointfloat_pat = r'(?:{digitpart}?{fraction})|(?:{digitpart}\.)'.format(digitpart=digitpart_pat,
                                                                                fraction=fraction_pat)
exponent_pat = r'(?:[eE]{plus_or_minus}?{digitpart})'.format(plus_or_minus=plus_or_minus_pat,
                                                                    digitpart=digitpart_pat)
exponentfloat_pat = r'(?:{digitpart}|{pointfloat}){exponent}'.format(digitpart=digitpart_pat,
                                                                            pointfloat=pointfloat_pat,
                                                                            exponent=exponent_pat)
floatnumber_pat = r'(?:{pointfloat}|{exponentfloat})'.format(pointfloat=pointfloat_pat,
                                                                    exponentfloat=exponentfloat_pat)

# Real literals (nothing imaginary).
real_pat = r'(?:{plus_or_minus}?(?:{integer}|{floatnumber}))'.format(plus_or_minus=plus_or_minus_pat,
                                                                            integer=integer_pat,
                                                                            floatnumber=floatnumber_pat)

# Imaginary literals.
imagnumber_pat = r'(?:{floatnumber}|{digitpart})[jJ]'.format(floatnumber=floatnumber_pat,
                                                                    digitpart=digitpart_pat)

# Numeric literals.
numeric_pat = r'(?:{plus_or_minus}?(?:{integer}|{floatnumber}|{imagnumber}))'.format(plus_or_minus=plus_or_minus_pat,
                                                                                            integer=integer_pat,
                                                                                            floatnumber=floatnumber_pat,
                                                                                            imagnumber=imagnumber_pat)
# Numeric literals with component labeling:
number_pat = r'(?P<number>{numeric})'.format(numeric=numeric_pat)

# Tolerances
tolerance_pat = r'(?:\[(?P<low_tolerance>{numeric}),(?P<high_tolerance>{numeric})\])'.format(numeric=numeric_pat)

# SI units taken from:
# http://www.csun.edu/~vceed002/ref/measurement/units/units.pdf
#
# Note: if Q were in this list, it would conflict with Wikidata nodes (below).
si_unit_pat = r'(?:m|kg|s|C|K|mol|cd|F|M|A|N|ohms|V|J|Hz|lx|H|Wb|V|W|Pa)'
si_power_pat = r'(?:-1|2|3)' # Might need more.
si_combiner_pat = r'[./]'
si_pat = r'(?P<si_units>{si_unit}{si_power}?(?:{si_combiner}{si_unit}{si_power}?)*)'.format(si_unit=si_unit_pat,
                                                                                        si_combiner=si_combiner_pat,
                                                                                        si_power=si_power_pat)
# Wikidata nodes (for units):
#
# https://www.wikidata.org/wiki/Wikidata:Identifiers
#
#    "Each Wikidata entity is identified by an entity ID, which is a number prefixed by a letter."
nonzero_digit_pat = r'[1-9]'
units_node_pat = r'(?P<units_node>Q{nonzero_digit}{digit}*)'.format(nonzero_digit=nonzero_digit_pat,
                                                                            digit=digit_pat)
# 30-Jun-2020: Amandeep requested underscore and increased laxness for
# datamart.

#lax_units_node_pat = r'(?P<units_node>Q[0-9A-Za-z][-0-9A-Za-z]*)'
lax_units_node_pat = r'(?P<units_node>Q[-_0-9A-Za-z]+)'


units_pat = r'(?:{si}|{units_node})'.format(si=si_pat,
                                                    units_node=units_node_pat)

lax_units_pat = r'(?:{si}|{units_node})'.format(si=si_pat,
                                                        units_node=lax_units_node_pat)


# This definition matches numbers or quantities.
number_or_quantity_pat = r'{numeric}{tolerance}?{units}?'.format(numeric=number_pat,
                                                                        tolerance=tolerance_pat,
                                                                        units=units_pat)

lax_number_or_quantity_pat = r'{numeric}{tolerance}?{units}?'.format(numeric=number_pat,
                                                                            tolerance=tolerance_pat,
                                                                            units=lax_units_pat)


# https://en.wikipedia.org/wiki/ISO_8601
#
# The "lax" patterns allow month 00 and day 00, which are excluded by ISO 8601.
# We will allow those values when requested in the code below.
#
# The first possible hyphen position determines whether we will parse in
# value as a "basic" (no hyphen) or "extended" format date/time.  A
# mixture is not permitted: either all hyphens (colons in the time
# section) must be present, or none.
#
# Year-month-day
year_pat: str = r'(?P<year>[-+]?[0-9]{4})'
month_pat: str = r'(?P<month>1[0-2]|0[1-9])'
day_pat: str = r'(?P<day>3[01]|0[1-9]|[12][0-9])'
date_pat: str = r'(?:{year}(?:(?P<hyphen>-)?{month}?(?:(?(hyphen)-){day})?)?)'.format(year=year_pat,
                                                                                        month=month_pat,
                                                                                        day=day_pat)

lax_year_pat: str = r'(?P<year>[-+]?[0-9]{4}(?:[0-9]+(?=-))?)' # Extra digits must by followed by hyphen.
lax_month_pat: str = r'(?P<month>1[0-2]|0[0-9])'
lax_day_pat: str = r'(?P<day>3[01]|0[0-9]|[12][0-9])'
lax_date_pat: str = r'(?P<date>(?:{year}(?:(?P<hyphen>-)?{month}?(?:(?(hyphen)-){day})?)?))'.format(year=lax_year_pat,
                                                                                                    month=lax_month_pat,
                                                                                                    day=lax_day_pat)
# hour-minutes-seconds
#
# NOTE: hour 24 is valid only when minutes and seconds are 00
# and options.allow_end_of_day is True
hour_pat: str = r'(?P<hour>2[0-4]|[01][0-9])'
minutes_pat: str = r'(?P<minutes>[0-5][0-9])'
seconds_pat: str = r'(?P<seconds>[0-5][0-9])'

# NOTE: It might be the case that the ":" before the minutes in the time zone pattern
# should be conditioned upon the hyphen indicator.  The Wikipedia article doesn't
# mention this requirement.
#
# NOTE: This pattern accepts a wider range of offsets than actually occur.
#
# TODO: consult the actual standard about the colon.
zone_pat: str = r'(?P<zone>Z|[-+][01][0-9](?::?[0-5][0-9])?)'

time_pat: str = r'(?P<time>(?:{hour}(?:(?(hyphen):){minutes}(?:(?(hyphen):){seconds})?)?{zone}?))'.format(hour=hour_pat,
                                                                                                            minutes=minutes_pat,
                                                                                                            seconds=seconds_pat,
                                                                                                            zone=zone_pat)

precision_pat: str = r'(?P<precision>[0-1]?[0-9])'

lax_date_and_times_pat: str = r'(?:\^(?P<date_and_time>{date}(?:T{time})?)(?:/{precision})?)'.format(date=lax_date_pat,
                                                                                                        time=time_pat,
                                                                                                        precision=precision_pat)

