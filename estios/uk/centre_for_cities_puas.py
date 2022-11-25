#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Data source and configuration for the Centre for Cities definitions of UK Cities.

Todo:
    * Address issues with Bournemouth between 2014 and 2019 definitions
"""

from datetime import datetime
from logging import getLogger
from typing import Final

from ..sources import MetaData

logger = getLogger(__name__)

CentreForCitiesSpec = dict[str, tuple[str, ...]]

CENTRE_FOR_CITIES_2022_CITY_REGIONS_METADATA: Final[MetaData] = MetaData(
    name="Centre for Cities 2022 Primary Urban Areas (PUA)",
    year=2022,
    description=(
        "A hand transcription of a pdf listing all UK and "
        "Northern Ireland Primary Urban Areas (PUAs)"
    ),
    region="United Kingdom and Northern Ireland",
    authors={
        "Centre for Cities": "https://www.centreforcities.org/city-by-city/puas/",
        "Centre for Urban and Regional Development Studies (CURDS)": {
            "url": "https://www.ncl.ac.uk/curds/",
            "institution": "University of Newcastle",
        },
    },
    url="https://www.centreforcities.org/wp-content/uploads/2022/08/2022-PUA-Table.pdf",
    date_time_obtained=datetime(2022, 8, 3, 10, 27),
    dict_key_appreviation="pua",
    # license=
)

logger.warning(f"License info for Centre for Cities data needed.")


# Changes from https://www.centreforcities.org/wp-content/uploads/2014/07/12-03-19-Primary-Urban-Areas-deffinitions.pdf
CENTRE_FOR_CITIES_2014_PUAS: Final[CentreForCitiesSpec] = {
    "Bournemouth": (
        "Bournemouth",
        "Christchurch",
        "Poole",
    ),
}


CENTRE_FOR_CITIES_2022_CITY_PUAS: Final[CentreForCitiesSpec] = {
    "Aberdeen": ("Aberdeen",),
    "Aldershot": (
        "Rushmoor",
        "Surrey Heath",
    ),
    "Barnsley": ("Barnsley",),
    "Basildon": ("Basildon",),
    "Belfast": (
        "Belfast",
        "Lisburn and Castlereagh",
    ),
    "Birkenhead": ("Wirral",),
    "Birmingham": (
        "Birmingham",
        "Dudley",
        "Sandwell",
        "Solihull",
        "Walsall",
        "Wolverhampton",
    ),
    "Blackburn": ("Blackburn with Darwen",),
    "Blackpool": (
        "Blackpool",
        "Fylde",
    ),
    "Bournemouth": (
        "Dorset",
        "Bournemouth Christchurch and Poole",
    ),
    "Bradford": ("Bradford",),
    "Brighton": (
        "Adur",
        "Brighton and Hove",
    ),
    "Bristol": (
        "City of Bristol",
        "South Gloucestershire",
    ),
    "Burnley": (
        "Burnley",
        "Pendle",
    ),
    "Cambridge": ("Cambridge",),
    "Cardiff": ("Cardiff",),
    "Chatham": ("Medway",),
    "Coventry": ("Coventry",),
    "Crawley": ("Crawley",),
    "Derby": ("Derby",),
    "Doncaster": ("Doncaster",),
    "Dundee": ("Dundee",),
    "Edinburgh": ("Edinburgh",),
    "Exeter": ("Exeter",),
    "Glasgow": (
        "East Dunbartonshire",
        "East Renfrewshire",
        "Glasgow",
        "Renfrewshire",
    ),
    "Gloucester": ("Gloucester",),
    "Huddersfield": ("Kirklees",),
    "Hull": ("Kingston upon Hull",),
    "Ipswich": ("Ipswich",),
    "Leeds": ("Leeds",),
    "Leicester": (
        "Blaby",
        "Leicester",
        "Oadby and Wigston",
    ),
    "Liverpool": (
        "Knowsley",
        "Liverpool",
    ),
    "London": (
        "Barking and Dagenham",
        "Barnet",
        "Bexley",
        "Brent",
        "Bromley",
        "Broxbourne",
        "Camden",
        "City of London",
        "Croydon",
        "Dartford",
        "Ealing",
        "Elmbridge",
        "Enfield",
        "Epping Forest",
        "Epsom and Ewell",
        "Gravesham",
        "Greenwich",
        "Hackney",
        "Hammersmith and Fulham",
        "Haringey",
        "Harrow",
        "Havering",
        "Hertsmere",
        "Hillingdon",
        "Hounslow",
        "Islington",
        "Kensington and Chelsea",
        "Kingston upon Thames",
        "Lambeth",
        "Lewisham",
        "Merton",
        "Newham",
        "Redbridge",
        "Richmond upon Thames",
        "Runnymede",
        "Southwark",
        "Spelthorne",
        "Sutton",
        "Three Rivers",
        "Tower Hamlets",
        "Waltham Forest",
        "Wandsworth",
        "Watford",
        "Westminster",
        "Woking",
    ),
    "Luton": ("Luton",),
    "Manchester": (
        "Bolton",
        "Bury",
        "Manchester",
        "Oldham",
        "Rochdale",
        "Salford",
        "Stockport",
        "Tameside",
        "Trafford",
    ),
    "Mansfield": (
        "Ashfield",
        "Mansfield",
    ),
    "Middlesbrough": (
        "Middlesbrough",
        "Redcar and Cleveland",
        "Stockton-on-Tees",
    ),
    "Milton Keynes": ("Milton Keynes",),
    "Newcastle": (
        "Gateshead",
        "Newcastle upon Tyne",
        "North Tyneside",
        "South Tyneside",
    ),
    "Newport": (
        "Newport",
        "Torfaen",
    ),
    "Northampton": ("West Northamptonshire",),
    "Norwich": (
        "Broadland",
        "Norwich",
    ),
    "Nottingham": (
        "Broxtowe",
        "Erewash",
        "Gedling",
        "Nottingham",
    ),
    "Oxford": ("Oxford",),
    "Peterborough": ("Peterborough",),
    "Plymouth": ("Plymouth",),
    "Portsmouth": (
        "Portsmouth",
        "Fareham",
        "Gosport",
        "Havant",
    ),
    "Preston": (
        "Chorley",
        "Preston",
        "South Ribble",
    ),
    "Reading": (
        "Reading",
        "Wokingham",
    ),
    "Sheffield": (
        "Rotherham",
        "Sheffield",
    ),
    "Slough": ("Slough",),
    "Southampton": (
        "Eastleigh",
        "Southampton",
    ),
    "Southend": (
        "Castlepoint",
        "Southend-on-Sea",
        "Rochford",
    ),
    "Stoke": (
        "Newcastle-under-Lyme",
        "Stoke-on-Trent",
    ),
    "Sunderland": ("Sunderland",),
    "Swansea": (
        "Neath Port Talbot",
        "Swansea",
    ),
    "Swindon": ("Swindon",),
    "Telford": ("Telford and Wrekin",),
    "Wakefield": ("Wakefield",),
    "Warrington": ("Warrington",),
    "Wigan": ("Wigan",),
    "Worthing": ("Worthing",),
    "York": ("York",),
}
