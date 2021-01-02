import requests
from loguru import logger
import pandas as pd
import copy
import zipfile
import os
import io
from humanfriendly import format_timespan
from time import perf_counter
from app import TMW as tmw
from datetime import datetime as dt, timedelta, time as dt_time
from geopy.distance import distance
from app import tmw_api_keys
import time
from app import constants
from app.co2_emissions import calculate_co2_emissions

pd.set_option('display.max_columns', 999)
pd.set_option('display.width', 1000)

# Define the (arbitrary waiting period at the airport
_PORT_WAITING_PERIOD = constants.WAITING_PERIOD_PORT


def load_port_database():
    """
    Load the airport database used to do smart calls to Skyscanner API.
    This DB can be reconstructed thanks to the recompute_airport_database (see end of file)
    """
    path = os.path.join(os.getcwd(), 'app/data/df_port_ferries_final_v1.csv')  #
    logger.info(path)
    port_list = pd.read_csv(path)
    port_list['geoloc'] = port_list.apply(lambda x: [x.lat_clean, x.long_clean], axis=1)
    logger.info('load the ferry port db. Here is a random example :')
    logger.info(port_list.sample(1))
    return port_list

def load_ferry_data():
    """
    Load the airport database used to do smart calls to Skyscanner API.
    This DB can be reconstructed thanks to the recompute_airport_database (see end of file)
    """
    path = os.path.join(os.getcwd(), 'app/data/df_ferry_final_v1.csv')  #
    logger.info(path)
    ferry_data = pd.read_csv(path)
    logger.info('load the ferry_data data. Here is a random example :')
    logger.info(ferry_data.sample(1))
    ferry_data['date_dep'] = pd.to_datetime(ferry_data['date_dep'])
    ferry_data['date_arr'] = pd.to_datetime(ferry_data['date_arr'])
    return ferry_data


# When the server starts it logs the airport db (only once)
_PORT_DF = load_port_database()
_FERRY_DATA = load_ferry_data()


def main(query):
    """
    This function takes an object query and returns a list of journey object thanks to the Skyscanner API
    """
    # extract departure and arrival points
    departure_point = query.start_point
    arrival_point = query.end_point
    # extract departure date as 'yyyy-mm-dd'
    date_departure = query.departure_date
    return get_ferries(date_departure, None, departure_point, arrival_point)


def get_ferries(date_departure, date_return, departure_point, arrival_point):
    """
    We create a ferry journey based on the ferry database we scraped
    """
    # Find relevant ports
    port_deps, port_arrs = get_ports_from_geo_locs(departure_point, arrival_point)

    # Find journeys
    journeys = _FERRY_DATA[(_FERRY_DATA.port_dep.isin(port_deps.port_clean.unique())) &
                           _FERRY_DATA.port_arr.isin(port_arrs.port_clean.unique())]

    journeys['date_dep'] = pd.to_datetime(journeys.date_dep)
    journeys = journeys[journeys.date_dep > date_departure]

    if len(journeys) == 0:
        logger.info(f'No ferry journey was found')
        return None

    journey_list = list()

    for index, row in journeys.iterrows():

        distance_m = row.distance_m
        local_emissions = calculate_co2_emissions(constants.TYPE_PLANE, constants.DEFAULT_CITY,
                                                  constants.DEFAULT_FUEL, constants.NB_SEATS_TEST,
                                                  constants.DEFAULT_NB_KM) * \
                          constants.DEFAULT_NB_PASSENGERS * distance_m
        journey_steps = list()
        journey_step = tmw.Journey_step(0,
                                    _type=constants.TYPE_WAIT,
                                    label=f'Arrive at the port {format_timespan(_PORT_WAITING_PERIOD)} before departure',
                                    distance_m=0,
                                    duration_s=_PORT_WAITING_PERIOD,
                                    price_EUR=[0],
                                    gCO2=0,
                                    departure_point=[row.lat_clean_dep, row.long_clean_dep],
                                    arrival_point=[row.lat_clean_dep, row.long_clean_dep],
                                    departure_date=row.date_dep - timedelta(seconds=_PORT_WAITING_PERIOD),
                                    arrival_date=row.date_dep ,
                                    geojson=[],
                                    )
        journey_steps.append(journey_step)

        journey_step = tmw.Journey_step(1,
                                    _type=constants.TYPE_FERRY,
                                    label=f'Sail Ferry from {row.port_dep} to {row.port_arr}',
                                    distance_m=distance_m,
                                    duration_s= (row.date_arr - row.date_dep).seconds,
                                    price_EUR=[row.price_clean_ar_eur/2],
                                    gCO2=local_emissions,
                                    departure_point=[row.lat_clean_dep, row.long_clean_dep],
                                    arrival_point=[row.lat_clean_arr, row.long_clean_arr],
                                    departure_date=row.date_dep,
                                    arrival_date=row.date_arr,
                                    geojson=[],
                                    )

        journey_steps.append(journey_step)

        journey = tmw.Journey(0, steps=journey_steps,
                                        departure_date= journey_steps[0].departure_date,
                                        arrival_date= journey_steps[1].arrival_date,
                                   )
        journey.total_gCO2 = local_emissions
        journey.category = constants.CATEGORY_FERRY_JOURNEY
        journey.booking_link = 'https://www.ferrysavers.co.uk/ferry-routes.htm'
        journey.departure_point = [row.lat_clean_dep, row.long_clean_dep]
        journey.arrival_point = [row.lat_clean_arr, row.long_clean_arr]
        journey.update()
        journey_list.append(journey)

    return journey_list


# Find the stops close to a geo point
def get_ports_from_geo_locs(geoloc_dep, geoloc_arrival):
    """
    This function takes in the departure and arrival points of the TMW journey and returns
        the most relevant corresponding Skyscanner cities to build a plane journey
    We first look at the closest airport, if the city is big enough we keep only one city,
        if not we add the next closest airport and if this 2nd city is big enough we keep only the two first
        else we look at the 3rd and final airports
    """
    stops_tmp = _PORT_DF.copy()
    # compute proxi for distance (since we only need to compare no need to take the earth curve into account...)
    stops_tmp['distance_dep'] = stops_tmp.apply(lambda x: (x.lat_clean - geoloc_dep[0]) ** 2 + (x.long_clean - geoloc_dep[1]) ** 2, axis=1)
    stops_tmp['distance_arrival'] = stops_tmp.apply(lambda x: (x.lat_clean - geoloc_arrival[0]) ** 2 + (x.long_clean - geoloc_arrival[1]) ** 2, axis=1)

    # We get the 2 closest ports for departure and arrival
    port_deps = stops_tmp.sort_values(by='distance_dep').head(2)
    port_arrs = stops_tmp.sort_values(by='distance_arrival').head(2)
    return port_deps, port_arrs



if __name__ == '__main__':
    main()
