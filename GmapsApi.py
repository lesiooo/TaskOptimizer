import googlemaps
import itertools
from datetime import datetime, timedelta
import json
from flask import Flask, request, abort
import glob

app = Flask(__name__)

with open('data/apikey.txt') as f:
    api_key = f.readline()
    f.close()

gmaps = googlemaps.Client(key=api_key)


def find_routes_between_cities(city_list, single_perm=False):
    traces_without_tolls = []
    traces_with_tolls = []
    if single_perm:
        try:
            direction_res_without_tolls = gmaps.directions(origin=city_list[0], destination=city_list[1], mode='driving',
                                                           avoid=['tolls'])
            distance = direction_res_without_tolls[0]['legs'][0]['distance']['value']
            duration = direction_res_without_tolls[0]['legs'][0]['duration']['value']
            traces_without_tolls.append([city_list[0], city_list[1], distance, duration])

            direction_res_with_tolls = gmaps.directions(origin=city_list[0], destination=city_list[1], mode='driving')
            distance = direction_res_with_tolls[0]['legs'][0]['distance']['value']
            duration = direction_res_with_tolls[0]['legs'][0]['duration']['value']
            traces_with_tolls.append([city_list[0], city_list[1], distance, duration])

        except Exception as e:
            print('Exception: ', e)

    else:
        for item in list(itertools.permutations(set(city_list), 2)):
            try:
                direction_res_without_tolls = gmaps.directions(origin=item[0], destination=item[1], mode='driving',
                                                               avoid=['tolls'])
                distance = direction_res_without_tolls[0]['legs'][0]['distance']['value']
                duration = direction_res_without_tolls[0]['legs'][0]['duration']['value']
                traces_without_tolls.append([item[0], item[1], distance, duration])

                direction_res_with_tolls = gmaps.directions(origin=item[0], destination=item[1], mode='driving')
                distance = direction_res_with_tolls[0]['legs'][0]['distance']['value']
                duration = direction_res_with_tolls[0]['legs'][0]['duration']['value']
                traces_with_tolls.append([item[0], item[1], distance, duration])
            except Exception as e:
                print('Exception: ', e)
    return traces_without_tolls, traces_with_tolls


def is_location_in_file(city_list):
    locations = load_locations_from_file()

    for item in list(itertools.permutations(set(city_list), 2)):
        city_key = str(item[0]) + str(item[1])
        if city_key not in locations:
            append_traces_to_files(str(item[0]), str(item[1]))


def append_traces_to_files(city1, city2):
    traces_without_tolls, traces_with_tolls = find_routes_between_cities([city1, city2], single_perm=True)

    with open('data/location_without_tolls.txt', 'a', encoding='utf-8') as f:
        for item in traces_without_tolls:
            line_to_save = '{};{};{};{}\n'.format(str(item[0]), str(item[1]),
                                                  str(item[2]), str(item[3]))
            f.write(line_to_save)

    with open('data/location_with_tolls.txt', 'a', encoding='utf-8') as f:
        for item in traces_with_tolls:
            line_to_save = '{};{};{};{}\n'.format(str(item[0]), str(item[1]),
                                                  str(item[2]), str(item[3]))
            f.write(line_to_save)



def save_traces(city_list):
    traces_without_tolls, traces_with_tolls = find_routes_between_cities(city_list)

    with open('data/location_without_tolls.txt', 'a', encoding='utf-8') as f:
        for item in traces_without_tolls:
            line_to_save = '{};{};{};{}\n'.format(str(item[0]), str(item[1]), str(item[2]), str(item[3]))
            f.write(line_to_save)

    with open('data/location_with_tolls.txt', 'a', encoding='utf-8') as f:
        for item in traces_with_tolls:
            line_to_save = '{};{};{};{}\n'.format(str(item[0]), str(item[1]), str(item[2]), str(item[3]))
            f.write(line_to_save)


def load_locations_from_file(avoid_tolls=False):
    locations = {}
    if avoid_tolls:
        link = 'data/location_without_tolls.txt'
    else:
        link = 'data/location_with_tolls.txt'

    with open(link, encoding='utf-8') as locations_file:
        for line in locations_file.readlines():
            try:
                line_values = line.replace('\n', '').split(';')
                city_key = str(line_values[0]) + str(line_values[1])
                locations[city_key] = {'distance': line_values[2], 'time': line_values[3]}
            except Exception as e:
                print(e, line_values)

    return locations



def find_min_route(cities, locations):
    permutation_with_data = []
    for iter, single_permutation in enumerate(list(itertools.permutations(cities[1:-1]))):

        permutation_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        full_trace_km = 0
        full_trace = list(single_permutation)
        full_trace.append(cities[len(cities) - 1])  # add Last City in trace
        full_trace.insert(0, cities[0])  # add first city in trace

        for iter, city in enumerate(full_trace):

            if len(full_trace) - 1 > iter:
                city_key = str(city) + str(full_trace[iter + 1])
                try:
                    trace_time = locations.get(city_key)['time']
                    trace_km = locations.get(city_key)['distance']
                    permutation_time += timedelta(seconds=int(trace_time))
                    full_trace_km += int(int(trace_km) / 1000)
                except Exception as e:
                    print(e, city_key)

        full_trace_time = permutation_time - datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        permutation_with_data.append([full_trace, full_trace_time.total_seconds(), full_trace_km])
    return min(permutation_with_data, key=lambda t: t[1]), min(permutation_with_data, key=lambda t: t[2])


def detect_location_files(city_list):
    files = glob.glob('*/*.txt', recursive=True)
    print(files)
    if 'data/location_with_tolls.txt' and 'data/location_without_tolls.txt' not in files:
        save_traces(city_list)
    else:
        is_location_in_file(city_list)


@app.route('/map-optymize', methods=['POST'])
def get_trace_data():
    try:
        req_data = request.get_json()
        start_point = req_data["start_point"]
        end_point = req_data["end_point"]
        cities = req_data["cities"]
        avoid_tolls = req_data["avoid_tolls"]
    except Exception as e:
        print(e)
        return abort(400, description="Missing required parameters")

    if len(cities) > 8:
        return abort(400, description="Too many cities in request")

    if start_point in cities:
        cities.remove(start_point)
    if end_point in cities:
        cities.remove(end_point)

    cities = list(set(cities))
    cities.insert(0, str(start_point))
    cities.append(end_point)

    detect_location_files(cities)

    min_time, min_km = find_min_route(cities, load_locations_from_file(avoid_tolls=avoid_tolls))

    return json.dumps(
        {"minimum km": {"trace": min_km[0], "trace_time": str(timedelta(seconds=min_km[1])), "trace_length": min_km[2]},
         "minimum time": {"trace": min_time[0], "trace_time": str(timedelta(seconds=min_time[1])), "trace_length": min_time[2]}},
        ensure_ascii = False), 200, {'ContentType': 'application/json'}

if __name__ == '__main__':
    app.run(debug=True)
