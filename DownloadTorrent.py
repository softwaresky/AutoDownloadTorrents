import os
import re
import platform
import threading
import time
import datetime
import rarbgapi
import pprint
import json
from clutch import core
from PTN.parse import PTN

def read_json(file_path=""):

    if file_path and os.path.exists(file_path):
        with open(file_path, "r") as file_:
            return json.loads(file_.read())
    return {}

CONFIG = read_json("config.json")
CACHE_FILE = ".downloaded"

def get_csv_rows(file_path="", delimiter=","):
    lst_result = []

    if file_path and os.path.exists(file_path):
        with open(file_path, "r") as file_:
            dict_index = {}
            for line_ in file_.readlines():
                line_ = f"{line_}".strip("\n").strip("\l").strip("\r")

                if line_:
                    lst_line_split = f"{line_}".split(delimiter)
                    if not dict_index:
                        for i in range(len(lst_line_split)):
                            dict_index[i] = lst_line_split[i]
                    else:
                        dict_data = {}
                        for i in range(len(lst_line_split)):
                            dict_data[dict_index[i]] = lst_line_split[i]

                        if dict_data:
                            lst_result.append(dict_data)

    return lst_result


def get_download_dir(search_name="", relative_dir="", *args, **kwargs):

    result = ""

    if search_name:
        lst_search_name_split = re.split("[._ ]+", str(search_name).lower())

        share_drive_dir =  ""
        if platform.system() == "Windows":
            share_drive_dir = CONFIG["share_drive"]["win_path"]
        else:
            share_drive_dir = CONFIG["share_drive"]["linux_path"]


        if share_drive_dir and os.path.exists(share_drive_dir):

            download_dir = os.path.join(share_drive_dir, relative_dir)
            download_dir = os.path.abspath(download_dir)

            for parent_dir_, dirs_, files_ in os.walk(download_dir):
                this_dir = parent_dir_
                found = False
                for dir_ in dirs_:
                    lst_dir_split = re.split("[._ ]+", str(dir_).lower())
                    lst_inter = list(set(lst_search_name_split) & set(lst_dir_split))
                    if lst_inter and len(lst_inter) == len(lst_search_name_split):
                        found = True
                        this_dir = os.path.join(parent_dir_, dir_)
                        break

                if found:
                    result = this_dir

            if not result:
                folder_name = ".".join([str(part).title() for part in lst_search_name_split])
                result = os.path.join(download_dir, folder_name)
                if not os.path.exists(result):
                    # os.mkdir(result)
                    print ("Created: {0}".format(result))

    return result

def get_shared_items(d1={}, d2={}):
    return {k: d1[k] for k in d1 if k in d2 and d1[k] == d2[k]}

def get_torrents(search_string="", download_dir="", converter_name="", *args, **kwargs):

    dict_torrents = {}

    # RarBg
    rarbg_client = rarbgapi.RarbgAPI()

    search_string_parser = PTN()
    dict_search_string_values = search_string_parser.parse(search_string)

    # category = [rarbg_client.CATEGORY_TV_EPISODES_UHD, rarbg_client.CATEGORY_TV_EPISODES_HD, rarbg_client.CATEGORY_TV_EPISODES]
    torrent_parser = PTN()

    dict_group_torrents = {}

    # Group torrents by season, episode
    lst_torrent = rarbg_client.search(search_string=search_string, limit=100)
    if lst_torrent:
        for torrent_ in sorted(lst_torrent, key=lambda item: item.filename):

            dict_torrent_values = torrent_parser.parse(name=torrent_.filename)
            shared_items = get_shared_items(dict_search_string_values, dict_torrent_values)
            dict_torrent_values.update(torrent_._raw)

            state_keys = len(shared_items) == len(dict_search_string_values)
            state_name = str(torrent_.filename).lower().startswith(search_string.lower().replace(" ", "."))

            if not (state_keys or state_name and "resolution" in dict_torrent_values):
                continue

            dict_torrent_values["download_dir"] = download_dir

            is_combined = False
            key = None
            if "tv" in dict_torrent_values["category"].lower():
                key = (dict_torrent_values['title'], "tv")
                is_combined = True
            elif 'season' in dict_torrent_values and 'episode' in dict_torrent_values:
                key = (dict_torrent_values['title'], dict_torrent_values['season'], dict_torrent_values['episode'])
            elif "movies" in "{0}".format(dict_torrent_values["category"].lower()).split("/"):
                key = (dict_torrent_values['title'], "movies")
                is_combined = True

            if key:
                dict_torrent_values["key_name"] = key

                if key not in dict_group_torrents:
                    dict_group_torrents[key] = []
                dict_group_torrents[key].append(dict_torrent_values)

            if is_combined:
                break


    # Get the best resolution and converter name by every key (season, episode)
    for key_name_, lst_data_ in dict_group_torrents.items():
        lst_data_.sort(key=lambda item: item["resolution"] if "resolution" in item else item["filename"])

        dict_torrent = lst_data_[0]

        if converter_name:
            for dict_torrent_ in lst_data_:

                converter_name_value_ = None
                if "excess" in dict_torrent_:
                    converter_name_value_ = dict_torrent_["excess"]

                if "group" in dict_torrent_:
                    converter_name_value_ = dict_torrent_["group"]

                if isinstance(converter_name_value_, str):
                    converter_name_value_ = [converter_name_value_]

                found = False
                for conv_name_value_ in converter_name_value_:
                    if str(conv_name_value_).endswith(converter_name):
                        found = True
                        break
                if found:
                    dict_torrent = dict_torrent_
                    break

        dict_torrents[key_name_] = dict_torrent

    return dict_torrents

def add_torrents(tc_client=None, lst_torrent=[]):

    if tc_client and lst_torrent:
        print("Add into torrent transmitter...")

        # Add into torrent transmission
        rbr = 0
        for dict_torrent_ in lst_torrent:
            # priority
            dict_data = {}
            dict_data["filename"] = dict_torrent_["download"]
            dict_data["download-dir"] = dict_torrent_["download_dir"]
            dict_data['priority'] = "low"
            if rbr <= 2:
                dict_data['priority'] = "high"
            elif rbr <= 4:
                dict_data['priority'] = "normal"

            print (f"Add {dict_data}")
            # tc_client.torrent.add(**dict_data)

            rbr += 1

def auto_download():


    tc_client = core.Client(address=CONFIG["url"], username='admin', password='openmediavault')
    dt_end = datetime.datetime.now()

    while True:

        if datetime.datetime.now() >= dt_end:

            lst_all_downloaded = []
            if os.path.exists(CACHE_FILE):
                with open(CACHE_FILE, "r") as file_:
                    lst_all_downloaded = [tuple(item_) for item_ in json.loads(file_.read())]

            for dict_data_ in get_csv_rows("input.csv"):

                download_dir = get_download_dir(**dict_data_)

                converter_name = dict_data_["converter_name"] if "converter_name" in dict_data_ else ""

                dict_torrents = get_torrents(search_string=dict_data_["search_name"], download_dir=download_dir, converter_name=converter_name)

                lst_diff = list(set(dict_torrents.keys()) - set(lst_all_downloaded))

                add_torrents(tc_client=tc_client, lst_torrent=[dict_torrents[key] for key in lst_diff])
                lst_all_downloaded += lst_diff

            with open(CACHE_FILE, "w+", encoding='utf-8') as file_:
                json.dump(lst_all_downloaded, file_,  ensure_ascii=False, indent=4)

            dt_end += datetime.timedelta(minutes=15)
        else:
            print (f"Next round: {dt_end}")

        time.sleep(25)


def main():

    thread_auto_dl = threading.Thread(target=auto_download)
    thread_auto_dl.start()

if __name__ == '__main__':
    main()