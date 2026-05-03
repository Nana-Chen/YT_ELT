import json
import os
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv

from airflow.models import Variable
from airflow.decorators import task

load_dotenv(dotenv_path="./.env")


MAX_RESULTS = 50
REQUEST_TIMEOUT_SECONDS = 30


def _get_required_config(name):
    value = os.getenv(name) or Variable.get(name, default_var=None)
    if not value:
        raise ValueError(f"Missing required configuration: {name}")
    return value

@task
def get_playlist_id():
    try:
        api_key = _get_required_config("API_KEY")
        channel_handle = _get_required_config("CHANNEL_HANDLE")
        url=f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

        response= requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)

        # print(response)
        response.raise_for_status()

        data= response.json()

        # print(json.dumps(data,indent=4))

        channel_items = data["items"][0]
        channel_playlisID=channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        print(channel_playlisID)
        return channel_playlisID
    except requests.exceptions.RequestException as e:
        raise e

# playlistID = get_playlist_id()

@task
def get_video_ids(playlistId):
    video_ids = []
    pageToken = None
    base_url = "https://youtube.googleapis.com/youtube/v3/playlistItems"
    api_key = _get_required_config("API_KEY")
    

    try:
        while True:
            params = {
                "part": "contentDetails",
                "playlistId": playlistId,
                "maxResults": MAX_RESULTS,
                "key": api_key
            }




            if pageToken:
                params["pageToken"] = pageToken

            response = requests.get(base_url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)

            pageToken = data.get("nextPageToken")

            if not pageToken:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e
    
@task
def batch_list(video_id_lst,batch_size):
    for video_id in range(0,len(video_id_lst),batch_size):
        yield video_id_lst[video_id:video_id+batch_size]


@task
def extract_video_data(video_ids):
    extracted_data = []
    api_key = _get_required_config("API_KEY")

    def batch_list(video_id_lst,batch_size):
        for video_id in range(0,len(video_id_lst),batch_size):
            yield video_id_lst[video_id:video_id+batch_size]
  
    try:
        for batch in batch_list(video_ids,MAX_RESULTS):
            video_ids_str =','.join(batch)

            base_url=f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={api_key}"

            response = requests.get(base_url, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            data = response.json()

            for item in data.get('items',[]):
                video_id=item['id']
                snippet=item['snippet']
                contentDetails=item['contentDetails']
                statistics=item['statistics']

                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount", None),                    
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None),
                }
                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):
    file_path = Path(f"./data/YT_data_{date.today()}.json")
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path,"w",encoding="utf_8") as json_outfile:
        json.dump(extracted_data,json_outfile,indent=4,ensure_ascii=False)
    return str(file_path)


if __name__ == "__main__":
    playlistId = get_playlist_id()
    video_ids = get_video_ids(playlistId)
    video_data=extract_video_data(video_ids)
    save_to_json(video_data)
    print(extract_video_data(video_ids))
    # video_data = extract_video_data(video_ids)
    # save_to_json(video_data)
