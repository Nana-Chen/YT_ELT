import requests
import json
from datetime import date
from airflow.models import Variable
import os
from dotenv import load_dotenv

from airflow.decorators import task

load_dotenv(dotenv_path="./.env")


API_KEY=os.getenv("API_KEY") #从env文件拿到
CHANNEL_HANDLE=Variable.get("CHANNEL_HANDLE")
maxResults = 50

@task
def get_playlist_id():
    try:
        url=f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response= requests.get(url)

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
    

    try:
        while True:
            params = {
                "part": "contentDetails",
                "playlistId": playlistId,
                "maxResults": maxResults,
                "key": API_KEY
            }




            if pageToken:
                params["pageToken"] = pageToken

            response = requests.get(base_url, params=params)
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

    def batch_list(video_id_lst,batch_size):
        for video_id in range(0,len(video_id_lst),batch_size):
            yield video_id_lst[video_id:video_id+batch_size]
  
    try:
        for batch in batch_list(video_ids,maxResults):
            video_ids_str =','.join(batch)

            base_url=f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"

            response = requests.get(base_url)
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
    file_path =f"./data/YT_data_{date.today()}.json"

    with open(file_path,"w",encoding="utf_8") as json_outfile:
        json.dump(extracted_data,json_outfile,indent=4,ensure_ascii=False)


if __name__ == "__main__":
    playlistId = get_playlist_id()
    video_ids = get_video_ids(playlistId)
    video_data=extract_video_data(video_ids)
    save_to_json(video_data)
    print(extract_video_data(video_ids))
    # video_data = extract_video_data(video_ids)
    # save_to_json(video_data)

