import os
import json
import pandas as pd
from datetime import datetime, timedelta
from googleapiclient.discovery import build



API_KEY = os.getenv("YOUTUBE_API_KEY")
MAX_MOVIES_PER_RUN = int(os.getenv("MAX_MOVIES_PER_RUN", "5"))

MOVIES_FILE = "movies.csv"
STATE_FILE = "state.json"
COMMENTS_FILE = "comments.csv"
TRAILER_NOT_FOUND_FILE = "to_check_trailer.csv"

TRAILER_LIFETIME_DAYS = 365 

youtube = build("youtube", "v3", developerKey=API_KEY)



def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"movies": {}}

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)

def movie_key(title, year, release):
    return f"{title.lower()}__{year}__{release}"

def classify_period(date):
    year = int(date[:4])
    if year in (2020, 2021):
        return "covid"
    if year >= 2022:
        return "post_covid"
    return None



def search_trailer(title):
    query = f"{title} bande annonce officielle"
    res = youtube.search().list(
        q=query,
        part="snippet",
        type="video",
        maxResults=1
    ).execute()

    items = res.get("items", [])
    if not items:
        return None

    video = items[0]
    return {
        "video_id": video["id"]["videoId"],
        "published_at": video["snippet"]["publishedAt"]
    }

def get_comments(video_id, since=None):
    comments = []
    req = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=100,
        textFormat="plainText"
    )

    while req:
        res = req.execute()
        for item in res.get("items", []):
            s = item["snippet"]["topLevelComment"]["snippet"]
            published = s["publishedAt"]

            if since and published <= since:
                continue

            period = classify_period(published)
            if not period:
                continue

            comments.append({
                "video_id": video_id,
                "author": s["authorDisplayName"],
                "text": s["textDisplay"],
                "published_at": published,
                "period": period
            })

        req = youtube.commentThreads().list_next(req, res)

    return comments



def main():
    movies = pd.read_csv(MOVIES_FILE)
    state = load_state()
    processed = 0
    new_comments = []
    still_to_check = []

    if os.path.exists(TRAILER_NOT_FOUND_FILE):
        trailer_not_found_df = pd.read_csv(TRAILER_NOT_FOUND_FILE)
    else:
        trailer_not_found_df = pd.DataFrame()

    for _, m in movies.iterrows():
        if processed >= MAX_MOVIES_PER_RUN:
            break

        title = m["TITRE Français"]
        year = m["ANNEE"]
        release = str(m["DATE DE SORTIE FR"])
        key = movie_key(title, year, release)

        movie_state = state["movies"].get(key, {})

       

        if "video_id" not in movie_state:
            trailer = search_trailer(title)

            if not trailer:
                still_to_check.append({
                    "TITRE Français": title,
                    "ANNEE": year,
                    "DATE DE SORTIE FR": release,
                    "last_checked": datetime.now().strftime("%Y-%m-%d")
                })
                continue

            movie_state["video_id"] = trailer["video_id"]
            movie_state["trailer_published_at"] = trailer["published_at"]
            movie_state["last_comment_date"] = None
            movie_state["finished"] = False

      

        trailer_date = datetime.fromisoformat(
            movie_state["trailer_published_at"].replace("Z", "")
        )

        if datetime.utcnow() - trailer_date > timedelta(days=TRAILER_LIFETIME_DAYS):
            movie_state["finished"] = True
            state["movies"][key] = movie_state
            continue

      

        comments = get_comments(
            movie_state["video_id"],
            movie_state.get("last_comment_date")
        )

        if comments:
            latest = max(c["published_at"] for c in comments)
            movie_state["last_comment_date"] = latest

            for c in comments:
                new_comments.append({
                    "title": title,
                    "video_id": c["video_id"],
                    "period": c["period"],
                    "author": c["author"],
                    "text": c["text"],
                    "published_at": c["published_at"]
                })

        state["movies"][key] = movie_state
        processed += 1

  

    if new_comments:
        pd.DataFrame(new_comments).to_csv(
            COMMENTS_FILE,
            mode="a",
            index=False,
            header=not os.path.exists(COMMENTS_FILE)
        )

    if still_to_check:
        df_new = pd.DataFrame(still_to_check)
        df_all = pd.concat([trailer_not_found_df, df_new], ignore_index=True)
        df_all.drop_duplicates(
            subset=["TITRE Français", "ANNEE", "DATE DE SORTIE FR"],
            inplace=True
        )
        df_all.sort_values("DATE DE SORTIE FR", inplace=True)
        df_all.to_csv(TRAILER_NOT_FOUND_FILE, index=False)

    save_state(state)
    print("✅ Done")

if __name__ == "__main__":
    main()
