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

TRAILER_LIFETIME_DAYS = 365  # 1 an

youtube = build("youtube", "v3", developerKey=API_KEY)


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
            # compat si ancien state.json
            if "movies" not in state:
                state["movies"] = {}
            if "next_index" not in state:
                state["next_index"] = 0
            return state
    return {"movies": {}, "next_index": 0}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def movie_key(title, year, release):
    return f"{str(title).strip().lower()}__{year}__{release}"


def classify_period(date_str):
    year = int(date_str[:4])
    if year in (2020, 2021):
        return "covid"
    if year >= 2022:
        return "post_covid"
    return None


def ensure_trailer_not_found_file():
    # crée le fichier vide si absent (avec colonnes)
    if not os.path.exists(TRAILER_NOT_FOUND_FILE):
        df = pd.DataFrame(columns=["TITRE Français", "ANNEE", "DATE DE SORTIE FR", "last_checked"])
        df.to_csv(TRAILER_NOT_FOUND_FILE, index=False)


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

            # since = dernière date sauvegardée → on ne reprend pas les anciens
            if since and published <= since:
                continue

            period = classify_period(published)
            if not period:
                continue

            comments.append({
                "video_id": video_id,
                "author": s.get("authorDisplayName", ""),
                "text": s.get("textDisplay", ""),
                "published_at": published,
                "period": period
            })

        req = youtube.commentThreads().list_next(req, res)

    return comments


def main():
    if not API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY manquant. Ajoute-le dans GitHub Secrets ou dans ton env local.")

    movies = pd.read_csv(MOVIES_FILE)
    movies = movies.reset_index(drop=True)

    state = load_state()
    ensure_trailer_not_found_file()

    trailer_not_found_df = pd.read_csv(TRAILER_NOT_FOUND_FILE)

    processed = 0
    new_comments = []
    still_to_check = []

    total = len(movies)
    start_index = int(state.get("next_index", 0))

    # Si on arrive à la fin, on recommence au début
    if start_index >= total:
        start_index = 0

    i = start_index

    # on boucle en "cercle" pour trouver MAX_MOVIES_PER_RUN films traitables
    # sans rester bloqué si certains sont "finished"
    visited = 0
    while processed < MAX_MOVIES_PER_RUN and visited < total:
        m = movies.iloc[i]
        visited += 1

        title = m["TITRE Français"]
        year = m["ANNEE"]
        release = str(m["DATE DE SORTIE FR"])
        key = movie_key(title, year, release)

        movie_state = state["movies"].get(key, {})

        # ✅ si déjà terminé → skip
        if movie_state.get("finished") is True:
            i = (i + 1) % total
            continue

        # ✅ si pas encore de trailer → on cherche
        if "video_id" not in movie_state:
            trailer = search_trailer(title)

            if not trailer:
                still_to_check.append({
                    "TITRE Français": title,
                    "ANNEE": year,
                    "DATE DE SORTIE FR": release,
                    "last_checked": datetime.utcnow().strftime("%Y-%m-%d")
                })
                # même si trailer absent, on considère ce film comme "traité" dans le batch
                processed += 1
                i = (i + 1) % total
                continue

            movie_state["video_id"] = trailer["video_id"]
            movie_state["trailer_published_at"] = trailer["published_at"]
            movie_state["last_comment_date"] = None
            movie_state["finished"] = False

        # ✅ règle 1 an : au-delà → finished
        trailer_date = datetime.fromisoformat(
            movie_state["trailer_published_at"].replace("Z", "")
        )
        if datetime.utcnow() - trailer_date > timedelta(days=TRAILER_LIFETIME_DAYS):
            movie_state["finished"] = True
            state["movies"][key] = movie_state
            processed += 1
            i = (i + 1) % total
            continue

        # ✅ récupère nouveaux commentaires
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
        i = (i + 1) % total

    # ✅ on mémorise où reprendre la prochaine fois
    state["next_index"] = i

    # ✅ écrit les nouveaux commentaires
    if new_comments:
        pd.DataFrame(new_comments).to_csv(
            COMMENTS_FILE,
            mode="a",
            index=False,
            header=not os.path.exists(COMMENTS_FILE)
        )

    # ✅ met à jour to_check_trailer.csv (trailer non trouvé)
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
    print(f"✅ Done | processed={processed} | next_index={state['next_index']}")


if __name__ == "__main__":
    main()
