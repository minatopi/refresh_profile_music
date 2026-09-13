import os
import json
import time
import ssl
from datetime import datetime, timezone, timedelta

import psycopg
import paho.mqtt.client as mqtt


# ============================================================
# 設定
# ============================================================

DATABASE_URL = os.environ["DATABASE_URL"]

# ============================================================
# HiveMQ
# ============================================================

MQTT_HOST = os.environ.get(
    "MQTT_HOST",
    "broker.hivemq.com"
)

MQTT_PORT = int(
    os.environ.get(
        "MQTT_PORT",
        "8883"
    )
)

MQTT_USERNAME = os.environ.get(
    "MQTT_USERNAME",
    ""
)

MQTT_PASSWORD = os.environ.get(
    "MQTT_PASSWORD",
    ""
)

# ============================================================
# 音楽設定
# ============================================================

MUSIC_TOPIC_PREFIX = "chanpro-post/music"

# 10時間以上経過したら再保存対象
REFRESH_AFTER_HOURS = 10

# MQTT受信待ち
MQTT_WAIT_SECONDS = 30

# MQTT publish 完了待ち
MQTT_PUBLISH_TIMEOUT = 30


# ============================================================
# 時刻
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def parse_datetime(value):
    """
    Supabase JSONB内の日時をdatetimeに変換
    """

    if not value:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc)

    value = str(value)

    try:
        dt = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    except Exception:
        return None


# ============================================================
# MQTT
# ============================================================

class MQTTReceiver:

    def __init__(self):

        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5,
        )

        if MQTT_USERNAME:
            self.client.username_pw_set(
                MQTT_USERNAME,
                MQTT_PASSWORD
            )

        # TLS
        self.client.tls_set(
            cert_reqs=ssl.CERT_REQUIRED
        )

        self.client.tls_insecure_set(False)

        self.messages = {}

        self.connected = False
        self.error = None

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

    # --------------------------------------------------------
    # connect
    # --------------------------------------------------------

    def connect(self):

        print(
            f"MQTT接続: {MQTT_HOST}:{MQTT_PORT}"
        )

        self.client.connect(
            MQTT_HOST,
            MQTT_PORT,
            keepalive=60
        )

        self.client.loop_start()

        start = time.time()

        while not self.connected:

            if self.error:
                raise RuntimeError(
                    f"MQTT接続失敗: {self.error}"
                )

            if time.time() - start > 20:
                raise TimeoutError(
                    "MQTT接続タイムアウト"
                )

            time.sleep(0.1)

    # --------------------------------------------------------
    # on_connect
    # --------------------------------------------------------

    def on_connect(
        self,
        client,
        userdata,
        flags,
        reason_code,
        properties=None
    ):

        if reason_code != 0:

            self.error = (
                f"reason_code={reason_code}"
            )

            return

        print("MQTT接続成功")

        self.connected = True

    # --------------------------------------------------------
    # on_disconnect
    # --------------------------------------------------------

    def on_disconnect(
        self,
        client,
        userdata,
        disconnect_flags,
        reason_code,
        properties=None
    ):

        if reason_code != 0:

            print(
                f"MQTT切断: {reason_code}"
            )

    # --------------------------------------------------------
    # on_message
    # --------------------------------------------------------

    def on_message(
        self,
        client,
        userdata,
        message
    ):

        self.messages[
            message.topic
        ] = bytes(message.payload)

        print(
            f"MQTT受信: {message.topic} "
            f"({len(message.payload)} bytes)"
        )

    # --------------------------------------------------------
    # subscribe
    # --------------------------------------------------------

    def subscribe(self, topic):

        print(
            f"MQTT subscribe: {topic}"
        )

        result = self.client.subscribe(
            topic,
            qos=1
        )

        if result[0] != mqtt.MQTT_ERR_SUCCESS:

            raise RuntimeError(
                f"subscribe失敗: {result}"
            )

    # --------------------------------------------------------
    # wait
    # --------------------------------------------------------

    def wait_for_topics(
        self,
        topics,
        timeout
    ):

        start = time.time()

        while True:

            missing = [
                topic
                for topic in topics
                if topic not in self.messages
            ]

            if not missing:
                print(
                    "必要なMQTTデータをすべて取得しました"
                )

                return True

            if time.time() - start >= timeout:

                print(
                    "MQTT受信タイムアウト"
                )

                print(
                    f"不足データ: {len(missing)}件"
                )

                for topic in missing[:20]:
                    print(
                        f"  未取得: {topic}"
                    )

                return False

            time.sleep(0.2)

    # --------------------------------------------------------
    # publish
    # --------------------------------------------------------

    def publish_retain(
        self,
        topic,
        payload
    ):

        print(
            f"MQTT再保存: {topic} "
            f"({len(payload)} bytes)"
        )

        info = self.client.publish(
            topic,
            payload=payload,
            qos=1,
            retain=True
        )

        if info.rc != mqtt.MQTT_ERR_SUCCESS:

            raise RuntimeError(
                f"publish失敗: {topic}, rc={info.rc}"
            )

        if not info.wait_for_publish(
            timeout=MQTT_PUBLISH_TIMEOUT
        ):

            raise TimeoutError(
                f"publishタイムアウト: {topic}"
            )

    # --------------------------------------------------------
    # close
    # --------------------------------------------------------

    def close(self):

        try:
            self.client.loop_stop()
        except Exception:
            pass

        try:
            self.client.disconnect()
        except Exception:
            pass


# ============================================================
# profile_music取得
# ============================================================

def get_oldest_music():

    print("Supabaseからプロフィール音楽を取得します")

    with psycopg.connect(
        DATABASE_URL
    ) as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    id,
                    profile_music
                FROM users
                WHERE profile_music IS NOT NULL
                """
            )

            rows = cur.fetchall()

    now = now_utc()

    candidates = []

    for user_id, profile_music in rows:

        if not profile_music:
            continue

        # JSON文字列になっている場合にも対応
        if isinstance(profile_music, str):

            try:
                profile_music = json.loads(
                    profile_music
                )
            except Exception:
                continue

        if not isinstance(profile_music, dict):
            continue

        music_id = profile_music.get(
            "music_id"
        )

        if not music_id:
            continue

        last_saved_at = parse_datetime(
            profile_music.get(
                "last_saved_at"
            )
        )

        created_at = parse_datetime(
            profile_music.get(
                "created_at"
            )
        )

        # last_saved_atが無い古いデータにも対応
        base_time = (
            last_saved_at
            or created_at
        )

        if not base_time:
            print(
                f"日時不明のためスキップ: "
                f"user={user_id}, music={music_id}"
            )

            continue

        age = now - base_time

        if age >= timedelta(
            hours=REFRESH_AFTER_HOURS
        ):

            candidates.append(
                {
                    "user_id": user_id,
                    "music": profile_music,
                    "base_time": base_time,
                    "age": age,
                }
            )

    if not candidates:

        print(
            "10時間以上経過した音楽はありません"
        )

        return None

    # 一番古いもの
    candidates.sort(
        key=lambda x: x["base_time"]
    )

    oldest = candidates[0]

    print(
        "再保存対象:"
    )

    print(
        f"  user_id     = {oldest['user_id']}"
    )

    print(
        f"  music_id    = "
        f"{oldest['music'].get('music_id')}"
    )

    print(
        f"  name        = "
        f"{oldest['music'].get('name')}"
    )

    print(
        f"  last_saved  = "
        f"{oldest['base_time'].isoformat()}"
    )

    print(
        f"  age         = "
        f"{oldest['age']}"
    )

    return oldest


# ============================================================
# Supabaseのlast_saved_at更新
# ============================================================

def update_last_saved_at(
    user_id,
    music,
    saved_at
):

    updated_music = dict(music)

    updated_music[
        "last_saved_at"
    ] = saved_at.isoformat()

    with psycopg.connect(
        DATABASE_URL
    ) as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                UPDATE users
                SET profile_music = %s::jsonb
                WHERE id = %s
                """,
                (
                    json.dumps(
                        updated_music,
                        ensure_ascii=False
                    ),
                    user_id
                )
            )

        conn.commit()

    print(
        "Supabaseのlast_saved_atを更新しました"
    )


# ============================================================
# MQTTから音楽を取得
# ============================================================

def download_music(
    mqtt_client,
    user_id,
    music
):

    music_id = music.get(
        "music_id"
    )

    if not music_id:
        raise ValueError(
            "music_idがありません"
        )

    chunks = int(
        music.get(
            "chunks",
            0
        )
    )

    if chunks <= 0:
        raise ValueError(
            f"chunksが不正です: {chunks}"
        )

    topic_prefix = (
        f"{MUSIC_TOPIC_PREFIX}/"
        f"{user_id}/"
        f"{music_id}"
    )

    meta_topic = (
        f"{topic_prefix}/meta"
    )

    chunk_topics = [
        f"{topic_prefix}/chunk/{i}"
        for i in range(chunks)
    ]

    all_topics = [
        meta_topic,
        *chunk_topics
    ]

    print(
        f"MQTTから取得: "
        f"{chunks} chunks"
    )

    # --------------------------------------------------------
    # subscribe
    # --------------------------------------------------------

    for topic in all_topics:

        mqtt_client.subscribe(
            topic
        )

    # --------------------------------------------------------
    # retained message待機
    # --------------------------------------------------------

    success = mqtt_client.wait_for_topics(
        all_topics,
        MQTT_WAIT_SECONDS
    )

    if not success:

        raise RuntimeError(
            "MQTTの必要データを全て取得できませんでした"
        )

    # --------------------------------------------------------
    # meta確認
    # --------------------------------------------------------

    meta_payload = mqtt_client.messages.get(
        meta_topic
    )

    if not meta_payload:

        raise RuntimeError(
            "MQTT metaが取得できませんでした"
        )

    try:

        mqtt_meta = json.loads(
            meta_payload.decode(
                "utf-8"
            )
        )

    except Exception as e:

        raise RuntimeError(
            f"MQTT meta JSON不正: {e}"
        )

    if mqtt_meta.get("music_id") != music_id:

        raise RuntimeError(
            "MQTT metaのmusic_idが一致しません"
        )

    # --------------------------------------------------------
    # chunks確認
    # --------------------------------------------------------

    chunk_data = []

    for i in range(chunks):

        topic = (
            f"{topic_prefix}/chunk/{i}"
        )

        if topic not in mqtt_client.messages:

            raise RuntimeError(
                f"chunk {i} がありません"
            )

        data = mqtt_client.messages[
            topic
        ]

        if not data:

            raise RuntimeError(
                f"chunk {i} が空です"
            )

        chunk_data.append(
            data
        )

    total_size = sum(
        len(x)
        for x in chunk_data
    )

    expected_size = int(
        music.get(
            "size",
            total_size
        )
    )

    print(
        f"取得サイズ: {total_size} bytes"
    )

    print(
        f"想定サイズ: {expected_size} bytes"
    )

    # --------------------------------------------------------
    # サイズ確認
    # --------------------------------------------------------

    if total_size != expected_size:

        raise RuntimeError(
            "MQTTデータのサイズが一致しません"
        )

    return {
        "meta_topic": meta_topic,
        "meta_payload": meta_payload,
        "chunk_data": chunk_data,
        "chunk_topics": chunk_topics,
        "topic_prefix": topic_prefix,
    }


# ============================================================
# MQTT再保存
# ============================================================

def refresh_music(
    mqtt_client,
    data
):

    # --------------------------------------------------------
    # 重要:
    # 全データ取得成功後にのみ再保存する
    # --------------------------------------------------------

    print(
        "全データ取得成功"
    )

    print(
        "MQTT Retainを再保存します"
    )

    # --------------------------------------------------------
    # chunks再保存
    # --------------------------------------------------------

    for topic, payload in zip(
        data["chunk_topics"],
        data["chunk_data"]
    ):

        mqtt_client.publish_retain(
            topic,
            payload
        )

    # --------------------------------------------------------
    # meta再保存
    # --------------------------------------------------------

    mqtt_client.publish_retain(
        data["meta_topic"],
        data["meta_payload"]
    )

    print(
        "MQTT再保存完了"
    )


# ============================================================
# main
# ============================================================

def main():

    print("=" * 60)

    print(
        "プロフィール音楽 MQTT 再保存処理"
    )

    print("=" * 60)

    target = get_oldest_music()

    if target is None:

        print(
            "今回の再保存処理はありません"
        )

        return

    user_id = target[
        "user_id"
    ]

    music = target[
        "music"
    ]

    mqtt_client = MQTTReceiver()

    try:

        # ----------------------------------------------------
        # MQTT接続
        # ----------------------------------------------------

        mqtt_client.connect()

        # ----------------------------------------------------
        # MQTTから完全取得
        # ----------------------------------------------------

        data = download_music(
            mqtt_client,
            user_id,
            music
        )

        # ----------------------------------------------------
        # 完全取得できた場合だけ再保存
        # ----------------------------------------------------

        refresh_music(
            mqtt_client,
            data
        )

        # ----------------------------------------------------
        # MQTT再保存成功後のみDB更新
        # ----------------------------------------------------

        saved_at = now_utc()

        update_last_saved_at(
            user_id,
            music,
            saved_at
        )

        print("=" * 60)

        print(
            "再保存成功"
        )

        print(
            f"music_id: "
            f"{music.get('music_id')}"
        )

        print(
            f"last_saved_at: "
            f"{saved_at.isoformat()}"
        )

        print("=" * 60)

    except Exception as e:

        print("=" * 60)

        print(
            "再保存失敗"
        )

        print(
            f"理由: {e}"
        )

        print(
            "last_saved_atは更新しません"
        )

        print(
            "次回のGitHub Actionsで再試行します"
        )

        print("=" * 60)

        raise

    finally:

        mqtt_client.close()


# ============================================================
# 実行
# ============================================================

if __name__ == "__main__":
    main()
