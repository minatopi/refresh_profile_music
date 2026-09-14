import os
import json
import time
import ssl
from datetime import datetime, timezone, timedelta

import psycopg
from paho.mqtt import client as mqtt


# ============================================================
# 設定
# ============================================================

DATABASE_URL = os.environ["DATABASE_URL"]

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

MUSIC_TOPIC_PREFIX = "chanpro-post/music"

# この時間以上経過したデータを再保存対象にする
REFRESH_AFTER_HOURS = 10

# MQTT Retain受信待機時間
MQTT_WAIT_SECONDS = 60

# MQTT publish完了待機時間
MQTT_PUBLISH_TIMEOUT = 30

# MQTT接続待機時間
MQTT_CONNECT_TIMEOUT = 20


# ============================================================
# 共通
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def parse_datetime(value):
    """
    Supabaseから返ってくる日時文字列をdatetimeへ変換
    """

    if not value:
        return None

    if isinstance(value, datetime):
        dt = value

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    value = str(value).strip()

    if not value:
        return None

    try:
        # Z対応
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"

        dt = datetime.fromisoformat(value)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    except Exception:
        print(
            f"日時解析失敗: {value}"
        )
        return None


def format_bytes(size):
    """
    バイト数を見やすく表示
    """

    size = int(size)

    if size < 1024:
        return f"{size} B"

    if size < 1024 * 1024:
        return f"{size / 1024:.2f} KB"

    return f"{size / 1024 / 1024:.2f} MB"


# ============================================================
# MQTT受信クラス
# ============================================================

class MQTTReceiver:

    def __init__(self):

        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5
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

        self.connected = False

        self.messages = {}

        self.connection_error = None

        self.disconnect_event = False

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

    # --------------------------------------------------------
    # 接続
    # --------------------------------------------------------

    def on_connect(
        self,
        client,
        userdata,
        flags,
        reason_code,
        properties
    ):

        if reason_code == 0:
            self.connected = True
            self.connection_error = None

            print(
                "MQTT接続成功"
            )

        else:
            self.connection_error = (
                f"MQTT接続失敗: {reason_code}"
            )

            print(
                self.connection_error
            )

    # --------------------------------------------------------
    # メッセージ
    # --------------------------------------------------------

    def on_message(
        self,
        client,
        userdata,
        msg
    ):

        topic = msg.topic
        payload = bytes(msg.payload)

        # 同じtopicが複数回届いた場合も
        # 最新データを保持
        self.messages[topic] = payload

        print(
            f"MQTT受信: {topic} "
            f"({len(payload)} bytes)"
        )

    # --------------------------------------------------------
    # 切断
    # --------------------------------------------------------

    def on_disconnect(
        self,
        client,
        userdata,
        disconnect_flags,
        reason_code,
        properties
    ):

        self.connected = False

        if reason_code != 0:

            print(
                f"MQTT切断: reason_code={reason_code}"
            )

    # --------------------------------------------------------
    # 接続開始
    # --------------------------------------------------------

    def connect(self):

        print(
            f"MQTT接続: "
            f"{MQTT_HOST}:{MQTT_PORT}"
        )

        self.client.connect(
            MQTT_HOST,
            MQTT_PORT,
            keepalive=60
        )

        self.client.loop_start()

        start = time.monotonic()

        while not self.connected:

            if self.connection_error:
                raise RuntimeError(
                    self.connection_error
                )

            if (
                time.monotonic() - start
                > MQTT_CONNECT_TIMEOUT
            ):
                raise TimeoutError(
                    "MQTT接続タイムアウト"
                )

            time.sleep(0.1)

    # --------------------------------------------------------
    # Subscribe
    # --------------------------------------------------------

    def subscribe(self, topic):

        if not self.connected:
            raise RuntimeError(
                "MQTT未接続です"
            )

        print(
            f"MQTT subscribe: {topic}"
        )

        result = self.client.subscribe(
            topic,
            qos=1
        )

        if result[0] != mqtt.MQTT_ERR_SUCCESS:

            raise RuntimeError(
                "MQTT subscribe失敗: "
                f"{result}"
            )

    # --------------------------------------------------------
    # 必要topic受信待ち
    # --------------------------------------------------------

    def wait_for_topics(
        self,
        expected_topics,
        timeout
    ):

        expected_topics = set(
            expected_topics
        )

        start = time.monotonic()

        last_report = -5

        while True:

            received = (
                expected_topics
                & set(self.messages.keys())
            )

            count = len(received)
            total = len(expected_topics)

            elapsed = int(
                time.monotonic() - start
            )

            if count >= total:

                print(
                    f"MQTT受信完了: "
                    f"{count}/{total}"
                )

                return True

            if elapsed - last_report >= 5:

                print(
                    f"MQTT受信待機中: "
                    f"{count}/{total} "
                    f"({elapsed}秒)"
                )

                last_report = elapsed

            if elapsed >= timeout:

                missing = (
                    expected_topics
                    - set(self.messages.keys())
                )

                print(
                    "MQTT受信タイムアウト"
                )

                print(
                    f"受信: {count}/{total}"
                )

                print(
                    f"不足データ: "
                    f"{len(missing)}件"
                )

                for topic in sorted(missing):
                    print(
                        f"  未取得: {topic}"
                    )

                return False

            time.sleep(0.1)

    # --------------------------------------------------------
    # Retain publish
    # --------------------------------------------------------

    def publish_retain(
        self,
        topic,
        payload
    ):

        if not self.connected:
            raise RuntimeError(
                "MQTT未接続です"
            )

        print(
            f"MQTT再保存: "
            f"{topic} "
            f"({len(payload)} bytes)"
        )

        result = self.client.publish(
            topic,
            payload,
            qos=1,
            retain=True
        )

        if result.rc != mqtt.MQTT_ERR_SUCCESS:

            raise RuntimeError(
                "MQTT publish失敗: "
                f"{result.rc}"
            )

        try:

            result.wait_for_publish(
                timeout=MQTT_PUBLISH_TIMEOUT
            )

        except Exception as e:

            raise RuntimeError(
                f"MQTT publish待機失敗: {e}"
            )

        if not result.is_published():

            raise RuntimeError(
                "MQTT publish完了を確認できませんでした"
            )

    # --------------------------------------------------------
    # 終了
    # --------------------------------------------------------

    def close(self):

        try:

            if self.connected:

                self.client.disconnect()

        except Exception as e:

            print(
                f"MQTT切断時エラー: {e}"
            )

        try:

            self.client.loop_stop()

        except Exception as e:

            print(
                f"MQTT loop停止時エラー: {e}"
            )


# ============================================================
# Supabaseから最も古いプロフィール音楽を取得
# ============================================================

def get_oldest_music():

    print(
        "Supabaseからプロフィール音楽を取得します"
    )

    candidates = []

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

    for row in rows:

        user_id = row[0]
        profile_music = row[1]

        if not profile_music:
            continue

        # JSON文字列の場合
        if isinstance(profile_music, str):

            try:
                music = json.loads(
                    profile_music
                )

            except Exception:

                print(
                    f"profile_music JSON解析失敗: "
                    f"user_id={user_id}"
                )

                continue

        elif isinstance(
            profile_music,
            dict
        ):

            music = profile_music.copy()

        else:

            print(
                f"profile_music形式不正: "
                f"user_id={user_id}"
            )

            continue

        music_id = music.get(
            "music_id"
        )

        if not music_id:

            continue

        created_at = parse_datetime(
            music.get("created_at")
        )

        last_saved_at = parse_datetime(
            music.get("last_saved_at")
        )

        # last_saved_atがあればそれを基準
        # なければcreated_at
        base_time = (
            last_saved_at
            or created_at
        )

        if not base_time:

            print(
                f"日時情報なし: "
                f"user_id={user_id}, "
                f"music_id={music_id}"
            )

            continue

        age = now - base_time

        if age >= timedelta(
            hours=REFRESH_AFTER_HOURS
        ):

            candidates.append(
                {
                    "user_id": str(user_id),
                    "music": music,
                    "base_time": base_time,
                    "age": age,
                }
            )

    if not candidates:

        print(
            "再保存対象はありません"
        )

        return None

    # 最も古いもの
    candidates.sort(
        key=lambda x: x["base_time"]
    )

    target = candidates[0]

    music = target["music"]

    print(
        "再保存対象:"
    )

    print(
        f"  user_id     = "
        f"{target['user_id']}"
    )

    print(
        f"  music_id    = "
        f"{music.get('music_id')}"
    )

    print(
        f"  name        = "
        f"{music.get('name', '')}"
    )

    print(
        f"  last_saved  = "
        f"{music.get('last_saved_at')}"
    )

    print(
        f"  age         = "
        f"{target['age']}"
    )

    return target


# ============================================================
# MQTTからプロフィール音楽を取得
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

        raise RuntimeError(
            "music_idがありません"
        )

    try:

        chunks = int(
            music.get(
                "chunks",
                0
            )
        )

    except Exception:

        raise RuntimeError(
            "chunksが不正です"
        )

    if chunks <= 0:

        raise RuntimeError(
            f"chunksが不正です: {chunks}"
        )

    print(
        f"MQTTから取得: "
        f"{chunks} chunks"
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

    expected_topics = [
        meta_topic,
        *chunk_topics
    ]

    # --------------------------------------------------------
    # ワイルドカード1回だけsubscribe
    # --------------------------------------------------------

    wildcard_topic = (
        f"{topic_prefix}/#"
    )

    mqtt_client.subscribe(
        wildcard_topic
    )

    # --------------------------------------------------------
    # Retain受信待ち
    # --------------------------------------------------------

    success = mqtt_client.wait_for_topics(
        expected_topics,
        MQTT_WAIT_SECONDS
    )

    if not success:

        print(
            "必要なMQTTデータを取得できませんでした"
        )

        if meta_topic in mqtt_client.messages:

            print(
                "meta: 取得済み"
            )

        else:

            print(
                "meta: 未取得"
            )

        missing_chunks = [
            i
            for i, topic in enumerate(
                chunk_topics
            )
            if topic not in mqtt_client.messages
        ]

        if missing_chunks:

            print(
                "不足chunk:"
            )

            print(
                missing_chunks
            )

        raise RuntimeError(
            "MQTTの必要データを全て取得できませんでした"
        )

    # ========================================================
    # meta確認
    # ========================================================

    meta_payload = (
        mqtt_client.messages[
            meta_topic
        ]
    )

    print(
        ""
    )

    print(
        "MQTT metadata:"
    )

    try:

        mqtt_meta = json.loads(
            meta_payload.decode(
                "utf-8"
            )
        )

    except Exception as e:

        raise RuntimeError(
            f"MQTT meta JSON解析失敗: {e}"
        )

    print(
        json.dumps(
            mqtt_meta,
            ensure_ascii=False,
            indent=2
        )
    )

    # --------------------------------------------------------
    # music_id確認
    # --------------------------------------------------------

    mqtt_music_id = mqtt_meta.get(
        "music_id"
    )

    if (
        mqtt_music_id
        and str(mqtt_music_id)
        != str(music_id)
    ):

        raise RuntimeError(
            "MQTT metaのmusic_idが一致しません"
        )

    # ========================================================
    # Supabase metadata
    # ========================================================

    try:

        expected_size = int(
            music.get(
                "size",
                0
            )
        )

    except Exception:

        expected_size = 0

    try:

        expected_chunks = int(
            music.get(
                "chunks",
                0
            )
        )

    except Exception:

        expected_chunks = 0

    # ========================================================
    # MQTT metaのsize/chunks
    # ========================================================

    try:

        mqtt_meta_size = int(
            mqtt_meta.get(
                "size",
                0
            )
        )

    except Exception:

        mqtt_meta_size = 0

    try:

        mqtt_meta_chunks = int(
            mqtt_meta.get(
                "chunks",
                0
            )
        )

    except Exception:

        mqtt_meta_chunks = 0

    print(
        ""
    )

    print(
        "サイズ診断:"
    )

    print(
        f"  Supabase size     = "
        f"{expected_size} bytes "
        f"({format_bytes(expected_size)})"
    )

    print(
        f"  MQTT meta size    = "
        f"{mqtt_meta_size} bytes "
        f"({format_bytes(mqtt_meta_size)})"
    )

    print(
        f"  Supabase chunks   = "
        f"{expected_chunks}"
    )

    print(
        f"  MQTT meta chunks  = "
        f"{mqtt_meta_chunks}"
    )

    # ========================================================
    # chunk取得
    # ========================================================

    chunk_data = []

    total_size = 0

    for i, topic in enumerate(
        chunk_topics
    ):

        if topic not in mqtt_client.messages:

            raise RuntimeError(
                f"chunk/{i}がありません"
            )

        data = (
            mqtt_client.messages[
                topic
            ]
        )

        chunk_data.append(
            data
        )

        total_size += len(data)

    # ========================================================
    # chunk数
    # ========================================================

    actual_chunks = len(
        chunk_data
    )

    print(
        ""
    )

    print(
        f"MQTT chunk数: "
        f"{actual_chunks}"
    )

    # ========================================================
    # 実サイズ
    # ========================================================

    print(
        f"MQTT chunk合計サイズ: "
        f"{total_size} bytes "
        f"({format_bytes(total_size)})"
    )

    # ========================================================
    # 最終診断
    # ========================================================

    print(
        ""
    )

    print(
        "============================================================"
    )

    print(
        "プロフィール音楽サイズ診断"
    )

    print(
        "============================================================"
    )

    print(
        f"Supabase size     : "
        f"{expected_size}"
    )

    print(
        f"MQTT meta size    : "
        f"{mqtt_meta_size}"
    )

    print(
        f"MQTT chunks total : "
        f"{total_size}"
    )

    print(
        f"Supabase chunks   : "
        f"{expected_chunks}"
    )

    print(
        f"MQTT meta chunks  : "
        f"{mqtt_meta_chunks}"
    )

    print(
        f"Actual chunks     : "
        f"{actual_chunks}"
    )

    print(
        "============================================================"
    )

    # ========================================================
    # chunk数チェック
    # ========================================================

    if (
        expected_chunks > 0
        and actual_chunks != expected_chunks
    ):

        raise RuntimeError(
            "MQTTチャンク数が一致しません"
        )

    if (
        mqtt_meta_chunks > 0
        and actual_chunks != mqtt_meta_chunks
    ):

        raise RuntimeError(
            "MQTT metaのチャンク数と実データが一致しません"
        )

    # ========================================================
    # MQTT meta sizeチェック
    # ========================================================

    if (
        mqtt_meta_size > 0
        and total_size != mqtt_meta_size
    ):

        print(
            ""
        )

        print(
            "MQTT metaとchunkのサイズが不一致です"
        )

        print(
            f"MQTT meta size = "
            f"{mqtt_meta_size}"
        )

        print(
            f"chunk total    = "
            f"{total_size}"
        )

        raise RuntimeError(
            "MQTT metaとチャンクのサイズが一致しません"
        )

    # ========================================================
    # Supabase sizeチェック
    # ========================================================

    if (
        expected_size > 0
        and total_size != expected_size
    ):

        print(
            ""
        )

        print(
            "Supabase metadataとMQTTデータのサイズが不一致です"
        )

        print(
            f"Supabase size = "
            f"{expected_size}"
        )

        print(
            f"MQTT size     = "
            f"{total_size}"
        )

        raise RuntimeError(
            "MQTTデータのサイズが一致しません"
        )

    # ========================================================
    # 全データOK
    # ========================================================

    print(
        ""
    )

    print(
        "MQTTデータサイズ確認OK"
    )

    print(
        f"取得サイズ: "
        f"{total_size} bytes"
    )

    print(
        f"想定サイズ: "
        f"{expected_size} bytes"
    )

    return {
        "meta_topic": meta_topic,
        "meta_payload": meta_payload,
        "chunk_topics": chunk_topics,
        "chunk_data": chunk_data,
        "total_size": total_size,
    }


# ============================================================
# MQTT Retain再保存
# ============================================================

def refresh_music(
    mqtt_client,
    data
):

    print(
        ""
    )

    print(
        "============================================================"
    )

    print(
        "MQTT Retain再保存開始"
    )

    print(
        "============================================================"
    )

    chunk_topics = data[
        "chunk_topics"
    ]

    chunk_data = data[
        "chunk_data"
    ]

    meta_topic = data[
        "meta_topic"
    ]

    meta_payload = data[
        "meta_payload"
    ]

    # ========================================================
    # chunks
    # ========================================================

    total = len(
        chunk_topics
    )

    for index, (
        topic,
        payload
    ) in enumerate(
        zip(
            chunk_topics,
            chunk_data
        ),
        start=1
    ):

        print(
            f"[{index}/{total}] "
            f"chunk再保存"
        )

        mqtt_client.publish_retain(
            topic,
            payload
        )

    # ========================================================
    # meta
    # ========================================================

    print(
        "meta再保存"
    )

    mqtt_client.publish_retain(
        meta_topic,
        meta_payload
    )

    print(
        ""
    )

    print(
        "MQTT Retain再保存完了"
    )


# ============================================================
# last_saved_at更新
# ============================================================

def update_last_saved_at(
    user_id,
    music,
    saved_at
):

    new_music = dict(
        music
    )

    new_music[
        "last_saved_at"
    ] = saved_at.isoformat()

    print(
        ""
    )

    print(
        "Supabaseのlast_saved_atを更新します"
    )

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
                        new_music,
                        ensure_ascii=False
                    ),
                    user_id
                )
            )

            if cur.rowcount != 1:

                raise RuntimeError(
                    "Supabaseの更新対象が見つかりません"
                )

        conn.commit()

    print(
        f"last_saved_at更新完了: "
        f"{saved_at.isoformat()}"
    )


# ============================================================
# メイン
# ============================================================

def main():

    print(
        ""
    )

    print(
        "============================================================"
    )

    print(
        "プロフィール音楽 MQTT 再保存処理"
    )

    print(
        "============================================================"
    )

    target = get_oldest_music()

    if not target:

        print(
            ""
        )

        print(
            "処理対象なし"
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
        # MQTTから取得
        # ----------------------------------------------------

        data = download_music(
            mqtt_client,
            user_id,
            music
        )

        # ----------------------------------------------------
        # MQTT Retain再保存
        # ----------------------------------------------------

        refresh_music(
            mqtt_client,
            data
        )

        # ----------------------------------------------------
        # 全処理成功後だけlast_saved_at更新
        # ----------------------------------------------------

        saved_at = now_utc()

        update_last_saved_at(
            user_id,
            music,
            saved_at
        )

        print(
            ""
        )

        print(
            "============================================================"
        )

        print(
            "プロフィール音楽の再保存成功"
        )

        print(
            "============================================================"
        )

        print(
            f"user_id  : {user_id}"
        )

        print(
            f"music_id : {music.get('music_id')}"
        )

        print(
            f"name     : {music.get('name', '')}"
        )

        print(
            f"size     : "
            f"{data['total_size']} bytes"
        )

        print(
            f"saved_at : "
            f"{saved_at.isoformat()}"
        )

        print(
            "============================================================"
        )

    except Exception as e:

        print(
            ""
        )

        print(
            "============================================================"
        )

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

        print(
            "============================================================"
        )

        raise

    finally:

        mqtt_client.close()


# ============================================================
# 実行
# ============================================================

if __name__ == "__main__":

    main()
