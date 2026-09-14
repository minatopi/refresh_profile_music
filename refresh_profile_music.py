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
# MQTT設定
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
# ChanPro MQTT
# ============================================================

MUSIC_TOPIC_PREFIX = "chanpro-post/music"


# ============================================================
# 再保存設定
# ============================================================

# 最後に保存してから10時間以上経過したら対象
REFRESH_AFTER_HOURS = 10

# MQTT Retain受信待ち時間
MQTT_WAIT_SECONDS = 60

# MQTT Publish完了待ち
MQTT_PUBLISH_TIMEOUT = 30


# ============================================================
# 時刻
# ============================================================

def now_utc():
    """
    現在時刻をUTCで取得
    """

    return datetime.now(timezone.utc)


def parse_datetime(value):
    """
    ISO形式の日時をdatetimeへ変換する
    """

    if not value:
        return None

    if isinstance(value, datetime):

        if value.tzinfo is None:
            return value.replace(
                tzinfo=timezone.utc
            )

        return value.astimezone(
            timezone.utc
        )

    value = str(value)

    try:

        dt = datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00"
            )
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.astimezone(
            timezone.utc
        )

    except Exception:

        return None


# ============================================================
# MQTTクライアント
# ============================================================

class MQTTReceiver:

    def __init__(self):

        self.client = mqtt.Client(
            callback_api_version=(
                mqtt.CallbackAPIVersion.VERSION2
            ),
            protocol=mqtt.MQTTv5,
        )

        # ----------------------------------------------------
        # MQTT認証
        # ----------------------------------------------------

        if MQTT_USERNAME:

            self.client.username_pw_set(
                MQTT_USERNAME,
                MQTT_PASSWORD
            )

        # ----------------------------------------------------
        # TLS
        # ----------------------------------------------------

        self.client.tls_set(
            cert_reqs=ssl.CERT_REQUIRED
        )

        self.client.tls_insecure_set(
            False
        )

        # ----------------------------------------------------
        # 状態
        # ----------------------------------------------------

        self.messages = {}

        self.connected = False

        self.error = None

        # ----------------------------------------------------
        # Callback
        # ----------------------------------------------------

        self.client.on_connect = (
            self.on_connect
        )

        self.client.on_message = (
            self.on_message
        )

        self.client.on_disconnect = (
            self.on_disconnect
        )

    # ========================================================
    # MQTT接続
    # ========================================================

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

        start = time.time()

        while not self.connected:

            if self.error:

                raise RuntimeError(
                    "MQTT接続失敗: "
                    f"{self.error}"
                )

            if (
                time.time() - start
                > 20
            ):

                raise TimeoutError(
                    "MQTT接続タイムアウト"
                )

            time.sleep(
                0.1
            )

    # ========================================================
    # MQTT on_connect
    # ========================================================

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

        self.connected = True

        print(
            "MQTT接続成功"
        )

    # ========================================================
    # MQTT on_disconnect
    # ========================================================

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
                f"MQTT切断: "
                f"{reason_code}"
            )

    # ========================================================
    # MQTT on_message
    # ========================================================

    def on_message(
        self,
        client,
        userdata,
        message
    ):

        topic = message.topic

        payload = bytes(
            message.payload
        )

        self.messages[
            topic
        ] = payload

        print(
            f"MQTT受信: "
            f"{topic} "
            f"({len(payload)} bytes)"
        )

    # ========================================================
    # MQTT Subscribe
    # ========================================================

    def subscribe(
        self,
        topic
    ):

        print(
            f"MQTT subscribe: "
            f"{topic}"
        )

        result = self.client.subscribe(
            topic,
            qos=1
        )

        if (
            result[0]
            != mqtt.MQTT_ERR_SUCCESS
        ):

            raise RuntimeError(
                f"MQTT subscribe失敗: "
                f"{result}"
            )

    # ========================================================
    # 必要Topicの受信待機
    # ========================================================

    def wait_for_topics(
        self,
        topics,
        timeout
    ):

        start = time.time()

        total = len(
            topics
        )

        last_report = -1

        while True:

            received = sum(
                1
                for topic in topics
                if topic in self.messages
            )

            # ------------------------------------------------
            # 全取得完了
            # ------------------------------------------------

            if received >= total:

                print(
                    f"MQTT受信完了: "
                    f"{received}/{total}"
                )

                return True

            # ------------------------------------------------
            # 経過時間
            # ------------------------------------------------

            elapsed = (
                time.time()
                - start
            )

            # ------------------------------------------------
            # 5秒ごとに進捗表示
            # ------------------------------------------------

            current_report = int(
                elapsed // 5
            )

            if (
                current_report
                != last_report
            ):

                last_report = (
                    current_report
                )

                print(
                    f"MQTT受信待機中: "
                    f"{received}/{total} "
                    f"({int(elapsed)}秒)"
                )

            # ------------------------------------------------
            # タイムアウト
            # ------------------------------------------------

            if elapsed >= timeout:

                missing = [
                    topic
                    for topic in topics
                    if topic
                    not in self.messages
                ]

                print(
                    "MQTT受信タイムアウト"
                )

                print(
                    f"受信: "
                    f"{received}/{total}"
                )

                print(
                    f"不足データ: "
                    f"{len(missing)}件"
                )

                for topic in missing[:30]:

                    print(
                        f"  未取得: "
                        f"{topic}"
                    )

                return False

            time.sleep(
                0.2
            )

    # ========================================================
    # MQTT Retain Publish
    # ========================================================

    def publish_retain(
        self,
        topic,
        payload
    ):

        print(
            f"MQTT再保存: "
            f"{topic} "
            f"({len(payload)} bytes)"
        )

        info = self.client.publish(
            topic,
            payload=payload,
            qos=1,
            retain=True
        )

        if (
            info.rc
            != mqtt.MQTT_ERR_SUCCESS
        ):

            raise RuntimeError(
                f"MQTT publish失敗: "
                f"{topic}, "
                f"rc={info.rc}"
            )

        if not info.wait_for_publish(
            timeout=MQTT_PUBLISH_TIMEOUT
        ):

            raise TimeoutError(
                f"MQTT publishタイムアウト: "
                f"{topic}"
            )

    # ========================================================
    # MQTT切断
    # ========================================================

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
# Supabaseから一番古い音楽を取得
# ============================================================

def get_oldest_music():

    print(
        "Supabaseからプロフィール音楽を取得します"
    )

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

    # ========================================================
    # 全ユーザー確認
    # ========================================================

    for user_id, profile_music in rows:

        if not profile_music:

            continue

        # ----------------------------------------------------
        # JSON文字列の場合
        # ----------------------------------------------------

        if isinstance(
            profile_music,
            str
        ):

            try:

                profile_music = json.loads(
                    profile_music
                )

            except Exception:

                print(
                    f"profile_music JSON不正: "
                    f"user={user_id}"
                )

                continue

        # ----------------------------------------------------
        # オブジェクト確認
        # ----------------------------------------------------

        if not isinstance(
            profile_music,
            dict
        ):

            continue

        # ----------------------------------------------------
        # music_id
        # ----------------------------------------------------

        music_id = profile_music.get(
            "music_id"
        )

        if not music_id:

            continue

        # ----------------------------------------------------
        # last_saved_at
        # ----------------------------------------------------

        last_saved_at = parse_datetime(
            profile_music.get(
                "last_saved_at"
            )
        )

        # ----------------------------------------------------
        # created_at
        # ----------------------------------------------------

        created_at = parse_datetime(
            profile_music.get(
                "created_at"
            )
        )

        # ----------------------------------------------------
        # last_saved_atが無い場合
        # created_atを使用
        # ----------------------------------------------------

        base_time = (
            last_saved_at
            or created_at
        )

        if not base_time:

            print(
                f"保存日時不明のためスキップ: "
                f"user={user_id}, "
                f"music={music_id}"
            )

            continue

        # ----------------------------------------------------
        # 経過時間
        # ----------------------------------------------------

        age = (
            now - base_time
        )

        # ----------------------------------------------------
        # 10時間以上経過
        # ----------------------------------------------------

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

    # ========================================================
    # 対象なし
    # ========================================================

    if not candidates:

        print(
            "10時間以上経過した音楽はありません"
        )

        return None

    # ========================================================
    # 一番古いもの
    # ========================================================

    candidates.sort(
        key=lambda x: x["base_time"]
    )

    oldest = candidates[0]

    music = oldest[
        "music"
    ]

    print(
        "再保存対象:"
    )

    print(
        f"  user_id     = "
        f"{oldest['user_id']}"
    )

    print(
        f"  music_id    = "
        f"{music.get('music_id')}"
    )

    print(
        f"  name        = "
        f"{music.get('name')}"
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
# MQTTからプロフィール音楽を取得
# ============================================================

def download_music(
    mqtt_client,
    user_id,
    music
):

    # ========================================================
    # music_id
    # ========================================================

    music_id = music.get(
        "music_id"
    )

    if not music_id:

        raise ValueError(
            "music_idがありません"
        )

    # ========================================================
    # chunks
    # ========================================================

    try:

        chunks = int(
            music.get(
                "chunks",
                0
            )
        )

    except Exception:

        raise ValueError(
            "chunksが数値ではありません"
        )

    if chunks <= 0:

        raise ValueError(
            f"chunksが不正です: "
            f"{chunks}"
        )

    # ========================================================
    # Topic
    # ========================================================

    topic_prefix = (
        f"{MUSIC_TOPIC_PREFIX}/"
        f"{user_id}/"
        f"{music_id}"
    )

    meta_topic = (
        f"{topic_prefix}/meta"
    )

    # ========================================================
    # chunk Topic
    # ========================================================

    chunk_topics = [
        f"{topic_prefix}/chunk/{i}"
        for i in range(chunks)
    ]

    # ========================================================
    # 必要なTopic一覧
    # ========================================================

    expected_topics = [
        meta_topic,
        *chunk_topics
    ]

    # ========================================================
    # 重要
    #
    # 個別にsubscribeしない
    #
    # 音楽1件について # を1回subscribeする
    # ========================================================

    wildcard_topic = (
        f"{topic_prefix}/#"
    )

    print(
        f"MQTTから取得: "
        f"{chunks} chunks"
    )

    print(
        f"MQTT subscribe: "
        f"{wildcard_topic}"
    )

    # ========================================================
    # Subscribe
    # ========================================================

    mqtt_client.subscribe(
        wildcard_topic
    )

    # ========================================================
    # Retain受信待ち
    # ========================================================

    success = mqtt_client.wait_for_topics(
        expected_topics,
        MQTT_WAIT_SECONDS
    )

    if not success:

        # ----------------------------------------------------
        # 現在の取得状況
        # ----------------------------------------------------

        received_chunks = 0

        for i in range(chunks):

            topic = (
                f"{topic_prefix}/chunk/{i}"
            )

            if topic in mqtt_client.messages:

                received_chunks += 1

        print(
            "MQTT Retain取得状況:"
        )

        print(
            f"  meta: "
            f"{'取得済み' if meta_topic in mqtt_client.messages else '未取得'}"
        )

        print(
            f"  chunks: "
            f"{received_chunks}/{chunks}"
        )

        # ----------------------------------------------------
        # 不足chunk
        # ----------------------------------------------------

        missing_chunks = []

        for i in range(chunks):

            topic = (
                f"{topic_prefix}/chunk/{i}"
            )

            if topic not in mqtt_client.messages:

                missing_chunks.append(
                    i
                )

        if missing_chunks:

            print(
                f"  不足chunk: "
                f"{missing_chunks}"
            )

        raise RuntimeError(
            "MQTTの必要データを全て取得できませんでした"
        )

    # ========================================================
    # meta
    # ========================================================

    meta_payload = (
        mqtt_client.messages.get(
            meta_topic
        )
    )

    if not meta_payload:

        raise RuntimeError(
            "MQTT metaが取得できませんでした"
        )

    # ========================================================
    # meta JSON
    # ========================================================

    try:

        mqtt_meta = json.loads(
            meta_payload.decode(
                "utf-8"
            )
        )

    except Exception as e:

        raise RuntimeError(
            f"MQTT meta JSON不正: "
            f"{e}"
        )

    # ========================================================
    # music_id確認
    # ========================================================

    if (
        mqtt_meta.get(
            "music_id"
        )
        != music_id
    ):

        raise RuntimeError(
            "MQTT metaのmusic_idが一致しません"
        )

    # ========================================================
    # chunks
    # ========================================================

    chunk_data = []

    for i in range(chunks):

        topic = (
            f"{topic_prefix}/chunk/{i}"
        )

        if topic not in mqtt_client.messages:

            raise RuntimeError(
                f"chunk {i} がありません"
            )

        data = (
            mqtt_client.messages[
                topic
            ]
        )

        if not data:

            raise RuntimeError(
                f"chunk {i} が空です"
            )

        chunk_data.append(
            data
        )

    # ========================================================
    # サイズ確認
    # ========================================================

    total_size = sum(
        len(data)
        for data in chunk_data
    )

    try:

        expected_size = int(
            music.get(
                "size",
                total_size
            )
        )

    except Exception:

        expected_size = (
            total_size
        )

    print(
        f"取得サイズ: "
        f"{total_size} bytes"
    )

    print(
        f"想定サイズ: "
        f"{expected_size} bytes"
    )

    # ========================================================
    # サイズが違っていても処理を継続
    # ========================================================

    if total_size != expected_size:

        print(
            "⚠ MQTTデータのサイズが一致しません"
        )

        print(
            "⚠ サイズ不一致ですが、"
            "取得したデータをそのまま再保存します"
        )

    else:

        print(
            "MQTTデータサイズ確認OK"
        )

    # ========================================================
    # 取得成功
    # ========================================================

    print(
        f"MQTT取得成功: "
        f"{chunks}/{chunks} chunks"
    )

    return {
        "meta_topic": meta_topic,
        "meta_payload": meta_payload,
        "chunk_data": chunk_data,
        "chunk_topics": chunk_topics,
        "topic_prefix": topic_prefix,
    }


# ============================================================
# MQTT Retain再保存
# ============================================================

def refresh_music(
    mqtt_client,
    data
):

    print(
        "全データ取得成功"
    )

    print(
        "MQTT Retainを再保存します"
    )

    # ========================================================
    # chunk再保存
    # ========================================================

    total = len(
        data["chunk_topics"]
    )

    for index, (
        topic,
        payload
    ) in enumerate(
        zip(
            data["chunk_topics"],
            data["chunk_data"]
        ),
        start=1
    ):

        print(
            f"再保存進捗: "
            f"{index}/{total}"
        )

        mqtt_client.publish_retain(
            topic,
            payload
        )

    # ========================================================
    # meta再保存
    # ========================================================

    print(
        "metaを再保存します"
    )

    mqtt_client.publish_retain(
        data["meta_topic"],
        data["meta_payload"]
    )

    print(
        "MQTT再保存完了"
    )


# ============================================================
# Supabase last_saved_at更新
# ============================================================

def update_last_saved_at(
    user_id,
    music,
    saved_at
):

    # --------------------------------------------------------
    # 元JSONを壊さない
    # --------------------------------------------------------

    updated_music = dict(
        music
    )

    # --------------------------------------------------------
    # 最終保存時刻
    # --------------------------------------------------------

    updated_music[
        "last_saved_at"
    ] = saved_at.isoformat()

    # --------------------------------------------------------
    # Supabase更新
    # --------------------------------------------------------

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
# main
# ============================================================

def main():

    print(
        "=" * 60
    )

    print(
        "プロフィール音楽 MQTT 再保存処理"
    )

    print(
        "=" * 60
    )

    # ========================================================
    # 一番古い対象を取得
    # ========================================================

    target = get_oldest_music()

    # ========================================================
    # 対象なし
    # ========================================================

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

        # ====================================================
        # MQTT接続
        # ====================================================

        mqtt_client.connect()

        # ====================================================
        # MQTTから完全取得
        # ====================================================

        data = download_music(
            mqtt_client,
            user_id,
            music
        )

        # ====================================================
        # 完全取得できた場合のみ再保存
        # ====================================================

        refresh_music(
            mqtt_client,
            data
        )

        # ====================================================
        # MQTT再保存成功
        # ====================================================

        saved_at = now_utc()

        # ====================================================
        # MQTT再保存成功後のみ
        # last_saved_atを更新
        # ====================================================

        update_last_saved_at(
            user_id,
            music,
            saved_at
        )

        # ====================================================
        # 完了
        # ====================================================

        print(
            "=" * 60
        )

        print(
            "再保存成功"
        )

        print(
            f"user_id: "
            f"{user_id}"
        )

        print(
            f"music_id: "
            f"{music.get('music_id')}"
        )

        print(
            f"name: "
            f"{music.get('name')}"
        )

        print(
            f"last_saved_at: "
            f"{saved_at.isoformat()}"
        )

        print(
            "=" * 60
        )

    except Exception as e:

        # ====================================================
        # 失敗
        # ====================================================

        print(
            "=" * 60
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
            "=" * 60
        )

        # GitHub Actionsを失敗扱いにする
        raise

    finally:

        # ====================================================
        # MQTT切断
        # ====================================================

        mqtt_client.close()


# ============================================================
# 実行
# ============================================================

if __name__ == "__main__":

    main()
