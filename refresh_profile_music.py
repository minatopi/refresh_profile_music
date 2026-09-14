import os
import json
import time
import ssl
import base64
import subprocess
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

# 10時間以上経過したものを再保存
REFRESH_AFTER_HOURS = 10

# MQTT受信待機
MQTT_WAIT_SECONDS = 60

# MQTT publish待機
MQTT_PUBLISH_TIMEOUT = 30

# MQTT接続待機
MQTT_CONNECT_TIMEOUT = 20

# バックアップ保存先
BACKUP_ROOT = "backup/profile_music"

# Git自動commit
AUTO_GIT_PUSH = os.environ.get(
    "AUTO_GIT_PUSH",
    "1"
) == "1"


# ============================================================
# 共通
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def parse_datetime(value):

    if not value:
        return None

    if isinstance(value, datetime):

        dt = value

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.astimezone(
            timezone.utc
        )

    value = str(value).strip()

    if not value:
        return None

    try:

        if value.endswith("Z"):
            value = (
                value[:-1]
                + "+00:00"
            )

        dt = datetime.fromisoformat(
            value
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.astimezone(
            timezone.utc
        )

    except Exception:

        print(
            f"日時解析失敗: {value}"
        )

        return None


def format_bytes(size):

    size = int(size)

    if size < 1024:
        return f"{size} B"

    if size < 1024 * 1024:
        return (
            f"{size / 1024:.2f} KB"
        )

    return (
        f"{size / 1024 / 1024:.2f} MB"
    )


# ============================================================
# MQTT受信クラス
# ============================================================

class MQTTReceiver:

    def __init__(self):

        self.client = mqtt.Client(
            callback_api_version=(
                mqtt.CallbackAPIVersion.VERSION2
            ),
            protocol=mqtt.MQTTv5
        )

        if MQTT_USERNAME:

            self.client.username_pw_set(
                MQTT_USERNAME,
                MQTT_PASSWORD
            )

        self.client.tls_set(
            cert_reqs=ssl.CERT_REQUIRED
        )

        self.client.tls_insecure_set(
            False
        )

        self.connected = False
        self.connection_error = None
        self.messages = {}

        self.client.on_connect = (
            self.on_connect
        )

        self.client.on_message = (
            self.on_message
        )

        self.client.on_disconnect = (
            self.on_disconnect
        )

    # --------------------------------------------------------
    # connect callback
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
                f"MQTT接続失敗: "
                f"{reason_code}"
            )

            print(
                self.connection_error
            )

    # --------------------------------------------------------
    # message callback
    # --------------------------------------------------------

    def on_message(
        self,
        client,
        userdata,
        msg
    ):

        topic = msg.topic

        payload = bytes(
            msg.payload
        )

        self.messages[
            topic
        ] = payload

        print(
            f"MQTT受信: "
            f"{topic} "
            f"({len(payload)} bytes)"
        )

    # --------------------------------------------------------
    # disconnect callback
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
                f"MQTT切断: "
                f"reason_code={reason_code}"
            )

    # --------------------------------------------------------
    # connect
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
                time.monotonic()
                - start
                > MQTT_CONNECT_TIMEOUT
            ):

                raise TimeoutError(
                    "MQTT接続タイムアウト"
                )

            time.sleep(0.1)

    # --------------------------------------------------------
    # subscribe
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

        if (
            result[0]
            != mqtt.MQTT_ERR_SUCCESS
        ):

            raise RuntimeError(
                f"MQTT subscribe失敗: "
                f"{result}"
            )

    # --------------------------------------------------------
    # wait topics
    # --------------------------------------------------------

    def wait_for_topics(
        self,
        expected_topics,
        timeout
    ):

        expected = set(
            expected_topics
        )

        start = time.monotonic()

        last_report = -5

        while True:

            received = (
                expected
                & set(self.messages.keys())
            )

            count = len(received)
            total = len(expected)

            elapsed = int(
                time.monotonic()
                - start
            )

            if count >= total:

                print(
                    f"MQTT受信完了: "
                    f"{count}/{total}"
                )

                return True

            if (
                elapsed - last_report
                >= 5
            ):

                print(
                    f"MQTT受信待機中: "
                    f"{count}/{total} "
                    f"({elapsed}秒)"
                )

                last_report = elapsed

            if elapsed >= timeout:

                missing = (
                    expected
                    - set(self.messages.keys())
                )

                print(
                    "MQTT受信タイムアウト"
                )

                print(
                    f"受信: "
                    f"{count}/{total}"
                )

                print(
                    f"不足データ: "
                    f"{len(missing)}件"
                )

                for topic in sorted(
                    missing
                ):

                    print(
                        f"  未取得: {topic}"
                    )

                return False

            time.sleep(0.1)

    # --------------------------------------------------------
    # publish retain
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

        if (
            result.rc
            != mqtt.MQTT_ERR_SUCCESS
        ):

            raise RuntimeError(
                f"MQTT publish失敗: "
                f"{result.rc}"
            )

        result.wait_for_publish(
            timeout=MQTT_PUBLISH_TIMEOUT
        )

        if not result.is_published():

            raise RuntimeError(
                "MQTT publish完了を確認できませんでした"
            )

    # --------------------------------------------------------
    # close
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
# Supabaseから対象取得
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

        if isinstance(
            profile_music,
            str
        ):

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

            music = dict(
                profile_music
            )

        else:

            continue

        music_id = music.get(
            "music_id"
        )

        if not music_id:
            continue

        created_at = parse_datetime(
            music.get(
                "created_at"
            )
        )

        last_saved_at = parse_datetime(
            music.get(
                "last_saved_at"
            )
        )

        base_time = (
            last_saved_at
            or created_at
        )

        if not base_time:
            continue

        age = now - base_time

        if age >= timedelta(
            hours=REFRESH_AFTER_HOURS
        ):

            candidates.append(
                {
                    "user_id": str(
                        user_id
                    ),
                    "music": music,
                    "base_time": base_time,
                    "age": age
                }
            )

    if not candidates:

        print(
            "再保存対象はありません"
        )

        return None

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
# MQTTから取得
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

    chunks = int(
        music.get(
            "chunks",
            0
        )
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
    # ワイルドカード購読
    # --------------------------------------------------------

    wildcard_topic = (
        f"{topic_prefix}/#"
    )

    mqtt_client.subscribe(
        wildcard_topic
    )

    # --------------------------------------------------------
    # Retain受信
    # --------------------------------------------------------

    success = mqtt_client.wait_for_topics(
        expected_topics,
        MQTT_WAIT_SECONDS
    )

    if not success:

        raise RuntimeError(
            "MQTTの必要データを全て取得できませんでした"
        )

    # ========================================================
    # meta
    # ========================================================

    meta_payload = (
        mqtt_client.messages[
            meta_topic
        ]
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
        ""
    )

    print(
        "MQTT metadata:"
    )

    print(
        json.dumps(
            mqtt_meta,
            ensure_ascii=False,
            indent=2
        )
    )

    # ========================================================
    # chunk
    # ========================================================

    chunk_data = []

    total_size = 0

    for topic in chunk_topics:

        if topic not in mqtt_client.messages:

            raise RuntimeError(
                f"MQTT chunk未取得: {topic}"
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
    # サイズ情報
    # ========================================================

    expected_size = int(
        music.get(
            "size",
            0
        )
    )

    mqtt_meta_size = int(
        mqtt_meta.get(
            "size",
            0
        )
    )

    expected_chunks = int(
        music.get(
            "chunks",
            0
        )
    )

    mqtt_meta_chunks = int(
        mqtt_meta.get(
            "chunks",
            0
        )
    )

    actual_chunks = len(
        chunk_data
    )

    # ========================================================
    # 診断
    # ========================================================

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
        f"  MQTT chunk total  = "
        f"{total_size} bytes "
        f"({format_bytes(total_size)})"
    )

    print(
        f"  Supabase chunks   = "
        f"{expected_chunks}"
    )

    print(
        f"  MQTT meta chunks  = "
        f"{mqtt_meta_chunks}"
    )

    print(
        f"  Actual chunks     = "
        f"{actual_chunks}"
    )

    # ========================================================
    # サイズ不一致フラグ
    # ========================================================

    size_mismatch = (
        expected_size > 0
        and total_size != expected_size
    )

    meta_size_mismatch = (
        mqtt_meta_size > 0
        and total_size != mqtt_meta_size
    )

    chunk_mismatch = (
        expected_chunks > 0
        and actual_chunks != expected_chunks
    )

    meta_chunk_mismatch = (
        mqtt_meta_chunks > 0
        and actual_chunks != mqtt_meta_chunks
    )

    # ========================================================
    # 不一致は警告のみ
    # ========================================================

    if (
        size_mismatch
        or meta_size_mismatch
        or chunk_mismatch
        or meta_chunk_mismatch
    ):

        print(
            ""
        )

        print(
            "============================================================"
        )

        print(
            "MQTTデータにサイズまたはチャンク数の不一致があります"
        )

        if size_mismatch:

            print(
                "・Supabase size と MQTT chunk total が不一致"
            )

        if meta_size_mismatch:

            print(
                "・MQTT meta size と MQTT chunk total が不一致"
            )

        if chunk_mismatch:

            print(
                "・Supabase chunks と実際のchunk数が不一致"
            )

        if meta_chunk_mismatch:

            print(
                "・MQTT meta chunks と実際のchunk数が不一致"
            )

        print(
            ""
        )

        print(
            "取得できたMQTTデータをバックアップし、"
            "強制的に再保存します"
        )

        print(
            "============================================================"
        )

    else:

        print(
            ""
        )

        print(
            "MQTTデータサイズ確認OK"
        )

    return {
        "meta_topic": meta_topic,
        "meta_payload": meta_payload,
        "mqtt_meta": mqtt_meta,
        "chunk_topics": chunk_topics,
        "chunk_data": chunk_data,
        "total_size": total_size,
        "expected_size": expected_size,
        "mqtt_meta_size": mqtt_meta_size,
        "expected_chunks": expected_chunks,
        "mqtt_meta_chunks": mqtt_meta_chunks,
        "actual_chunks": actual_chunks,
        "size_mismatch": size_mismatch,
        "meta_size_mismatch": meta_size_mismatch,
        "chunk_mismatch": chunk_mismatch,
        "meta_chunk_mismatch": meta_chunk_mismatch
    }


# ============================================================
# バックアップ作成
# ============================================================

def create_backup(
    user_id,
    music,
    data
):

    music_id = music.get(
        "music_id"
    )

    timestamp = now_utc().strftime(
        "%Y%m%d_%H%M%S"
    )

    backup_dir = os.path.join(
        BACKUP_ROOT,
        str(user_id),
        str(music_id),
        timestamp
    )

    os.makedirs(
        backup_dir,
        exist_ok=True
    )

    print(
        ""
    )

    print(
        "============================================================"
    )

    print(
        "バックアップ作成"
    )

    print(
        f"保存先: {backup_dir}"
    )

    print(
        "============================================================"
    )

    # ========================================================
    # metadata backup
    # ========================================================

    backup_info = {
        "backup_created_at":
            now_utc().isoformat(),

        "user_id":
            str(user_id),

        "music_id":
            music_id,

        "name":
            music.get("name", ""),

        "supabase_metadata":
            music,

        "mqtt_metadata":
            data["mqtt_meta"],

        "supabase_size":
            data["expected_size"],

        "mqtt_meta_size":
            data["mqtt_meta_size"],

        "mqtt_chunk_total_size":
            data["total_size"],

        "supabase_chunks":
            data["expected_chunks"],

        "mqtt_meta_chunks":
            data["mqtt_meta_chunks"],

        "actual_chunks":
            data["actual_chunks"],

        "size_mismatch":
            data["size_mismatch"],

        "meta_size_mismatch":
            data["meta_size_mismatch"],

        "chunk_mismatch":
            data["chunk_mismatch"],

        "meta_chunk_mismatch":
            data["meta_chunk_mismatch"]
    }

    info_path = os.path.join(
        backup_dir,
        "backup_info.json"
    )

    with open(
        info_path,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            backup_info,
            f,
            ensure_ascii=False,
            indent=2
        )

    # ========================================================
    # MQTT meta backup
    # ========================================================

    meta_path = os.path.join(
        backup_dir,
        "meta.json"
    )

    with open(
        meta_path,
        "wb"
    ) as f:

        f.write(
            data["meta_payload"]
        )

    # ========================================================
    # chunk backup
    # ========================================================

    chunk_dir = os.path.join(
        backup_dir,
        "chunks"
    )

    os.makedirs(
        chunk_dir,
        exist_ok=True
    )

    for index, payload in enumerate(
        data["chunk_data"]
    ):

        path = os.path.join(
            chunk_dir,
            f"chunk_{index:04d}.bin"
        )

        with open(
            path,
            "wb"
        ) as f:

            f.write(payload)

    print(
        "バックアップ完了"
    )

    print(
        f"  metadata: {info_path}"
    )

    print(
        f"  meta    : {meta_path}"
    )

    print(
        f"  chunks  : "
        f"{len(data['chunk_data'])}"
    )

    print(
        f"  size    : "
        f"{format_bytes(data['total_size'])}"
    )

    return backup_dir


# ============================================================
# Gitへバックアップを保存
# ============================================================

def git_commit_backup(
    backup_dir,
    user_id,
    music
):

    if not AUTO_GIT_PUSH:

        print(
            "AUTO_GIT_PUSH=0 のため"
            "Git保存をスキップします"
        )

        return

    print(
        ""
    )

    print(
        "Gitへバックアップを保存します"
    )

    try:

        subprocess.run(
            [
                "git",
                "config",
                "user.name",
                "github-actions[bot]"
            ],
            check=True
        )

        subprocess.run(
            [
                "git",
                "config",
                "user.email",
                "41898282+github-actions[bot]@users.noreply.github.com"
            ],
            check=True
        )

        subprocess.run(
            [
                "git",
                "add",
                backup_dir
            ],
            check=True
        )

        result = subprocess.run(
            [
                "git",
                "status",
                "--porcelain"
            ],
            check=True,
            capture_output=True,
            text=True
        )

        if not result.stdout.strip():

            print(
                "Gitに保存する変更はありません"
            )

            return

        music_name = music.get(
            "name",
            "profile music"
        )

        commit_message = (
            "backup profile music: "
            f"{music_name}"
        )

        subprocess.run(
            [
                "git",
                "commit",
                "-m",
                commit_message
            ],
            check=True
        )

        subprocess.run(
            [
                "git",
                "push"
            ],
            check=True
        )

        print(
            "Gitへのバックアップ保存完了"
        )

    except Exception as e:

        print(
            f"Gitバックアップ保存失敗: {e}"
        )

        # MQTT再保存そのものは継続できるようにする
        print(
            "Gitバックアップ失敗ですが"
            "MQTT再保存は続行します"
        )


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

    total = len(
        chunk_topics
    )

    # ========================================================
    # chunks
    # ========================================================

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

        # ====================================================
        # MQTT接続
        # ====================================================

        mqtt_client.connect()

        # ====================================================
        # MQTTから取得
        # ====================================================

        data = download_music(
            mqtt_client,
            user_id,
            music
        )

        # ====================================================
        # 不一致なら先にバックアップ
        # ====================================================

        has_problem = (
            data["size_mismatch"]
            or data["meta_size_mismatch"]
            or data["chunk_mismatch"]
            or data["meta_chunk_mismatch"]
        )

        if has_problem:

            backup_dir = create_backup(
                user_id,
                music,
                data
            )

            # GitHub Actionsのpush
            git_commit_backup(
                backup_dir,
                user_id,
                music
            )

        # ====================================================
        # MQTT Retain強制再保存
        # ====================================================

        refresh_music(
            mqtt_client,
            data
        )

        # ====================================================
        # MQTT再保存成功
        # ====================================================

        saved_at = now_utc()

        update_last_saved_at(
            user_id,
            music,
            saved_at
        )

        # ====================================================
        # 完了
        # ====================================================

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
            f"MQTT size: "
            f"{data['total_size']} bytes"
        )

        if has_problem:

            print(
                "サイズ不一致がありましたが、"
                "バックアップ後に強制再保存しました"
            )

        else:

            print(
                "サイズ一致のため通常再保存しました"
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
