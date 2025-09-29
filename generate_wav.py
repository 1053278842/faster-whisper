
import os

import requests
from dotenv import load_dotenv
from rabbitmq_utils import rabbitmq_client
import json
from datetime import datetime, timedelta
import threading
import time
from faster_whisper import WhisperModel

audio_path = "audio.wav"

# 加载环境变量
load_dotenv()
# url = os.environ.get("UPLOAD_API")
# if url is None:
#     print("未找到UPLOAD_API配置")
#     raise Exception("UPLOAD_API 环境变量未配置！请参考ai-butler-api项目接口")

def download_audio(url: str, local_path: str):
    """
    下载流式音频文件（支持 Range / 大文件）。
    """
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"下载完成: {local_path}")
        stt()
        return True
    except Exception as e:
        print("下载失败:", e)
        return False

def stt():
    segments, info = model.transcribe(audio=audio_path, word_timestamps=True)
    full_text = " ".join([segment.text for segment in segments])
    print("✅ 语音转文本成功：",full_text)
    return full_text

def mq_callback(ch, method, properties, body):
    try:
        # 解析消息 JSON
        msg = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError:
        print("消息不是有效的 JSON，跳过:", body)
        return

    # 检查 type 字段
    msg_type = msg.get("type")
    if msg_type != "stt" :
        print("消息 type 不符合，跳过:", msg_type)
        return

    # 获取 question 字段并生成哈希
    url = msg.get("url")
    if not url:
        print("消息没有 url 字段，跳过")
        return
    
    # 获取 time 字段并解析
    time_str = msg.get("time")
    if not time_str:
        print("消息没有 time 字段，跳过")
        return

    try:
        sent_time = datetime.fromisoformat(time_str)
    except ValueError:
        print("time 字段格式不正确，跳过:", time_str)
        return

    # 判断是否在 1 min
    now = datetime.now()
    if now - sent_time > timedelta(minutes=1):
        print(f"消息已过期 (>1h): {url}")
        return

    # 如果都符合条件，处理消息
    print(f"消息有效 ✅")
    print(f"发送时间: {time_str}")
    print(f"消息类型: {msg_type}")
    print(f"音频地址: {url}")
    
    download_audio(url=url,local_path=audio_path)
    


if __name__ == "__main__":
    # 初始化
    model = WhisperModel("tiny", device="cuda")
    
    # 开辟新的线程 启动消费者 
    def start_consumer():
        try:
            rabbitmq_client.consume(callback=mq_callback)
        except Exception as e:
            print("消费者线程异常:", e)

    t = threading.Thread(target=start_consumer, daemon=True)
    t.start()
   
    while True:
        time.sleep(1)