import csv
import os.path
import logging.config
import time

import yaml
from urllib import request

request_headers = {
    "Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.2924.87 Safari/537.36",
    "Accept-Encoding": "gzip, deflate, sdch",
    "Accept-Language": "zh-CN,zh;q=0.8,ja;q=0.6",
}

LOGGING_FILE= "./logging.yml"


def logging_config(file: str = LOGGING_FILE):
    if os.path.exists(file):
        try:
            with open(file, "rt") as f:
                config = yaml.safe_load(f)
                logging.config.dictConfig(config)
        except Exception as e:
            logging.basicConfig(level=logging.WARN)


def save(file: str, header: [], data: []):
    # save as csv file
    if len(data) == 0:
        logging.warning("can not store %s file, no data available.", file)
        return
    with open(file, "w", newline="") as f:
        writer = csv.DictWriter(f, header)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    logging.info("stored csv file: %s", file)


def request_url(url: str, method: str = 'GET',
                data: dict = None, headers=None):
    """
    利用 requests 请求网站, 爬取网站内容, 如网站链接失败, 可重复爬取 20 次
    :param url: string 网站地址
    :param encoding: string 编码类型: "utf-8", "gbk", "gb2312"
    :param method: string 访问方法: "get", "post"
    :param data: dict 上传数据: 键值对
    :param headers: dict 游览器请求头: 键值对
    :return: requests.response 爬取返回内容: response
    """
    if headers is None:
        headers = request_headers
    i = 0
    while True:
        try:
            req = request.Request(url, data=data, headers=headers, method=method)
            return request.urlopen(req)
        except:
            i += 1
            print(f"第{str(i)}次链接失败, 最多尝试 5 次")
            time.sleep(5)
            if i > 5:
                return None
