
import os, sys
import cv2
import imutils
from imgbb.client import Client
import base64, json
import numpy as np
from configparser import ConfigParser
import requests

# pip install git+https://github.com/extr3mis/imgbb.git
class LINEBOT:
    def __init__(self):
        cfg = ConfigParser()
        cfg.read("config.ini", encoding="utf-8")
        self.token = cfg.get('line', 'token')
        self.imgbb_key = cfg.get('line', 'imgbb_key')
        self.imgbb_upload_url = cfg.get('line', 'imgbb_url')

    def upload_img(self, img_path, expire_mins):
        with open(img_path, "rb") as file:
            url = self.imgbb_upload_url
            payload = {
                "key": self.imgbb_key,
                "image": base64.b64encode(file.read()),
                "expiration ": expire_mins*60
            }
            res = requests.post(url, payload)
            json_o = json.loads(res.content.decode("utf-8"))
            URL = json_o['data']['url']
            print("uploaded img", json_o)

        return URL

    def line_notify(self, msg, img_path=None):
        img_url = None
        if img_path is not None:
            try:
                img_url = self.upload_img(img_path, expire_mins=24*60)
            except:
                print('error upload img to Imgbb')
                pass


        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type" : "application/x-www-form-urlencoded"
            #"Content-Type": "multipart/form-data"
        }

        if img_url is not None:
            payload = {'message': msg,  'imageThumbnail':img_url, 'imageFullsize':img_url }
        else:
            payload = {'message': msg }
        #file  = { 'imageFile':open('/home/digits/works/Netprob_grep/room.jpg','rb') }
        #file  = { 'imageFile':'@/home/digits/works/Netprob_grep/room.jpg' }
        r = requests.post("https://notify-api.line.me/api/notify", headers=headers, params=payload, timeout=10)
        print("Line send status", r.status_code)

        return r.status_code

    def callWebLine(self, eid, title, msg):
        url = "https://notify.sunplusit.com/api/notesnotify"
        data= {
            'EmpNo': eid, 
            'DeptNo': '',
            'Title': title,
            'Message': msg,
            'Image': ''
        }

        try:
            r = requests.post(url, data=data, timeout=2.0).text
            print("Call to webserver!")
            return True

        except requests.Timeout:
            print('web requests.Timeout')
            return False

        except requests.ConnectionError:
            print('web requests.ConnectionError')
            return False

        except requests.exceptions.RequestException as e:
            print('web requests.exceptions.RequestException')
            return False
