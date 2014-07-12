import requests
import json


class Email(object):
    def __init__(self):
        self.api = 'edebbb41-84c1-4145-8cce-344fe6ec0c0b'

        self.headers =  {
            "Accept" : "application/json",
            "Content-Type" : "application/json",
            "X-Postmark-Server-Token" : self.api,
        }

        self.From = "info@addxy.com"

    def send(self):
        payload = {
        "From" : self.From,
        "To" : self.To,
        "Subject" : self.Subject,
        "Tag" : "Invitation",
        "HtmlBody" : self.Body,
        "TextBody" : self.Body,
        "ReplyTo" : "0ad2f0549c701f356d88b06dd9e7883e@inbound.postmarkapp.com",
        "Headers" : [{ "Name" : "AddXY", "Value" : "value" }]
        }

        url = "http://api.postmarkapp.com/email"
        r = requests.post(url, data=json.dumps(payload), headers=self.headers)
        print r
        print r.content


if __name__ == '__main__':
    e = Email()
    e.To = 'carriere.denis@gmail.com'
    e.From = 'info@addxy.com'
    e.Subject = 'Hello World'
    e.Body = 'I am saying Hello'
    e.send()
