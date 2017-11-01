import base64
import hashlib
import hmac
import time


class GdaxAuth:
    # Provided by gdax: https://docs.gdax.com/#signing-a-message
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def get_signature(self, method, path_url, body=None):
        timestamp = str(round(time.time()))
        message = '{}{}{}{}'.format(timestamp, method, path_url, body if body else '')
        message = message.encode('ascii')
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest())
        return signature_b64, timestamp

    def get_ws_headers(self, method, path_url, body=None):
        signature_b64, timestamp = self.get_signature(method=method, path_url=path_url, body=body)
        headers = {
            'signature': signature_b64,
            'timestamp': timestamp,
            'key': self.api_key,
            'passphrase': self.passphrase
        }
        return headers

    def get_headers(self, method, path_url, body=None):
        signature_b64, timestamp = self.get_signature(method=method, path_url=path_url, body=body)
        headers = {
            'User-Agent': 'GdaxPythonClient/0.1',
            'Content-Type': 'Application/JSON',
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase
        }
        return headers