import functools
import json
import logging

import tornado.ioloop
from tornado.concurrent import return_future
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.httputil import url_concat

from gdax_async.auth import GdaxAuth


class AsyncGdaxClient:

	def __init__(self, io_loop: tornado.ioloop, gdax_auth: GdaxAuth, api_url):
		self.io_loop = io_loop
		self.gdax_auth = gdax_auth
		self.http_client = AsyncHTTPClient(io_loop=io_loop)
		self.api_url = api_url

	@return_future
	def request(self, endpoint, args=None, callback=None, method="GET", headers=None, body=None, **kwargs):
		path_url = endpoint
		if args:
			path_url = url_concat(path_url, args)
		full_url = '{}{}'.format(self.api_url, path_url)
		logging.info('Request: method={} path_url={} body={}'.format(method, path_url, body))
		auth_headers = self.gdax_auth.get_headers(method=method, path_url=path_url, body=body)
		if headers:
			auth_headers.update(headers)
		request = HTTPRequest(url=full_url, method=method, headers=auth_headers, body=body, **kwargs)
		self.http_client.fetch(request, functools.partial(self.handle_response, callback))

	def handle_response(self, callback, response):
		headers = response.headers
		try:
			text = response.body.decode('utf-8')
			data = json.loads(text)
		except Exception as e:
			logging.error('Unable to parse response as json: %s', response)
			data = None
		if callback:
			callback({'data': data, 'headers': headers})
