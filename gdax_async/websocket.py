import datetime
import json
import logging
import time

import tornado.httpclient
from tornado import gen
from tornado.websocket import websocket_connect


class DispatcherWSClient:
	def __init__(self, url, io_loop, reconnect_timeout=0):
		self.url = url
		self.io_loop = io_loop
		self.reconnect_timeout = reconnect_timeout
		self.ws = None
		self.last_heartbeat = None

		self.callbacks = dict()

	def register_callback(self, name, callback):
		if name in self.callbacks:
			raise Exception('Callback with name "{}" already registered'.format(name))
		self.callbacks[name] = callback

	@gen.coroutine
	def connect(self, callback=None):
		logging.debug('Connecting to ws: %s', self.url)
		if self.ws:
			self.ws.close()
		request = tornado.httpclient.HTTPRequest(self.url)
		self.ws = yield websocket_connect(request, io_loop=self.io_loop, on_message_callback=self.handle_websocket_message)
		if self.ws:
			logging.debug('WS connected: %s', self.url)
			if callback:
				callback(self)
			return self.ws
		else:
			logging.debug('WS connection failed: %s', self.url)
			return None

	def handle_websocket_message(self, message):
		try:
			if message is None:
				if self.reconnect_timeout > 0:
					logging.warning('Websocket {} disconnected. Attempting to reconnect in 10s.'.format(self.url))
					self.io_loop.add_timeout(time.time()+10, self.connect)
					return
				else:
					logging.warning('Websocket {} disconnected.'.format(self.url))
			elif message == 'HEARTBEAT':
				self.last_heartbeat = datetime.datetime.now()
			else:
				try:
					data = json.loads(message)
				except:
					data = message
				for name, callback in self.callbacks.items():
					callback(data, self)
				#logging.debug('WS data: %s', data)
		except Exception as e:
			logging.exception('Cannot parse incoming websocket message with error: %s: \n%s', e, message)
			return

	def close(self):
		if self.ws:
			self.ws.close()
			# self.ws = None


class QuickWSClient:
	def __init__(self, url, io_loop, reconnect_timeout=0):
		self.url = url
		self.io_loop = io_loop
		self.reconnect_timeout = reconnect_timeout
		self.ws = None
		self.last_heartbeat = None

		self.callback = None

	def register_callback(self, callback):
		self.callback = callback

	@gen.coroutine
	def connect(self, callback=None):
		logging.debug('Connecting to ws: %s', self.url)
		if self.ws:
			self.ws.close()
		request = tornado.httpclient.HTTPRequest(self.url)
		self.ws = yield websocket_connect(request, io_loop=self.io_loop, on_message_callback=self.handle_websocket_message)
		if self.ws:
			logging.debug('WS connected: %s', self.url)
			if callback:
				callback(self)
			return self.ws
		else:
			logging.debug('WS connection failed: %s', self.url)
			return None

	def handle_websocket_message(self, message):
		try:
			data = json.loads(message)
			self.callback(data, self)
		except Exception as e:
			if message is None:
				if self.reconnect_timeout > 0:
					logging.warning('Websocket {} disconnected. Attempting to reconnect in 10s.'.format(self.url))
					self.io_loop.add_timeout(time.time()+10, self.connect)
					return
				else:
					logging.warning('Websocket {} disconnected.'.format(self.url))
			elif message == 'HEARTBEAT':
				self.last_heartbeat = datetime.datetime.now()
			else:
				logging.exception('Cannot parse incoming websocket message with error: %s: \n%s', e, message)
				return

	def close(self):
		if self.ws:
			self.ws.close()
			# self.ws = None