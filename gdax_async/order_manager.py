import logging
import collections
import datetime
import json
import pprint
from decimal import Decimal
from tornado.concurrent import return_future
from gdax_async.auth import GdaxAuth
from gdax_async.client import AsyncGdaxClient
from gdax_async.order_book import OrderBook, Order, Side


class PositionManager:
	def __init__(self, gdax_auth: GdaxAuth, async_gdax_client: AsyncGdaxClient):
		self.gdax_auth = gdax_auth
		self.async_gdax_client = async_gdax_client

		self.profile_id = None
		self.user_id = None
		self.status = None
		self.accounts = None

	def example(self):
		result = yield self.async_gdax_client.request(endpoint='/fills')
		logging.info('Fills Response: {}'.format(result))

		params = {
			#"client_oid": "",
			"size": "0.01",
			"price": "100",
			"side": "buy",
			"product_id": "ETH-USD",
			"cancel_after": "min",
			"time_in_force": "GTT",
			"post_only": "True"
		}
		result = yield self.async_gdax_client.request(endpoint='/orders', method='POST', body=json.dumps(params))
		logging.info('Place Order Response: {}'.format(result))

	def fetch_positions(self):
		self.async_gdax_client.request(endpoint='/position', callback=self.on_positions)

	def on_positions(self, result):
		data = result['data']
		headers = result['headers']
		logging.info('Positions Response:\n{}'.format(pprint.pformat(data)))
		self.profile_id = data['profile_id']
		self.accounts = data['accounts']
		self.status = data['status']
		self.user_id = data['user_id']


class OrderManager:
	def __init__(self, gdax_auth: GdaxAuth, async_gdax_client: AsyncGdaxClient):
		self.gdax_auth = gdax_auth
		self.async_gdax_client = async_gdax_client

		self.order_id_to_open_order = collections.OrderedDict()
		self.order_id_to_pending_order = collections.OrderedDict()
		self.order_id_to_cancelled_order = collections.OrderedDict()
		self.order_id_to_pending_cancel = collections.OrderedDict()

		self.order_id_to_fills = collections.OrderedDict()
		self.trade_id_to_fill = collections.OrderedDict()

	def fetch_orders(self, before=None, after=None):
		args = {}
		if before:
			args['before'] = before
		if after:
			args['after'] = after
		self.async_gdax_client.request(endpoint='/orders', args=args if args else None, callback=self.on_orders)

	def on_orders(self, result):
		data = result['data']
		headers = result['headers']
		cb_before = headers.get('Cb-Before')
		cb_after = headers.get('Cb-After')
		requesting_after = False
		if len(data) > 99 and cb_after:
			requesting_after = True
			self.fetch_orders(after=cb_after)

		logging.info('Orders Response: len={} before={} after={} requesting_after={}'.format(len(data), cb_before, cb_after, requesting_after))
		for message in data:
			order_id = message['id']
			status = message['status']
			tif = message.get('time_in_force')
			filled_size = Decimal(message.get('filled_size'))
			size = Decimal(message.get('size'))
			if filled_size and size:
				size -= filled_size
			price = Decimal(message.get('price'))
			time = datetime.datetime.strptime(message['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
			order = Order(
				sequence=None,
				order_id=order_id,
				product_id=message['product_id'],
				time=time,
				order_type=message['type'],
				side=Side.BUY if message['side'] == 'buy' else Side.SELL,
				price=price,
				size=size
			)
			order_item = dict(
					order=order,
					tif=tif,
					status=status
				)
			if status == 'open':
				self.order_id_to_open_order[order_id] = order_item
			elif status == 'pending':
				self.order_id_to_open_order[order_id] = order_item
			logging.info('Order status: {} {} {} {} {}'.format(order, status, tif, order.order_id, order.time))

	def fetch_fills(self, before=None, after=None):
		args = {}
		if before:
			args['before'] = before
		if after:
			args['after'] = after
		self.async_gdax_client.request(endpoint='/fills', args=args if args else None, callback=self.on_fills)

	def on_fills(self, result):
		data = result['data']
		headers = result['headers']
		cb_before = headers.get('Cb-Before')
		cb_after = headers.get('Cb-After')
		requesting_after = False
		if len(data) > 99 and cb_after:
			requesting_after = True
			self.fetch_fills(after=cb_after)
		logging.info('Fills Response: len={} before={} after={} requesting_after={}\n{}'.format(len(data), cb_before, cb_after, requesting_after, pprint.pformat(data)))

		'''
		{
{'created_at': '2017-06-30T21:28:24.148Z',
  'fee': '0.0000000000000000',
  'liquidity': 'M',
  'order_id': '94a94155-6410-4530-8747-69eaaa5f95b0',
  'price': '282.38000000',
  'product_id': 'ETH-USD',
  'profile_id': '851ec5ad-a415-4c09-ab97-c6f972de4289',
  'settled': True,
  'side': 'buy',
  'size': '0.50000000',
  'trade_id': 6916763,
  'user_id': '52a5f0bc9553afddc7000088'},
		}'''
		for message in data:
			order_id = message['order_id']
			trade_id = message['trade_id']
			fills = self.order_id_to_fills.setdefault(order_id, dict())
			fills[trade_id] = message
			self.trade_id_to_fill[trade_id] = message


	def place_order(self):
		params = {
			#"client_oid": "",
			"size": "0.01",
			"price": "100",
			"side": "buy",
			"product_id": "ETH-USD",
			"cancel_after": "min",
			"time_in_force": "GTT",
			"post_only": "True"
		}
		result = yield self.async_gdax_client.request(endpoint='/orders', method='POST', body=json.dumps(params))
		logging.info('Place Order Response: {}'.format(result))