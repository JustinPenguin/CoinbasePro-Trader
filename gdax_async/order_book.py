import collections
import datetime
import json
import logging

from decimal import Decimal
from bintrees import RBTree

from gdax_async.auth import GdaxAuth
from gdax_async.client import AsyncGdaxClient
from gdax_async.websocket import QuickWSClient

from enum import Enum

class Side(Enum):
	BUY = 1
	SELL = -1


class MissingSequencesException(Exception):
	pass


class Order:
	def __init__(self, time: datetime.datetime, sequence: int, product_id: str, order_id: str, order_type: str, side: Side, price: Decimal, size: Decimal, funds: Decimal=None):
		self.time = time
		self.sequence = sequence
		self.product_id = product_id
		self.order_id = order_id
		self.order_type = order_type
		self.side = side
		self.price = price
		self.size = size
		self.funds = funds

	def to_dict(self):
		return {
			'time': self.time.isoformat(),
			'sequence': self.sequence,
			'product_id': self.product_id,
			'order_id': self.order_id,
			'order_type': self.order_type,
			'side': self.side,
			'price': self.price,
			'size': self.size,
			'funds': self.funds
		}
	
	def __repr__(self):
		if self.order_type == 'market':
			size = (self.size or 0) * self.side.value
			funds = (self.funds or 0) * self.side.value
			return 'Order {} {} {:+6.3f} {:.2f}'.format(self.product_id, self.order_type, size, funds)
		else:
			return 'Order {} {} {:+6.3f}@{:.2f}'.format(self.product_id, self.order_type, self.size*self.side.value, self.price)


class OrderBook:
	LARGE_THRESHOLD = 50000
	def __init__(self, product_id):
		self.product_id = product_id

		self._asks = RBTree()
		self._bids = RBTree()
		self._order_id_to_order = dict()
		self._order_id_to_unreflected_order = dict()

		self.pending_order_book_snapshot = False

		self._sequence = -1
		self.snapshots = collections.deque()
		self.message_queue = collections.deque()

	def set_pending_order_book_snapshot(self):
		self.pending_order_book_snapshot = True
		logging.debug('Pending order book snapshot for {}'.format(self.product_id))

	def on_order_book_snapshot(self, result):
		data = result['data']
		logging.info('Order Book Snapshot received for {} {}'.format(self.product_id, data['sequence']))

		self.snapshots.append(data)
		self.apply_order_book_snapshot(snapshot=data)

		while self.message_queue:
			message = self.message_queue.popleft()
			self.update_order_book(message)
		self.pending_order_book_snapshot = False

	def parse_snapshot_order(self, snapshot_order, sequence, side: Side) -> Order:
		return Order(
			time=None,
			sequence=sequence,
			order_id=snapshot_order[2],
			order_type='limit',
			product_id=self.product_id,
			side=side,
			price=Decimal(snapshot_order[0]),
			size=Decimal(snapshot_order[1]),
		)

	def apply_order_book_snapshot(self, snapshot):
		self._asks = RBTree()
		self._bids = RBTree()
		self._order_id_to_order = dict()
		sequence = snapshot['sequence']
		for snapshot_order in snapshot['bids']:
			self.add(self.parse_snapshot_order(snapshot_order=snapshot_order, sequence=sequence, side=Side.BUY))
		for snapshot_order in snapshot['asks']:
			self.add(self.parse_snapshot_order(snapshot_order=snapshot_order, sequence=sequence, side=Side.SELL))
		self._sequence = snapshot['sequence']

	def update_order_book(self, message):
		sequence = message['sequence']
		if sequence <= self._sequence:
			# ignore older messages (e.g. before order book initialization from getProductOrderBook)
			return
		elif sequence > self._sequence + 1:
			raise Exception('Error: {} order book missing {} messages ({} - {}).'.format(self.product_id, sequence-self._sequence, sequence, self._sequence))

		msg_type = message['type']
		if msg_type == 'open':
			time = datetime.datetime.strptime(message['time'], "%Y-%m-%dT%H:%M:%S.%fZ")
			sequence = message['sequence']
			order_id = message['order_id']
			size = Decimal(message['remaining_size'])
			unreflected_order = self._order_id_to_unreflected_order.get(order_id)
			if unreflected_order:
				#logging.debug('Opening unreflected order: order_id={}'.format(unreflected_order.order_id))
				order = unreflected_order
				order.time = time
				order.sequence = sequence
				order.size = size
				del self._order_id_to_unreflected_order[order_id]
			else:
				order = Order(
					time=time,
					sequence=sequence,
					product_id=message['product_id'],
					order_id=order_id,
					order_type='limit',
					side=Side.BUY if message['side'] == 'buy' else Side.SELL,
					price=Decimal(message['price']),
					size=size,
				)
			self.add(order=order, message=message)
		elif msg_type == 'done' and 'price' in message:
			order_id = message['order_id']
			reason = message['reason']
			price = Decimal(message['price'])
			side = Side.BUY if message['side'] == 'buy' else Side.SELL
			remaining_size = Decimal(message['remaining_size'])
			time = datetime.datetime.strptime(message['time'], "%Y-%m-%dT%H:%M:%S.%fZ")
			sequence = message['sequence']
			self.remove(order_id=order_id, time=time, sequence=sequence, side=side, price=price, remaining_size=remaining_size, reason=reason, message=message)
		elif msg_type == 'match':
			'''
			{
				"type": "match",
				"trade_id": 10,
				"sequence": 50,
				"maker_order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
				"taker_order_id": "132fb6ae-456b-4654-b4e0-d681ac05cea1",
				"time": "2014-11-07T08:19:27.028459Z",
				"product_id": "BTC-USD",
				"size": "5.23512",
				"price": "400.23",
				"side": "sell"
			}
			taker_user_id: "5844eceecf7e803e259d0365",
			user_id: "5844eceecf7e803e259d0365",
			taker_profile_id: "765d1549-9660-4be2-97d4-fa2d65fa3352",
			profile_id: "765d1549-9660-4be2-97d4-fa2d65fa3352"
			'''
			time = datetime.datetime.strptime(message['time'], "%Y-%m-%dT%H:%M:%S.%fZ")
			sequence = message['sequence']
			maker_order_id = message['maker_order_id']
			taker_order_id = message['taker_order_id']
			trade_id = message['trade_id']
			price = Decimal(message['price'])
			side = Side.BUY if message['side'] == 'buy' else Side.SELL
			size = Decimal(message['size'])
			# taker_user_id = message['taker_user_id']
			# maker_user_id = message['user_id']
			# taker_profile_id = message['taker_profile_id']
			# maker_profile_id = message['profile_id']
			# logging.debug('Match IDs {} {} {} {}'.format(maker_user_id, taker_user_id, maker_profile_id, taker_profile_id))
			self.match(time=time, sequence=sequence, trade_id=trade_id, maker_order_id=maker_order_id, taker_order_id=taker_order_id, side=side, price=price, size=size, message=message)
		elif msg_type == 'change':
			'''
			{
				"type": "change",
				"time": "2014-11-07T08:19:27.028459Z",
				"sequence": 80,
				"order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
				"product_id": "BTC-USD",
				"new_size": "5.23512",
				"old_size": "12.234412",
				"price": "400.23",
				"side": "sell"
			}
			'''
			time = datetime.datetime.strptime(message['time'], "%Y-%m-%dT%H:%M:%S.%fZ")
			sequence = message['sequence']
			order_id = message['order_id']
			price = Decimal(message['price'])
			side = Side.BUY if message['side'] == 'buy' else Side.SELL
			new_size = Decimal(message['new_size'])
			old_size = Decimal(message['old_size'])
			self.change(time=time, sequence=sequence, order_id=order_id, price=price, side=side, new_size=new_size, old_size=old_size, message=message)
		elif msg_type == 'received':
			'''
			{
				"type": "received",
				"time": "2014-11-07T08:19:27.028459Z",
				"product_id": "BTC-USD",
				"sequence": 10,
				"order_id": "d50ec984-77a8-460a-b958-66f114b0de9b",
				"size": "1.34",
				"price": "502.1",
				"side": "buy",
				"order_type": "limit"
			}
			'''
			order_type = message['order_type']
			funds = message.get('funds')
			if order_type == 'market':
				price = None
				size = Decimal(message['size']) if message.get('size') else None
				funds = Decimal(funds) if funds else None
			elif order_type == 'limit':
				size = Decimal(message['size'])
				price = Decimal(message['price'])
			else:
				size = Decimal(message.get('size'))
				price = Decimal(message.get('price'))
			order = Order(
				time=datetime.datetime.strptime(message['time'], "%Y-%m-%dT%H:%M:%S.%fZ"),
				sequence=message['sequence'],
				product_id=message['product_id'],
				order_id=message['order_id'],
				order_type=order_type,
				side=Side.BUY if message['side'] == 'buy' else Side.SELL,
				price=price,
				size=size,
				funds=funds
			)
			self.received(order=order, order_type=order_type, message=message)

		self._sequence = sequence

	def received(self, order: Order, order_type: str, message):
		self._order_id_to_unreflected_order[order.order_id] = order
		self.on_received(order=order, order_type=order_type, message=message)

	def on_received(self, order: Order, order_type: str, message):
		pass
		if order_type == 'market':
			logging.debug('Received {} {} {:+6.3f} funds={:.2f} order_id={} '.format(order_type, order.side, order.side.value*(order.size or 0), order.side.value*(order.funds or 0), order.order_id))
		else:
			logging.debug('Received {} {:+6.3f}@{:.2f} order_id={} '.format(order_type, order.size*order.side.value, order.price, order.order_id))

	def on_message(self, message):
		if self.pending_order_book_snapshot:
			self.message_queue.append(message)
		else:
			self.update_order_book(message)

	def add(self, order: Order, message=None):
		if order.side == Side.BUY:
			bids = self.get_bids(order.price)
			if bids is None:
				bids = [order]
				self.set_bids(order.price, bids)
			else:
				bids.append(order)
		else:
			asks = self.get_asks(order.price)
			if asks is None:
				asks = [order]
				self.set_asks(order.price, asks)
			else:
				asks.append(order)
		self._order_id_to_order[order.order_id] = order
		self.on_add(order=order, message=message)

	def on_add(self, order: Order, message):
		pass
		latency_seconds = None
		if order.time:
			latency = datetime.datetime.utcnow()-order.time
			latency_seconds = latency.seconds+latency.microseconds/1000000
		logging.debug('Added {:+6.3f}@{:.2f} order_id={} latency={}'.format(order.side.value*order.size, order.price, order.order_id, latency_seconds))
		if order.size*order.price > self.LARGE_THRESHOLD:
			logging.info('Large order: {:+6.3f}@{:.2f} {} order_id={}'.format(order.side.value*order.size, order.price, self.product_id, order.order_id))

	def remove(self, time: datetime.datetime, sequence: int, order_id: str, side: Side, price: Decimal, remaining_size: Decimal, reason: str, message=None):
		order = self._order_id_to_order.get(order_id)
		if not order:
			unreflected_order = self._order_id_to_unreflected_order.get(order_id)
			if unreflected_order:
				logging.debug('Removing unreflected order_id={}'.format(order_id))
				del self._order_id_to_unreflected_order[order_id]
			else:
				logging.error('Trying to remove unrecognized order_id={} side={} price={} remaining_size={} reason={}'.format(order_id, side, price, remaining_size, reason))
			return
		if side != order.side:
			logging.error('Trying to remove order with inconsistent side order_id={} done_side={} known_side={}'.format(order_id, side, order.side))
		if price != order.price:
			logging.error('Trying to remove order with inconsistent price order_id={} done_price={} known_price={}'.format(order_id, price, order.price))

		if side == Side.BUY:
			bids = self.get_bids(price)
			if bids is not None:
				bids = [o for o in bids if o.order_id != order_id]
				if len(bids) > 0:
					self.set_bids(price, bids)
				else:
					self.remove_bids(price)
		else:
			asks = self.get_asks(price)
			if asks is not None:
				asks = [o for o in asks if o.order_id != order_id]
				if len(asks) > 0:
					self.set_asks(price, asks)
				else:
					self.remove_asks(price)
		del self._order_id_to_order[order_id]
		self.on_remove(order=order, time=time, sequence=sequence, remaining_size=remaining_size, reason=reason, message=message)

	def on_remove(self, order: Order, time: datetime.datetime, sequence: int, remaining_size: Decimal, reason: str, message):
		pass
		latency_seconds = None
		if time:
			latency = datetime.datetime.utcnow()-time
			latency_seconds = latency.seconds+latency.microseconds/1000000
		logging.debug('Removed {:+6.3f}@{:.2f} {} order_id={} latency={}'.format(order.size, order.price, reason, order.order_id, latency_seconds))
		if remaining_size*order.price > self.LARGE_THRESHOLD:
			logging.info('Large cancel: {:+6.3f}@{:.2f} {} order_id={}'.format(order.side.value*order.size, order.price, self.product_id, order.order_id))

	def match(self, time: datetime.datetime, sequence: int, side: Side, price: Decimal, size: Decimal, trade_id: str, maker_order_id:str, taker_order_id:str, message=None):
		maker_order = self._order_id_to_order.get(maker_order_id)
		if maker_order:
			pass # we should receive Done to confirm removal of filled passive order
		else:
			logging.error('Unable to find maker order for trade: {} {} {}'.format(time, sequence, maker_order_id))
		taker_order = self._order_id_to_unreflected_order.get(taker_order_id) # or self._order_id_to_order.get(taker_order_id)
		if taker_order:
			if taker_order.size:
				taker_order.size -= size
				if taker_order.size <= 0:
					del self._order_id_to_unreflected_order[taker_order_id]
					logging.debug('Matched taker fully filled order_id={}'.format(taker_order.order_id))
				else:
					pass
					logging.debug('Matched taker size={} remaining size={} order_id={}'.format(size, taker_order.size, taker_order.order_id))
			else:
				del self._order_id_to_unreflected_order[taker_order_id]
				logging.debug('Taker order with no size removed order_id={}'.format(taker_order.order_id))
		else:
			logging.error('Unable to find taker order for trade: {} {} {}'.format(time, sequence, taker_order_id))
		if side == 'buy':
			bids = self.get_bids(price)
			if not bids:
				return
			assert bids[0].order_id == maker_order_id
			if bids[0].size == size:
				self.set_bids(price, bids[1:])
			else:
				bids[0].size -= size
				self.set_bids(price, bids)
		else:
			asks = self.get_asks(price)
			if not asks:
				return
			assert asks[0].order_id == maker_order_id
			if asks[0].size == size:
				self.set_asks(price, asks[1:])
			else:
				asks[0].size -= size
				self.set_asks(price, asks)

		self.on_match(message=message, time=time, sequence=sequence, side=side, price=price, size=size, trade_id=trade_id, maker_order_id=maker_order_id, taker_order_id=taker_order_id, maker_order=maker_order)

	def on_match(self, time: datetime.datetime, sequence: int, side: Side, price: Decimal, size: Decimal, trade_id: str, maker_order_id:str, taker_order_id:str, maker_order: Order, message):
		pass
		latency_seconds = None
		if time:
			latency = datetime.datetime.utcnow()-time
			latency_seconds = latency.seconds+latency.microseconds/1000000
		logging.info('Trade: {:+6.3f}@{:.2f} {} mkt: {:.2f} / {:.2f} {} maker={} taker={} latency={}'.format(side.value*-1*size, price, self.product_id, self.get_bid(), self.get_ask(), time.isoformat(), maker_order_id, taker_order_id, latency_seconds))

	def change(self, time: datetime.datetime, sequence: int, order_id: str, side: Side, price: Decimal, old_size: Decimal, new_size: Decimal, message=None):
		order = self._order_id_to_order.get(order_id)
		if not order:
			logging.error('Trying to change unrecognized order_id={} side={} price={} old_size={} old_size={}'.format(order_id, side, price, old_size, new_size))
			return
		if order.size != old_size:
			logging.error('Trying to change order but old_size={} does not match order size={} for order_id={}'.format(old_size, order.size, order_id))
		order.size = new_size
		# if side == Side.BUY:
		# 	bids = self.get_bids(price)
		# 	if bids is None or not any(o.order_id == order_id for o in bids):
		# 		return
		# 	index = map(itemgetter('order_id'), bids).index(order_id)
		# 	old_size = bids[index].size
		# 	book_order = bids[index]
		# 	bids[index].size = new_size
		# 	self.set_bids(price, bids)
		# else:
		# 	asks = self.get_asks(price)
		# 	if asks is None or not any(o.order_id == order_id for o in asks):
		# 		return
		# 	index = map(itemgetter('order_id'), asks).index(order_id)
		# 	old_size = asks[index].size
		# 	book_order = asks[index]
		# 	asks[index].size = new_size
		# 	self.set_asks(price, asks)

		# tree = self._asks if order.side == 'sell' else self._bids
		# node = tree.get(price)
		#
		# if node is None or not any(o['id'] == order['order_id'] for o in node):
		# 	return

		self.on_change(time=time, sequence=sequence, order=order, old_size=old_size, new_size=new_size, message=message)

	def on_change(self, time: datetime.datetime, sequence: int, order: Order, old_size: Decimal, new_size: Decimal, message=None):
		pass
		lookup = None
		level = self.get_asks(price=order.price) if order.side == Side.SELL else self.get_bids(price=order.price)
		for o in level:
			if o.order_id == order.order_id:
				lookup = o
		logging.debug('Change: order_id={} old_size={} new_size={} order_size={} lookup={}'.format(order.order_id, old_size, new_size, order.size, o))


	def get_ask(self):
		return self._asks.min_key()

	def get_asks(self, price):
		return self._asks.get(price)

	def remove_asks(self, price):
		self._asks.remove(price)

	def set_asks(self, price, asks):
		self._asks.insert(price, asks)

	def get_bid(self):
		return self._bids.max_key()

	def get_bids(self, price):
		return self._bids.get(price)

	def remove_bids(self, price):
		self._bids.remove(price)

	def set_bids(self, price, bids):
		self._bids.insert(price, bids)


class OrderBookManager:
	def __init__(self, gdax_auth: GdaxAuth, ws_client: QuickWSClient, async_gdax_client: AsyncGdaxClient):
		self.gdax_auth = gdax_auth
		self.ws_client = ws_client
		self.ws_client.register_callback(callback=self.on_message)
		self.async_gdax_client = async_gdax_client
		self.product_id_to_order_book = dict()

	def get_order_book(self, product_id) -> OrderBook:
		return self.product_id_to_order_book.get(product_id)

	def init_order_book(self, product_id):
		if product_id in self.product_id_to_order_book:
			raise Exception('Product {} already initialized in OrderBookManager')
		order_book = OrderBook(product_id=product_id)
		self.product_id_to_order_book[product_id] = order_book
		self.subscribe(product_id=product_id)

	def fetch_order_book_snapshot(self, order_book: OrderBook, async_gdax_client: AsyncGdaxClient):
		params = {"level": "3"}
		logging.info('Requesting Order Book Snapshot for {}'.format(order_book.product_id))
		order_book.set_pending_order_book_snapshot()
		async_gdax_client.request(endpoint='/products/{}/book'.format(order_book.product_id), method='GET', args=params, callback=order_book.on_order_book_snapshot)

	def on_message(self, message, ws_client: QuickWSClient):
		product_id = message['product_id']
		order_book = self.get_order_book(product_id)
		if order_book:
			if not order_book.snapshots and not order_book.pending_order_book_snapshot:
				self.fetch_order_book_snapshot(order_book=order_book, async_gdax_client=self.async_gdax_client)
			order_book.on_message(message=message)
		else:
			logging.warning('Received message for unregistered product {}'.format(product_id))

	def subscribe(self, product_id):
		if not self.ws_client.ws:
			raise Exception('Cannot subscribe to {} because WS is disconnected'.format(product_id))
		ws_sub_request = {
			"type": "subscribe",
			"product_ids": [
				product_id
			],
		}
		ws_sub_request.update(self.gdax_auth.get_ws_headers(method='GET', path_url='/users/self'))
		ws_sub_request['signature'] = ws_sub_request['signature'].decode('ascii')
		logging.debug('Subscribing to WS: {}'.format(ws_sub_request))
		self.ws_client.ws.write_message(json.dumps(ws_sub_request))


