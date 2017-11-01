import json
import logging

import tornado
import tornado.httpclient
import tornado.httpserver
import tornado.httputil
import tornado.ioloop
import tornado.log
import tornado.web
from gdax_async.auth import GdaxAuth
from gdax_async.client import AsyncGdaxClient
from gdax_async.utils import load_config, configure_logging
from gdax_async.websocket import QuickWSClient
from tornado.options import define, options, parse_command_line

from gdax_async.order_book import OrderBookManager
from gdax_async.order_manager import PositionManager, OrderManager


@tornado.gen.coroutine
def init_sequence(gdax_auth: GdaxAuth, async_gdax_client: AsyncGdaxClient, ws_client: QuickWSClient):
	order_manager = OrderManager(gdax_auth=gdax_auth, async_gdax_client=async_gdax_client)
	position_manager = PositionManager(gdax_auth=gdax_auth, async_gdax_client=async_gdax_client)

	position_manager.fetch_positions()
	order_manager.fetch_orders()
	order_manager.fetch_fills()
	order_manager.place_order()



	result = yield ws_client.connect()
	order_book_manager = OrderBookManager(ws_client=ws_client, async_gdax_client=async_gdax_client, gdax_auth=gdax_auth)
	order_book_manager.init_order_book(product_id='ETH-USD')



def run():
	define('port', type=int, default=8000)
	define('keys_config', type=str, default='config/keys.json')
	define('api_config', type=str, default='config/api.json')
	define('file_log_level', type=str, default='DEBUG')
	define('console_log_level', type=str, default='INFO')
	define('log_file', type=str, default='logs/trader')
	options.logging = None
	parse_command_line()

	configure_logging(log_file=options.log_file, file_log_level=options.file_log_level, console_log_level=options.console_log_level)
	io_loop = tornado.ioloop.IOLoop()

	keys_config = load_config(options.keys_config)
	api_config = load_config(options.api_config)

	gdax_auth = GdaxAuth(api_key=keys_config.get('api_key'), secret_key=keys_config.get('secret_key'), passphrase=keys_config.get('passphrase'))
	async_gdax_client = AsyncGdaxClient(io_loop=io_loop, gdax_auth=gdax_auth, api_url=api_config.get('api_url'))
	ws_client = QuickWSClient(io_loop=io_loop, url=api_config.get('ws_url'))

	init_sequence(gdax_auth=gdax_auth, async_gdax_client=async_gdax_client, ws_client=ws_client)

	handlers = [
		tornado.web.url(r'/static/?(.*)?', tornado.web.StaticFileHandler,
		                {'path': 'static', 'default_filename': 'index.html'}),
	]

	settings = {
		'debug': False,
	}
	applicaton = tornado.web.Application(handlers, **settings)
	applicaton.listen(options.port)

	try:
		io_loop.start()
	finally:
		logging.info('Shutting down and closing websockets')

if __name__ == '__main__':
	run()
