
import asyncio
import asyncpg


conn_string = "host='127.0.0.1' dbname='vh_test' user='postgres' password='c7M2wy4gt36d3YD'"

async def run():
	conn = await asyncpg.connect(dsn='', user='postgres', password='c7M2wy4gt36d3YD', database='vh_test', host='127.0.0.1')
	#conn = await asyncpg.connect(conn_string)
	values = await conn.fetch('SELECT * FROM prefload;')
	await conn.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(run())


