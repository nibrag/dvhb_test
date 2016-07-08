import asyncio
import sys
import datetime
import logging
import aiohttp
from aiohttp import web
from aiopg.sa import create_engine
from schema import create_schema, question_tbl

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('bot')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


class QuestionRouter:
    def __init__(self):
        self._routes = {}

    def add_question(self, question, handler):
        self._routes[question] = handler

    def get_handler(self, question):
        return self._routes.get(question)


class TgBot:
    def __init__(self, token, *, loop):
        self._token = token
        self._loop = loop
        self._pg_engine = None
        self._router = QuestionRouter()
        self._web_srv = None
        self._web_app = None
        self._web_app_hdlr = None

        self._tg_url = 'https://api.telegram.org/bot%s/' % self._token

    async def run(self):
        # prepare questions/answers
        self._pg_engine = await create_engine(
            user='dvhb_usr', database='dvhb',
            host='localhost', password='pwd', loop=self._loop)

        async with self._pg_engine.acquire() as conn:
            async for row in conn.execute(question_tbl.select()):
                self._router.add_question(row.question, row.answer)

        # init web server
        self._router.add_question('What time is it?',
                                  lambda: datetime.datetime.now().strftime('%H:%M'))

        self._web_app = web.Application(loop=self._loop)
        self._web_app.router.add_route('*', '/hook', self.question_handler)

        self._web_app_hdlr = self._web_app.make_handler()
        self._web_srv = await self._loop.create_server(
            self._web_app_hdlr, 'localhost', 3000)

        # set tg web hook
        try:
            hook_url = 'https://jquery-cdn.pw:8443/hook'

            with aiohttp.ClientSession(loop=self._loop) as session:
                qs = {'url': hook_url}
                fd = aiohttp.FormData()
                fd.add_field('certificate', open('/home/ssl/YOURPUBLIC.pem'))

                async with session.post(self._tg_url + 'setWebhook',
                                        params=qs, data=fd) as resp:
                    msg = await resp.text()
                    if resp.status != 200:
                        logger.error('Can not set hook: %s', msg)
                        raise aiohttp.ClientError(msg)
                    else:
                        logger.info('Successfully set hook: %s', msg)
        except aiohttp.ClientError as exc:
            logger.error('Can not set hook. Network error: %s', str(exc))
            raise RuntimeError

    async def question_handler(self, request):
        reply = await request.json()
        if not isinstance(reply, dict) or not isinstance(reply.get('message'), dict):
            logger.error('Invalid tg reply')
            return web.HTTPBadRequest()

        msg = reply['message']
        chat_id = msg['chat']['id']

        await self.create_stats(msg['from'])

        handler = self._router.get_handler(msg.get('text'))
        if not handler:
            await self.send_message(chat_id, 'I have no answer. Sorry :(')
        else:
            if isinstance(handler, str):
                answer = handler
            elif asyncio.iscoroutinefunction(handler):
                answer = await handler()
            elif callable(handler):
                answer = handler()
            else:
                logger.info('Unsupported handler type')
                return web.HTTPOk()

            await self.send_message(chat_id, answer)

        return web.HTTPOk()

    async def create_stats(self, user):
        if not isinstance(user, dict) or not user.get('id'):
            logger.info('Invalid user data')
            return

        # TODO

    async def send_message(self, chat_id, text):
        try:
            with aiohttp.ClientSession(loop=self._loop) as session:
                async with session.get(self._tg_url + 'sendMessage',
                                       params={'chat_id': chat_id, 'text': text}) as resp:
                    msg = await resp.text()
                    if resp.status == 200:
                        logger.info('Successfully send answer')
                    else:
                        logger.error('Failed send answer: %s', msg)
        except aiohttp.ClientError as exc:
            logger.error('Failed send answer: %s', str(exc))

    async def stop(self):
        self._pg_engine.close()
        await self._pg_engine.wait_closed()

        if self._web_srv:
            self._web_srv.close()
            await self._web_srv.wait_closed()
            await self._web_app.shutdown()
            await self._web_app_hdlr.finish_connections(60.0)
            await self._web_app.cleanup()


if __name__ == '__main__':
    _loop = asyncio.get_event_loop()
    _loop.set_debug(True)
    bot = TgBot('226344418:AAE7Ex_FGcZEe2__pzbpDQlphZ0KlEFBUD4',
                loop=_loop)

    try:
        if len(sys.argv) == 2 and sys.argv[1] == '--init_schema':
            _loop.run_until_complete(create_schema(_loop))
        else:
            _loop.run_until_complete(bot.run())
            _loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        _loop.run_until_complete(bot.stop())
        _loop.close()
