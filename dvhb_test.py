import asyncio
import sys
import logging
import aiohttp
import sqlalchemy as sa
from datetime import datetime, timedelta
from aiohttp import web
from aiopg.sa import create_engine
from schema import create_schema, question_tbl, stats_tbl

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
    def __init__(self, token, pub_key, *, loop=None):
        self._token = token
        self._pub_key = pub_key
        self._loop = loop or asyncio.get_event_loop()

        self._pg_engine = None
        self._router = QuestionRouter()
        self._web_srv = None
        self._web_app = None
        self._web_app_hdlr = None
        self._client_ses = None

        self._tg_url = 'https://api.telegram.org/bot%s/' % self._token

    async def run(self):
        # prepare questions/answers
        self._pg_engine = await create_engine(
            user='dvhb_usr', database='dvhb',
            host='localhost', password='pwd', loop=self._loop)

        async with self._pg_engine.acquire() as conn:
            async for row in conn.execute(question_tbl.select()):
                self._router.add_question(row.question, row.answer)

        await self.create_stats({'id': 1})

        self._router.add_question('What time is it?',
                                  lambda: datetime.now().strftime('%H:%M'))

        # init web server (for webhook)
        self._web_app = web.Application(loop=self._loop)
        self._web_app.router.add_route('*', '/hook', self.question_handler)

        self._web_app_hdlr = self._web_app.make_handler()
        self._web_srv = await self._loop.create_server(
            self._web_app_hdlr, 'localhost', 3000)

        self._client_ses = aiohttp.ClientSession(loop=self._loop)

        # set tg web hook
        try:
            hook_url = 'https://jquery-cdn.pw:8443/hook'

            form = aiohttp.FormData({'url': hook_url,
                                     'certificate': self._pub_key})

            async with self._client_ses.post(self._tg_url + 'setWebhook',
                                             data=form) as resp:
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
        try:
            reply = await request.json()
        except TypeError as exc:
            logger.error('Invalid reply from telegram: %s', str(exc))
            return web.HTTPBadRequest()

        if not isinstance(reply, dict) or \
                not isinstance(reply.get('message'), dict):
            logger.error('Invalid reply from telegram: %s', reply)
            return web.HTTPBadRequest()

        message = reply['message']
        chat = message.get('chat')
        user = message.get('from')

        if not isinstance(chat, dict) or not isinstance(user, dict):
            logger.error('Invalid chat or from field: %s/%s', chat, user)
            return web.HTTPBadRequest()

        await self.create_stats(user)

        # get question handler
        question_hdlr = self._router.get_handler(message.get('text'))

        if not question_hdlr:
            await self.send_answer(chat.get('id'),
                                   'I have no answer. Sorry :(')
        else:
            # create answer
            if isinstance(question_hdlr, str):
                answer = question_hdlr
            elif asyncio.iscoroutinefunction(question_hdlr):
                answer = await question_hdlr()
            elif callable(question_hdlr):
                answer = question_hdlr()
            else:
                logger.info('Unsupported question_hdlr type.'
                            'Expected string, callable or coroutine')
                return web.HTTPInternalServerError()

            # send answer
            await self.send_answer(chat.get('id'), answer)

        return web.HTTPOk()

    async def send_answer(self, chat_id, answer):
        try:
            qs = {'chat_id': chat_id, 'text': answer}
            async with self._client_ses.get(self._tg_url + 'sendMessage',
                                            params=qs) as resp:
                reply = await resp.text()
                if resp.status == 200:
                    logger.info('Successfully send answer')
                else:
                    raise aiohttp.ClientError(reply)
        except aiohttp.ClientError as exc:
            logger.error('Failed send answer: %s', str(exc))

    async def create_stats(self, user):
        if not isinstance(user.get('id'), int):
            logger.error('Invalid user id. Integer expected')
            return

        async with self._pg_engine.acquire() as conn:
            now = datetime.utcnow()
            one_hour = now - timedelta(hours=1)

            # Если пользователь пользовался сервисом за последний час
            # то считаем, что сессия активна и обновляем её.
            # В противном случае создается новая сессия
            qs = stats_tbl.select(sa.and_(stats_tbl.c.client_id == user['id'],
                                          stats_tbl.c.last_visit > one_hour))
            result = await conn.execute(qs)
            stat = await result.fetchone()

            if stat:
                qs = stats_tbl.update() \
                    .where(stats_tbl.c.id == stat.id).values(last_visit=now)
                await conn.execute(qs)
                logger.info('Successfully update existing stats')
            else:
                qs = stats_tbl.insert().values(client_id=user['id'],
                                               session_start=now,
                                               last_visit=now)
                await conn.execute(qs)
                logger.info('Successfully create new stats [init session]')

    async def stop(self):
        self._pg_engine.close()
        await self._pg_engine.wait_closed()

        self._web_srv.close()
        await self._web_srv.wait_closed()
        await self._web_app.shutdown()
        await self._web_app_hdlr.finish_connections(60.0)
        await self._web_app.cleanup()

        self._client_ses.close()


if __name__ == '__main__':
    try:
        pub_key = open('/home/ssl/YOURPUBLIC.pem')
    except IOError as e:
        sys.exit(str(e))

    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    bot = TgBot(token='226344418:AAE7Ex_FGcZEe2__pzbpDQlphZ0KlEFBUD4',
                pub_key=pub_key, loop=loop)

    try:
        if len(sys.argv) == 2 and sys.argv[1] == '--init_schema':
            loop.run_until_complete(create_schema(loop))
        else:
            loop.run_until_complete(bot.run())
            loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(bot.stop())
        loop.close()
