import pytz
import sqlalchemy as sa
from aiopg.sa import create_engine

__all__ = ('question_tbl', 'stats_tbl', 'create_schema')

metadata = sa.MetaData()

question_tbl = sa.Table('questions', metadata,
                        sa.Column('id', sa.Integer, primary_key=True),
                        sa.Column('question', sa.String(255)),
                        sa.Column('answer', sa.String(255)))

stats_tbl = sa.Table('stats', metadata,
                     sa.Column('id', sa.Integer, primary_key=True),
                     sa.Column('client_id', sa.Integer),
                     sa.Column('session_start', sa.DateTime(timezone=pytz.utc)),
                     sa.Column('last_visit', sa.DateTime(timezone=pytz.utc)))


async def create_schema(loop):
    async with create_engine(
            user='dvhb_usr', database='dvhb',
            host='localhost', password='pwd', loop=loop) as engine:
        async with engine.acquire() as conn:
            question_sql = '''CREATE TABLE IF NOT EXISTS questions (
                                    id serial PRIMARY KEY,
                                    question character varying(255) NOT NULL,
                                    answer character varying(255) NOT NULL)'''
            stats_sql = '''CREATE TABLE IF NOT EXISTS stats (
                                    id serial PRIMARY KEY,
                                    client_id integer NOT NULL,
                                    session_start timestamp with time zone NOT NULL,
                                    last_visit timestamp with time zone)'''

            await conn.execute('DROP TABLE IF EXISTS questions')
            await conn.execute('DROP TABLE IF EXISTS stats')
            await conn.execute(question_sql)
            await conn.execute(stats_sql)

            q1 = question_tbl.insert().values(question='Who are you?',
                                              answer='Hello! I am bot!')
            q2 = question_tbl.insert().values(question='How old are you?',
                                              answer='I am 2 years old')
            await conn.execute(q1)
            await conn.execute(q2)
