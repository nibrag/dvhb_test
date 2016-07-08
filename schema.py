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
        sa.Column('session_end', sa.DateTime(timezone=pytz.utc)))


async def create_schema(loop):
    async with create_engine(user='dvhb_usr', database='dvhb',
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
                                    session_end timestamp with time zone NOT NULL)'''
            await conn.execute(question_sql)
            await conn.execute(stats_sql)

            await conn.execute(question_tbl.insert().values(question='Who are you?',
                                                            answer='Hello! I am bot!'))
            await conn.execute(question_tbl.insert().values(question='How old are you?',
                                                            answer='I am 2 years old'))
