import os
import logging
from collections import namedtuple

import pymysql
import dataset
import click

import bot

Params = namedtuple('Params', ['database', 'host', 'port',
                               'user', 'password', 'rootpass'])


@click.group()
@click.pass_context
@click.option('-d', '--database', envvar="MYSQL_DATABASE",
              type=str, help='database')
@click.option('-h', '--host', envvar="MYSQL_HOST",
              type=str, help='host')
@click.option('-p', '--port', envvar="MYSQL_PORT",
              type=int, help='port')
@click.option('-u', '--user', envvar="MYSQL_USER",
              type=str, help='user')
@click.option('-P', '--password', envvar="MYSQL_PASSWORD",
              type=str, help='password')
@click.option('-r', '--rootpass', envvar="MYSQL_ROOT_PASSWORD",
              type=str, help='rootpass')
def dbutil(ctx, database, host, port, user, password, rootpass):
    p = Params(database, host, port, user, password, rootpass)
    logger = init_logger()
    Obj = namedtuple('Obj', ['logger', 'params'])
    ctx.obj = Obj(logger, p)

    prmsstr = 'database:{}  host:{}  port:{}  user:{}'
    if password:
        prmsstr = prmsstr + '  password:*****'
    if rootpass:
        prmsstr = prmsstr + '  rootpass:*****'

    logger.info('==> run dbutil')
    logger.info('==> params(' +
                prmsstr.format(database, host, port, user) + ')')


@dbutil.command()
@click.pass_context
def create_all_tables(ctx):
    ctx.obj.logger.info('==> create all tables')
    db = init_db(ctx.obj.params)
    create_url(db)
    create_url_info(db)
    create_sessions(db)


@dbutil.command()
@click.pass_context
def drop_all_tables(ctx):
    ctx.obj.logger.info('==> drop all tables')
    db = init_db(ctx.obj.params)
    for t in db.tables:
        db.load_table(t).drop()


@dbutil.command()
@click.pass_context
@click.option('--drop', is_flag=True, help='drop database')
def create_db_and_tables(ctx, drop):
    recreate_db(ctx.obj.params, drop=drop)
    db = init_db(ctx.obj.params)
    create_url(db)
    create_url_info(db)
    create_sessions(db)


@dbutil.command()
@click.pass_context
@click.option('--filename', type=str, help='url file', required=True)
def load_url_from_file(ctx, filename):
    logger = ctx.obj.logger
    db = init_db(ctx.obj.params)
    t = db.load_table('url')
    repo = bot.URLRepo(db, t)

    fpath = os.path.abspath(filename)
    logger.info('load from file({})'.format(fpath))
    repo.load_from_file(fpath)


def init_db(params):
    connstr = ('mysql+pymysql://{user}:{password}'
               '@{host}:{port}/{database}').format(
        user=params.user,
        password=params.password,
        host=params.host,
        port=params.port,
        database=params.database,
    )
    return dataset.connect(connstr)


def recreate_db(params, drop=False):
    conn = pymysql.connect(host=params.host, port=params.port,
                           user='root', password=params.rootpass)
    if drop:
        conn.cursor(). \
            execute('drop database if exists {}'.format(params.database))

    conn.cursor().execute('create database {}'.format(params.database))

    conn.cursor(). \
        execute("grant all on {}.* to '{}'@'%'".
                format(params.database, params.user))

    conn.close()


def init_db_for_test():
    db_host = os.getenv("MYSQL_HOST")
    db_port = int(os.getenv("MYSQL_PORT"))
    db_user = os.getenv("MYSQL_USER")
    db_root_pass = os.getenv("MYSQL_ROOT_PASSWORD")
    db_password = os.getenv("MYSQL_PASSWORD")
    dbname = 'crawler_test'

    recreate_db(db_host, db_port, db_root_pass, dbname, db_user)

    connstr = ('mysql+pymysql://{user}:{password}'
               '@{host}:{port}/{database}').format(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=dbname,
    )
    db = dataset.connect(connstr)
    db.begin()
    create_url(db)
    create_url_info(db)
    create_sessions(db)
    return db


def init_logger():
    logger = logging.getLogger()
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


def create_url(db):
    db.query("""
    CREATE TABLE IF NOT EXISTS `url` (
        `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
        `scheme` VARCHAR(255) NOT NULL DEFAULT '',
        `host` VARCHAR(255) NOT NULL DEFAULT '',
        `path` VARCHAR(255) NOT NULL DEFAULT '',
        `query` VARCHAR(255) NOT NULL DEFAULT '',
        `fragment` VARCHAR(255) NOT NULL DEFAULT '',
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP,
        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `invalid` INT NOT NULL DEFAULT 0,
        PRIMARY KEY (`id`),
        KEY ix_path (`path`),
        KEY ix_query (`query`),
        KEY ix_created_at (`created_at`),
        UNIQUE KEY ix_path_query (`path`,`query`)
    )
            """)


def create_url_info(db):
    db.query("""
    CREATE TABLE IF NOT EXISTS `url_info` (
        `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
        `url_id` INT UNSIGNED NOT NULL,
        `url_type` INT UNSIGNED NOT NULL DEFAULT 0,
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP,
        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY ix_url_id (`url_id`),
        KEY ix_created_at (`created_at`)
    )
            """)


def create_sessions(db):
    db.query("""
    CREATE TABLE IF NOT EXISTS `sessions` (
        `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
        `url_id` INT UNSIGNED NOT NULL,
        `start_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP,
        `end_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `state` INT UNSIGNED NOT NULL DEFAULT 100,
        `response_code` INT UNSIGNED NOT NULL DEFAULT 0,
        `result` INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (`id`),
        KEY ix_url_id_result_state (`url_id`, `result`, `state`)
    )
            """)


def main():
    dbutil()


if __name__ == '__main__':
    main()
