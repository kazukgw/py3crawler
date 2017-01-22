import asyncio
import datetime
import random
from urllib import parse

import requests
# from IPython import embed

from bot import dbutil

# 以下DBのTableとのマッピングをしているクラスがあるが
# そのなかの幾つかのmethodではカラムを明示するために
# あえて obj.__dict__ のようなものは使わずすべてのカラムの対応を書いている


class URL:

    def __init__(self, row):
        # dataset-package において insert時のdictの
        # nullと値なしは明確に区別されるため
        # id, upated_at, created_at については
        # ほかのメソッドでも通常のカラムとは別の処理をしてある
        self.id = row.get('id')
        self.updated_at = row.get('updated_at')
        self.created_at = row.get('created_at')

        self.scheme = row['scheme']
        self.host = row['host']
        self.path = row['path']
        self.query = row['query']
        self.fragment = row['fragment']
        self.invalid = row['invalid']

    @classmethod
    def from_string(cls, urlstr):
        u = parse.urlparse(urlstr)
        row = {
            'scheme': u.scheme,
            'host': u.netloc,
            'path': u.path,
            'query': u.query,
            'fragment': u.fragment,
            'invalid': 0,
        }
        return cls(row)

    def to_dict(self):
        r = {
            'scheme': self.scheme,
            'host': self.host,
            'path': self.path,
            'query': self.query,
            'fragment': self.fragment,
            'invalid': self.invalid,
        }
        if self.id:
            r['id'] = self.id
        if self.updated_at:
            r['updated_at'] = self.updated_at
        if self.created_at:
            r['created_at'] = self.created_at

        return r

    def to_string(self):
        return parse.urlunparse([
            self.scheme,
            self.host,
            self.path,
            '',
            self.query,
            self.fragment,
        ])


class URLRepo:

    def __init__(self, db, table):
        self.db = db
        self.table = table

    def next(self):
        query = """
        SELECT
            url.*
        FROM
            url
        FORCE INDEX
            (PRIMARY)
        LEFT JOIN
            sessions
        FORCE INDEX
            (ix_url_id_result_state)
        ON
            url.id = sessions.url_id
        WHERE
            sessions.result IS NULL
        OR
            sessions.result <= 600
        GROUP BY
            url.id
        ORDER BY
            count(sessions.id)
        LIMIT 1
        """
        res = self.db.query(query)
        for r in res:
            if r:
                return URL(r)

    def bulk_save(self, urls):
        dicts = [u.to_dict() for u in urls]
        self.table.insert_many(dicts)

    def load_from_file(self, filename):
        lines = None
        with open(filename) as f:
            lines = f.readlines()

        urls = [URL.from_string(l) for l in lines]
        self.bulk_save(urls)


class Session:

    def __init__(self, row):
        self.id = row['id']
        self.url_id = row['url_id']
        self.start_time = row['start_time']
        self.end_time = row['end_time']
        self.state = row['state']
        self.response_code = row['response_code']
        self.result = row['result']

        self.url = None
        self.response = None

    def to_dict(self):
        return {
            'id': self.id,
            'url_id': self.url_id,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'state': self.state,
            'response_code': self.response_code,
            'result': self.result,
        }


class SessionRepo:

    def __init__(self, db, table):
        self.db = db
        self.table = table

    def new_session(self, url):
        sid = self.table.insert({'url_id': url.id})
        row = self.table.find_one(id=sid)
        sess = Session(row)
        sess.url = url
        return sess

    def save(self, sess):
        self.table.upsert(sess.to_dict(), ['id'])


class Context:

    def __init__(self):
        self.session_history = []


class ControllerBase:

    def can_run(self, ctx, bot):
        raise NotImplementedError()

    def on_fetch(self, sess, bot):
        raise NotImplementedError()

    def on_except(self, e, sess, bot):
        raise NotImplementedError()


class Bot:

    def __init__(self, schedule, proxies, controller, dbparams):

        self.ctx = Context()
        self.schedule = schedule
        self.proxies = proxies
        self.current_proxy_idx = 0
        self.controller = controller

        db = dbutil.init_db(dbparams)
        self.url_repo = URLRepo(db, db.load_table('url'))
        self.session_repo = SessionRepo(db, db.load_table('sessions'))

    def in_active_schedule(self):
        n = datetime.datetime.now().time()
        in_time_range = self.schedule['start_time'] \
            <= n <= self.schedule['end_time']

        if self.schedule['active_weekday'] == '*':
            return in_time_range

        wd = datetime.datetime.now().weekday()
        is_active_weekday = wd in self.schedule.active_weekday

        return in_time_range and is_active_weekday

    def next_proxy(self):
        if not self.proxies:
            return

        self.curent_proxy_idx = (self.current_proxy_idx + 1) % \
            len(self.proxies)
        return self.proxies[self.current_proxy_idx]

    def next_ua(self):
        return random.choice([
            'Mozilla/5.0 (Macintosh Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36',
            'Mozilla/5.0 (compatible MSIE 10.0 Windows NT 6.1 Trident/4.0 InfoPath.2 SV1 .NET CLR 2.0.50727 WOW64)',
            'Mozilla/5.0 (compatible MSIE 9.0 Windows NT 6.1 Win64 x64 Trident/5.0 .NET CLR 3.5.30729 .NET CLR 3.0.30729 .NET CLR 2.0.50727 Media Center PC 6.0)',
            'Mozilla/5.0 (compatible MSIE 8.0 Windows NT 5.2 Trident/4.0 Media Center PC 4.0 SLCC1 .NET CLR 3.0.04320)',
            'Mozilla/4.0 (compatible MSIE 8.0 Windows NT 6.2 Trident/4.0 SLCC2 .NET CLR 2.0.50727 .NET CLR 3.5.30729 .NET CLR 3.0.30729 Media Center PC 6.0)',
            'Mozilla/5.0 (Macintosh Intel Mac OS X 10_10 rv:33.0) Gecko/20100101 Firefox/33.0 Mozilla/5.0 (Windows NT 6.3 rv:36.0) Gecko/20100101 Firefox/36.0',
            'Mozilla/5.0 (Linux U Android 4.0.3 ja - jp LG - L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
            'Mozilla/5.0 (Linux U Android 4.0.3 ja - jp HTC Sensation Build/IML74K) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
            'Mozilla/5.0 (Linux U Android 2.3 ja - jp) AppleWebKit/999 + (KHTML, like Gecko) Safari/999.9',
            'Mozilla/5.0 (Linux U Android 2.3.5 ja - jp HTC_IncredibleS_S710e Build/GRJ90) AppleWebKit/533.1 (KHTML, like Gecko)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0)',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13+ (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2',
        ])

    def start(self):
        asyncio.async(self._start())
        loop = asyncio.get_event_loop()
        try:
            loop.run_forever()
        finally:
            loop.close()

    @asyncio.coroutine
    def _start(self):
        print('==> start')
        # 指定されたスケジュール and
        # コンテキストが条件にマッチする場合 にのみ実行する
        if not self.in_active_schedule() or \
                not self.controller.can_run(self.ctx, self):
            yield from asyncio.sleep(1)
            print('==> skip')
            return asyncio.async(self._start())

        yield from asyncio.sleep(self.schedule['every'])

        sess = None
        try:
            url = self.url_repo.next()
            sess = self.session_repo.new_session(url)
            prox = self.next_proxy()
            ua = self.next_ua()
            asyncio.async(self.fetch(sess, proxy=prox, ua=ua))
        except Exception as e:
            self.controller.on_except(e, sess, self)
        finally:
            asyncio.async(self._start())

    @asyncio.coroutine
    def fetch(self, sess, proxy=None, ua=None):
        headers = requests.utils.default_headers()
        if ua:
            headers.update({'User-Agent': ua})
        sess.response = requests.get(sess.url.to_string(),
                                     headers=headers, proxies=proxy)
        self.controller.on_fetch(sess, self)
