from datetime import datetime
from copy import deepcopy

# import pytest

from bot import bot
from bot import dbutil


class TestURL:

    def test_init(self):
        row = {
            'id': 1,
            'scheme': 'https:',
            'host': 'www.example.com',
            'path': '/test/example',
            'query': 'foo=bar&hoge=fuga',
            'fragment': 'a_fragment',
            'updated_at': datetime.now(),
            'created_at': datetime.now(),
            'invalid': 0,
        }
        u = bot.URL(row)
        assert u.id == row['id']
        assert u.scheme == row['scheme']
        assert u.host == row['host']
        assert u.path == row['path']
        assert u.query == row['query']
        assert u.fragment == row['fragment']
        assert u.updated_at == row['updated_at']
        assert u.created_at == row['created_at']
        assert u.invalid == row['invalid']

    def test_from_string(self):
        urlstr = 'https://www.example.com/test/example/?foo=bar#top'
        u = bot.URL.from_string(urlstr)
        assert u.id is None
        assert u.scheme == 'https'
        assert u.host == 'www.example.com'
        assert u.path == '/test/example/'
        assert u.query == 'foo=bar'
        assert u.fragment == 'top'
        assert u.updated_at is None
        assert u.created_at is None
        assert u.invalid == 0

    def test_to_dict(self):
        urlstr = 'https://www.example.com/test/example/?foo=bar#top'
        u = bot.URL.from_string(urlstr)
        urldict = u.to_dict()
        assert urldict['id'] == u.id
        assert urldict['scheme'] == u.scheme
        assert urldict['host'] == u.host
        assert urldict['path'] == u.path
        assert urldict['query'] == u.query
        assert urldict['fragment'] == u.fragment
        assert urldict['updated_at'] == u.updated_at
        assert urldict['created_at'] == u.created_at
        assert urldict['invalid'] == u.invalid

    def test_to_string(self):
        urlstr = 'https://www.example.com/test/example/?foo=bar#top'
        u = bot.URL.from_string(urlstr)
        assert u.to_string() == urlstr


class TestURLRepo:

    def setup_method(self, method):
        db = dbutil.init_db_for_test()
        setattr(self, 'db', db)

    def teardown_method(self, method):
        self.db.commit()

    def test_next(self):
        urlids = self.prepare_urls()

        t = self.db.load_table('url')
        repo = bot.URLRepo(self.db, t)
        u = repo.next()
        assert u.id == urlids[3]

    def prepare_urls(self):
        t = self.db.load_table('url')
        src = {
            'scheme': 'https:',
            'host': 'www.example.com',
            'path': '/test/example',
            'query': 'foo=bar&hoge=fuga',
            'fragment': 'a_fragment',
            'updated_at': datetime.now(),
            'created_at': datetime.now(),
            'invalid': 0,
        }
        urlids = []
        r1 = src
        urlids.append(t.insert(r1))

        r2 = deepcopy(src)
        r2['path'] = '/test/example2'
        urlids.append(t.insert(r2))

        r3 = deepcopy(src)
        r3['path'] = '/test/example3'
        urlids.append(t.insert(r3))

        r4 = deepcopy(src)
        r4['path'] = '/test/example4'
        urlids.append(t.insert(r4))

        t2 = self.db.load_table('sessions')
        src2 = {
            'url_id': urlids[0],
            'start_time': datetime.now(),
            'end_time': datetime.now(),
            'state': 200,
            'response_code': 200,
            'result': 200,
        }
        sr1 = src2
        t2.insert(sr1)

        sr2 = deepcopy(src2)
        sr2['url_id'] = urlids[1]
        t2.insert(sr2)

        sr3 = deepcopy(src2)
        sr3['url_id'] = urlids[0]
        t2.insert(sr3)

        sr4 = deepcopy(src2)
        sr4['url_id'] = urlids[2]
        t2.insert(sr4)

        return urlids
