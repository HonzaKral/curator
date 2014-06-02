from curator import es_repo_mgr

from mock import patch, Mock

from . import CuratorTestCase

class TestRepoMgr(CuratorTestCase):

    def test_repository_will_be_created_and_listed_and_deleted(self):
        body = es_repo_mgr.create_repo_body(repo_type='fs')
        es_repo_mgr._create_repository(self.client, self.args['repository'], body=body)
        pre = es_repo_mgr._get_repository(self.client, self.args['repository'])
        self.assertEqual('fs', pre[self.args['repository']]['type'])
        self.assertEqual(self.args['repository'], pre.keys()[0])
        es_repo_mgr._delete_repository(self.client, self.args['repository'])
        post = es_repo_mgr._get_repository(self.client, self.args['repository'])
        self.assertEqual(None, post)
