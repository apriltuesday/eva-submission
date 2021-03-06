from unittest import TestCase

from eva_submission.submission_config import EloadConfig


class TestEloadConfig(TestCase):

    def test_add_to_config(self):
        eload_cfg = EloadConfig()
        eload_cfg.set('key', value='Value1')
        assert eload_cfg.content['key'] == 'Value1'

        eload_cfg.set('level1', 'level2', 'level3', value='Value2')
        assert eload_cfg.content['level1']['level2']['level3'] == 'Value2'

    def test_remove_from_config(self):
        eload_cfg = EloadConfig()
        eload_cfg.set('level1', 'level2', 'level3', value='value')
        assert eload_cfg.pop('level1', 'lunch time', default='spaghetti') == 'spaghetti'
        assert eload_cfg.pop('level1', 'level2', 'level3', default='spaghetti') == 'value'
        assert eload_cfg.pop('level1', 'level2', 'level3', default='spaghetti') == 'spaghetti'
