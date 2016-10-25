from mock import MagicMock
import pytest
from tendrl.gluster_bridge import config


class TestTendrlconfig(object):
    def setup_method(self, method):
        config.os.path.exists = MagicMock(return_value=True)

    def test_with_default_path(self):
        config.TendrlConfig()
        config.os.path.exists.assert_called_with(
            "/etc/tendrl/tendrl.conf"
            )

    def test_with_config_path(self, monkeypatch):
        monkeypatch.setattr(config.os, 'environ', {"TENDRL_CONFIG": "/temp/"})
        config.TendrlConfig()
        config.os.path.exists.assert_called_with(
            "/temp/"
            )

    def test_with_path_exist_error(self):
        config.TendrlConfig()
        config.os.path.exists = MagicMock(return_value=False)
        with pytest.raises(config.ConfigNotFound):
            config.TendrlConfig()
