from mock import MagicMock
import pytest
from tendrl.gluster_bridge import ini2json
from tendrl.gluster_bridge.tests.test_gluster_bridge import TestGluster_bridge


class Test_StrictConfigParser(TestGluster_bridge):

    def test_parse_empty_nodefaults(self):
        filename = self._makeFile('empty', '')
        sections = ini2json.ini_to_dict(filename)
        assert sections == {}

    def test_parse_empty_with_comment_line(self):
        body = """# empty with comment line"""
        filename = self._makeFile('pytest', body)
        sections = ini2json.ini_to_dict(filename)
        assert sections == {}

    def test_parse_empty_no_leading_whitespace(self):
        body = """rem ember_last = True"""
        filename = self._makeFile('pytest', body)
        sections = ini2json.ini_to_dict(filename)
        assert sections == {}

    def test_parse_duplicate_section(self):
        body = """[foo]\na=1\n[foo]\nbar=1\nbaz=2"""
        filename = self._makeFile('pytest', body)
        with pytest.raises(ValueError):
            ini2json.ini_to_dict(filename)

    def test_parse_continuation_line(self):
        body = """[foo]\nbar:1 2 jammy:jam\n foo bar\n boo"""
        filename = self._makeFile('pytest', body)
        sections = ini2json.ini_to_dict(filename)
        assert sections == (
            {'foo': {'bar': ['1', '2', 'jammy:jam', 'foo', 'bar', 'boo']}}
            )

    def test_parse_(self):
        body = """[foo]\na=1\n[memo]\n bar=1\nbaz=2"""
        filename = self._makeFile('pytest', body)
        with pytest.raises(ini2json.ParsingError):
            ini2json.ini_to_dict(filename)

    def test_parse_body_default(self):
        body = """[foo]\nbar=1\nbaz=2"\n[DEFAULT]\na=1"""
        filename = self._makeFile('pytest', body)
        sections = ini2json.ini_to_dict(filename)
        assert sections == (
            {'foo': {'a': '1', 'bar': '1', 'baz': '2"'}}
            )

    def test_parse_missing_section(self):
        body = """\nbar=1\nbaz=2"\n[DEFAULT]\na=1i"""
        filename = self._makeFile('pytest', body)
        with pytest.raises(ini2json.MissingSectionHeaderError):
            ini2json.ini_to_dict(filename)

    def test_parse_with_tokens(self):
        body = """[foo]\nbar: 1 2 3 4 jammy:jam\n"""
        filename = self._makeFile('pytest', body)
        sections = ini2json.ini_to_dict(filename)
        assert sections == (
            {'foo': {'bar': ['1', '2', '3', '4', 'jammy:jam']}}
            )

    def test_parse_with_bad_format(self):
        body = """[foo]\nbar: 1 ;\n 2 3 4 jammy:jam\n"""
        filename = self._makeFile('pytest', body)
        sections = ini2json.ini_to_dict(filename)
        assert sections == (
            {'foo': {'bar': ['1', '2', '3', '4', 'jammy:jam']}}
            )

    def test_parse_without_token(self):
        body = """[foo]\nbar:\n"""
        filename = self._makeFile('pytest', body)
        sections = ini2json.ini_to_dict(filename)
        assert sections == (
            {'foo': {'bar': ''}}
            )

    def test_parse_without_space(self):
        body = """[foo]\nbar:""\n"""
        filename = self._makeFile('pytest', body)
        sections = ini2json.ini_to_dict(filename)
        assert sections == (
            {'foo': {'bar': ''}}
            )

    def test_dget_with_dummy_section(self):
        self.strict_config_parser = ini2json.StrictConfigParser()
        assert self.strict_config_parser.dget("pytest", 1, 12345) == 12345

    def test_dget_with_type_str(self):
        self.strict_config_parser = ini2json.StrictConfigParser()
        self.strict_config_parser.has_option = MagicMock(return_value=True)
        self.strict_config_parser.get = MagicMock(return_value="str called")
        assert self.strict_config_parser.dget(
            "pytest", 1, 12345, str) == "str called"

    def test_dget_with_type_int(self):
        self.strict_config_parser = ini2json.StrictConfigParser()
        self.strict_config_parser.has_option = MagicMock(return_value=True)
        self.strict_config_parser.getint = MagicMock(
            return_value="int called")
        assert self.strict_config_parser.dget(
            "pytest", 1, 12345, int) == "int called"

    def test_dget_with_type_bool(self):
        self.strict_config_parser = ini2json.StrictConfigParser()
        self.strict_config_parser.has_option = MagicMock(return_value=True)
        self.strict_config_parser.getboolean = MagicMock(
            return_value="bool called")
        assert self.strict_config_parser.dget(
            "pytest", 1, 12345, bool) == "bool called"

    def test_dget_with_not_implemented_type(self):
        self.strict_config_parser = ini2json.StrictConfigParser()
        self.strict_config_parser.has_option = MagicMock(return_value=True)
        with pytest.raises(NotImplementedError):
            assert self.strict_config_parser.dget("pytest", 1, 12345, float)
