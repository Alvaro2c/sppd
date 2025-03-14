from src.dl_parser.utils import flatten_dict


def test_flatten_dict():
    nested_dict = {"a": {"b": {"c": 1}}, "d": 2}
    flat_dict = flatten_dict(nested_dict)
    expected_dict = {"a.b.c": 1, "d": 2}
    assert flat_dict == expected_dict
