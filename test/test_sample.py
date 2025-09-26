from data_agency.sample import add_numbers


def test_add_numbers_positive():
    """Tests adding two positive integers."""
    assert add_numbers(2, 3) == 5


def test_add_numbers_negative():
    """Tests adding two negative integers."""
    assert add_numbers(-5, -10) == -15


def test_add_numbers_mixed():
    """Tests adding a positive and a negative integer."""
    assert add_numbers(10, -3) == 7