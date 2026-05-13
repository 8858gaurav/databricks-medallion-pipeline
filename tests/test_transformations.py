# tests/test_transformations.py
def calculate_discount(price, discount):
    return price - discount

def test_calculate_discount():
    # This is a 'Unit Test'
    assert calculate_discount(100, 20) == 80
    assert calculate_discount(50, 50) == 0