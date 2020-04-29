from sources.sina_data_funcs import get_futures_daily_data_from_sina


def test_get_futures_daily_data_from_sina():
    data = get_futures_daily_data_from_sina('M0')
    assert data is not None
