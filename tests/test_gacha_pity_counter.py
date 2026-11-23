import pytest
import gameplay.gacha_pity_counter as mod

CASES = [
    ("pool_migrate", (1,)),
    ("concurrent_pull", (1,)),
    ("timezone_reset", (1,)),
    ("version_migrate", (1,)),
    ("mail_compensate", (1,)),
    ("determinism", (1,)),
    ("counter_apply", (1,)),
    ("validation", (1,)),
    ("audit", (1,)),
    ("fallback", (1,)),
    ("pity_cap", (1,)),
    ("metrics", (1,)),
    ("ten_pull", (1,)),
    ("query", (1,)),
    ("rollback", (1,)),
]

@pytest.mark.parametrize("fn_name,args", CASES)
def test_contract(fn_name, args):
    fn = getattr(mod, fn_name)
    assert fn(*args) is not None
