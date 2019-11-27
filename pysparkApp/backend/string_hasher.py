def string_hash(source: str) -> int:
    """Inspired by: https://cp-algorithms.com/string/string-hashing.html"""
    p = 53
    m = 2 << 31
    hash_val = 0
    p_pow = 1
    for i in range(0, len(source)):
        hash_val += ord(source[i]) * p_pow
        p_pow *= p
    return hash_val % m
