def time_format(seconds: int) -> str:
    '''prints time in s in pretty format'''
    if seconds is not None:
        seconds = int(seconds)
        d = seconds // (3600 * 24)
        h = seconds // 3600 % 24
        m = seconds % 3600 // 60
        s = seconds % 3600 % 60
        if d > 0:
            return f'{d:02d}d{h:02d}h{m:02d}m{s:02d}s'
        elif h > 0:
            return f'{h:02d}h{m:02d}m{s:02d}s'
        elif m > 0:
            return f'{m:02d}m{s:02d}s'
        elif s > 0:
            return f'{s:02d}s'
    return '-'