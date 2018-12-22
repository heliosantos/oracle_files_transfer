import time


def get_update_function(updateFrequency):
    """
    Updates the progress of the transfer at a give frequency
    """
    totalTranf = 0
    intervalTansf = 0    
    intervalStart = time.monotonic()
    
    def update(transfered):
        nonlocal totalTranf
        nonlocal intervalTansf
        nonlocal intervalStart
        
        totalTranf += transfered
        intervalTansf += transfered
        
        if intervalStart + updateFrequency > time.monotonic():
            return
        
        print("transfered {}\t@ {:.1f} MiB/s".format(
            format_file_size(totalTranf), 
            (intervalTansf /(time.monotonic() - intervalStart)) / 1024 / 1024))
        
        intervalTansf = 0
        intervalStart = time.monotonic()
       
    return update


def format_file_size(fsize):
    result = []
    units = {s: u for s, u in zip(reversed([2 ** n for n in range(0, 40, 10)]), ['GiB', 'MiB', 'KiB', 'bytes'])}
    for s, u in units.items():
        t = fsize // s
        if t > 0:
            result.append('{} {}'.format(t, u))
        fsize = fsize % s
    return ', '.join(result) or '0 bytes'
